"""MCP Streamable-HTTP transport for the gateway bridge.

Speaks JSON-RPC over POST only: no GET SSE stream, no DELETE teardown, no
oauth/discovery, and never an Authorization header (iron-proxy injects the
gateway Bearer on host+path). Handles both live transport profiles:

- FastMCP (clickhouse): SSE responses, stateful sessions: a bare request gets
  HTTP 400 "Missing session ID", and 400s can carry bogus session headers that
  must be ignored; the session id is only ever taken from the initialize
  response.
- rmcp (minisim): plain JSON, stateless, and may negotiate a protocolVersion
  newer than requested, which is not an error.

Optimistic-call-first: with a cached or absent session the target request goes
out immediately; on a session-shaped rejection (400 missing / 404 expired) the
endpoint handshakes and retries exactly once. Those are the only auto-retried
signals: they prove the server did not execute the call. Timeouts and 5xx on
calls surface as "outcome unknown".
"""

from __future__ import annotations

import json
from typing import Any

import httpx

# This package is loaded under three module contexts (wheel `centaur_tool_mcp`,
# the shim runner's `mcp` package, flat modules under pytest), so sibling
# imports are relative-first with a flat fallback.
try:
    from ._cache import read as cache_read
    from ._cache import write as cache_write
except ImportError:  # flat module context (pytest with CWD on sys.path)
    from _cache import read as cache_read  # type: ignore[no-redef]
    from _cache import write as cache_write  # type: ignore[no-redef]

PROTOCOL_VERSION = "2025-06-18"
CLIENT_INFO = {"name": "centaur-mcp-bridge", "version": "0.1.0"}


class TransportError(RuntimeError):
    """Network or HTTP-level failure; the request may or may not have run."""


class RateLimitedError(TransportError):
    """HTTP 429 from the gateway. Surfaced once, never retried."""

    def __init__(self, message: str, retry_after: str | None = None):
        super().__init__(message)
        self.retry_after = retry_after


class NotGrantedError(RuntimeError):
    """HTTP 401/403: this channel's token does not cover the server."""

    def __init__(self, message: str, status: int, kong_authored: bool, body: str, server: str = ""):
        super().__init__(message)
        self.status = status
        self.kong_authored = kong_authored
        self.body = body
        self.server = server


class McpError(RuntimeError):
    """A JSON-RPC error result: the server executed and rejected the input."""

    def __init__(self, message: str, code: int | None = None, data: Any = None):
        super().__init__(message)
        self.code = code
        self.data = data


def _kong_authored(response: httpx.Response) -> bool:
    # ponytail: heuristic fingerprint, calibrated against live responses during
    # the Phase 0 dark verify. Kong stamps x-kong-* headers / Via on responses
    # it authors; plugin denials use a bare {"message": ...} JSON body.
    headers = response.headers
    if any(name.lower().startswith("x-kong-") for name in headers):
        return True
    if "kong" in headers.get("via", "").lower() or "kong" in headers.get("server", "").lower():
        return True
    try:
        body = response.json()
    except ValueError:
        return False
    return isinstance(body, dict) and set(body) == {"message"}


def _decode_sse(text: str, want_id: int) -> dict[str, Any]:
    """Select the JSON-RPC message matching our request id from an SSE stream.

    Id-matching (not last-event-wins) tolerates interleaved notifications.
    """
    messages: list[dict[str, Any]] = []
    for block in text.replace("\r\n", "\n").split("\n\n"):
        data_lines = [line[5:].lstrip() for line in block.splitlines() if line.startswith("data:")]
        if not data_lines:
            continue
        try:
            message = json.loads("\n".join(data_lines))
        except ValueError:
            continue
        if isinstance(message, dict):
            messages.append(message)
    for message in messages:
        if message.get("id") == want_id:
            return message
    raise TransportError(
        f"no JSON-RPC response with id {want_id} in SSE stream ({len(messages)} messages)"
    )


class Endpoint:
    """One MCP server behind the gateway, addressed via a shared httpx client."""

    def __init__(self, name: str, url: str, http: httpx.Client, use_cache: bool = True):
        self.name = name
        self.url = url
        self._http = http
        self._use_cache = use_cache
        self._session: dict[str, Any] | None = None
        self._next_id = 1
        self.init_result: dict[str, Any] | None = None

    # -- session state ------------------------------------------------------

    def _load_session(self) -> dict[str, Any] | None:
        if self._session is None and self._use_cache:
            cached = cache_read(self.name, "session.json")
            if isinstance(cached, dict):
                self._session = cached
        return self._session

    def _store_session(self, session: dict[str, Any]) -> None:
        self._session = session
        if self._use_cache:
            cache_write(self.name, "session.json", session)

    # -- wire ---------------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        session = self._load_session()
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": (session or {}).get("protocol_version") or PROTOCOL_VERSION,
        }
        session_id = (session or {}).get("session_id")
        if session_id:
            headers["Mcp-Session-Id"] = session_id
        return headers

    def _post(self, payload: dict[str, Any], timeout: float | None = None) -> httpx.Response:
        try:
            kwargs: dict[str, Any] = {"headers": self._headers()}
            if timeout is not None:
                kwargs["timeout"] = timeout
            return self._http.post(self.url, json=payload, **kwargs)
        except httpx.RequestError as exc:
            raise TransportError(
                f"{self.name}: request failed ({exc.__class__.__name__}: {exc}); "
                "outcome unknown: check state before re-running"
            ) from exc

    @staticmethod
    def _is_session_rejection(response: httpx.Response) -> bool:
        if response.status_code not in (400, 404):
            return False
        return "session" in response.text.lower()

    def _raise_for_status(self, response: httpx.Response) -> None:
        status = response.status_code
        if status < 400:
            return
        if status in (401, 403):
            raise NotGrantedError(
                f"{self.name}: HTTP {status}",
                status,
                _kong_authored(response),
                response.text,
                server=self.name,
            )
        if status == 429:
            raise RateLimitedError(
                f"{self.name}: rate limited (HTTP 429)",
                retry_after=response.headers.get("retry-after"),
            )
        # A 4xx body often carries its diagnosis as a JSON-RPC error: parse it
        # before raising a bare HTTP error.
        try:
            body = response.json()
        except ValueError:
            body = None
        if isinstance(body, dict) and isinstance(body.get("error"), dict):
            error = body["error"]
            raise McpError(
                f"{self.name}: {error.get('message', 'JSON-RPC error')} (HTTP {status})",
                code=error.get("code"),
                data=error.get("data"),
            )
        raise TransportError(
            f"{self.name}: HTTP {status}: {response.text[:500]}; "
            "outcome unknown: check state before re-running"
        )

    def _decode(self, response: httpx.Response, want_id: int) -> dict[str, Any]:
        content_type = response.headers.get("content-type", "")
        if content_type.startswith("text/event-stream"):
            message = _decode_sse(response.text, want_id)
        else:
            try:
                message = response.json()
            except ValueError as exc:
                raise TransportError(f"{self.name}: non-JSON response: {response.text[:200]}") from exc
        if not isinstance(message, dict):
            raise TransportError(f"{self.name}: unexpected JSON-RPC payload shape")
        if isinstance(message.get("error"), dict):
            error = message["error"]
            raise McpError(
                f"{self.name}: {error.get('message', 'JSON-RPC error')}",
                code=error.get("code"),
                data=error.get("data"),
            )
        return message.get("result", {})

    # -- protocol -----------------------------------------------------------

    def handshake(self, timeout: float | None = None) -> dict[str, Any]:
        """POST initialize + notifications/initialized; store the session.

        The session id is captured ONLY from the initialize response: 400s
        mint bogus session headers that must never be trusted.
        """
        request_id = self._next_id
        self._next_id += 1
        self._session = {"session_id": None, "protocol_version": PROTOCOL_VERSION}
        response = self._post(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "initialize",
                "params": {
                    "protocolVersion": PROTOCOL_VERSION,
                    "clientInfo": CLIENT_INFO,
                    "capabilities": {},
                },
            },
            timeout=timeout,
        )
        self._raise_for_status(response)
        result = self._decode(response, request_id)
        session_id = response.headers.get("mcp-session-id")
        # A newer negotiated protocolVersion is accepted, not an error.
        negotiated = result.get("protocolVersion") or PROTOCOL_VERSION
        self._store_session(
            {"session_id": session_id, "protocol_version": negotiated, "stateless": not session_id}
        )
        ack = self._post(
            {"jsonrpc": "2.0", "method": "notifications/initialized"}, timeout=timeout
        )
        if ack.status_code >= 400:
            raise TransportError(
                f"{self.name}: notifications/initialized failed with HTTP {ack.status_code}"
            )
        self.init_result = result
        return result

    def rpc(self, method: str, params: dict[str, Any] | None = None, timeout: float | None = None) -> Any:
        """Optimistic JSON-RPC request: ~1 HTTP round trip in steady state."""
        request_id = self._next_id
        self._next_id += 1
        payload: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            payload["params"] = params
        response = self._post(payload, timeout=timeout)
        if self._is_session_rejection(response):
            # The server provably did not execute: handshake and retry once.
            self.handshake(timeout=timeout)
            response = self._post(payload, timeout=timeout)
        self._raise_for_status(response)
        return self._decode(response, request_id)

    def collect(self, method: str, key: str, timeout: float | None = None) -> list[dict[str, Any]]:
        """Run a paginated list method (tools/list, resources/list) to completion."""
        items: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            params = {"cursor": cursor} if cursor else {}
            result = self.rpc(method, params, timeout=timeout)
            items.extend(result.get(key, []))
            cursor = result.get("nextCursor")
            if not cursor:
                return items
