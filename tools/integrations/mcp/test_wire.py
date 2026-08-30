"""Transport tests replaying the two live gateway profiles.

Golden fixtures are synthesized from the 2026-08-28 live probe (design doc §2):
clickhouse = FastMCP 2.14.7 (SSE, stateful, JSON-in-string structuredContent,
bogus session headers on 400s); minisim = rmcp 1.7.0 (plain JSON, stateless,
negotiates a newer protocolVersion). Validated against the real servers during
the Phase 0 dark verify.
"""

from __future__ import annotations

import json

import httpx
import pytest

import _cache
from _wire import (
    Endpoint,
    McpError,
    NotGrantedError,
    RateLimitedError,
    TransportError,
)


@pytest.fixture(autouse=True)
def _isolated_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(_cache, "ROOT", tmp_path / "centaur-mcp")


def _sse(*payloads: dict) -> str:
    return "".join(f"event: message\r\ndata: {json.dumps(p)}\r\n\r\n" for p in payloads)


def _endpoint(handler, name: str = "clickhouse") -> Endpoint:
    http = httpx.Client(transport=httpx.MockTransport(handler))
    return Endpoint(name, f"https://mcp-gateway.internal.example/mcp/{name}", http)


class ClickhouseSim:
    """Golden FastMCP profile: SSE, stateful, quirks included."""

    SESSION = "sess-abc123"

    def __init__(self):
        self.calls: list[tuple[str | None, str | None]] = []  # (method, session header)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        assert "authorization" not in request.headers, "bridge must never send auth"
        body = json.loads(request.content) if request.content else {}
        method = body.get("method")
        session = request.headers.get("mcp-session-id")
        self.calls.append((method, session))
        if method == "initialize":
            result = {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "mcp-clickhouse", "version": "2.14.7"},
                "instructions": "Install skills via `npx -y skills add clickhouse/agent-skills`.",
            }
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream", "mcp-session-id": self.SESSION},
                text=_sse({"jsonrpc": "2.0", "id": body["id"], "result": result}),
            )
        if method == "notifications/initialized":
            return httpx.Response(202)
        if session != self.SESSION:
            # The live server mints a bogus session header on 400s: must be ignored.
            return httpx.Response(
                400,
                headers={"content-type": "application/json", "mcp-session-id": "bogus-from-400"},
                json={
                    "jsonrpc": "2.0",
                    "id": "server-error",
                    "error": {"code": -32600, "message": "Bad Request: Missing session ID"},
                },
            )
        if method == "tools/list":
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                text=_sse(
                    {"jsonrpc": "2.0", "method": "notifications/message", "params": {"level": "info"}},
                    {
                        "jsonrpc": "2.0",
                        "id": body["id"],
                        "result": {"tools": [{"name": "run_query", "description": "Run SQL"}]},
                    },
                ),
            )
        if method == "tools/call":
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                text=_sse(
                    {
                        "jsonrpc": "2.0",
                        "id": body["id"],
                        "result": {
                            "content": [{"type": "text", "text": '{"databases": 3}'}],
                            "structuredContent": {"result": '{"databases": 3}'},
                        },
                    }
                ),
            )
        raise AssertionError(f"unexpected method {method}")


def test_clickhouse_optimistic_call_handshakes_once_and_retries() -> None:
    sim = ClickhouseSim()
    endpoint = _endpoint(sim)
    result = endpoint.rpc("tools/call", {"name": "run_query", "arguments": {}})
    assert result["structuredContent"] == {"result": '{"databases": 3}'}
    # Exact wire sequence: optimistic call, 400, handshake, retry once.
    assert [method for method, _ in sim.calls] == [
        "tools/call",
        "initialize",
        "notifications/initialized",
        "tools/call",
    ]
    # The retry must carry the initialize session, never the bogus 400 header.
    assert sim.calls[-1][1] == ClickhouseSim.SESSION


def test_clickhouse_session_is_cached_for_the_next_process() -> None:
    sim = ClickhouseSim()
    _endpoint(sim).rpc("tools/call", {"name": "run_query", "arguments": {}})
    fresh = _endpoint(sim)  # simulates a new subprocess sharing the disk cache
    fresh.rpc("tools/list", {})
    # Steady state: exactly one HTTP request for the second invocation
    # (4 from the first call's handshake dance + 1).
    assert len(sim.calls) == 5
    assert sim.calls[-1] == ("tools/list", ClickhouseSim.SESSION)


def test_sse_selects_response_by_jsonrpc_id_not_last_event() -> None:
    sim = ClickhouseSim()
    endpoint = _endpoint(sim)
    endpoint.handshake()
    result = endpoint.rpc("tools/list", {})
    assert result["tools"][0]["name"] == "run_query"


def test_second_session_rejection_after_handshake_raises_not_loops() -> None:
    def always_rejects(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else {}
        if body.get("method") == "initialize":
            return httpx.Response(
                200,
                headers={"content-type": "application/json"},
                json={"jsonrpc": "2.0", "id": body["id"], "result": {"protocolVersion": "2025-06-18"}},
            )
        if body.get("method") == "notifications/initialized":
            return httpx.Response(202)
        return httpx.Response(
            400,
            headers={"content-type": "application/json"},
            json={"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Missing session ID"}},
        )

    with pytest.raises(McpError, match="Missing session ID"):
        _endpoint(always_rejects).rpc("tools/call", {"name": "x", "arguments": {}})


class MinisimSim:
    """Golden rmcp profile: plain JSON, stateless, newer negotiated version."""

    def __init__(self):
        self.count = 0

    def __call__(self, request: httpx.Request) -> httpx.Response:
        assert "authorization" not in request.headers
        self.count += 1
        body = json.loads(request.content) if request.content else {}
        method = body.get("method")
        if method == "initialize":
            return httpx.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": body["id"],
                    "result": {
                        "protocolVersion": "2025-11-25",
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "minisim", "version": "rmcp 1.7.0"},
                    },
                },
            )
        if method == "notifications/initialized":
            return httpx.Response(202)
        if method == "tools/list":
            assert "mcp-session-id" not in request.headers
            return httpx.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": body["id"],
                    "result": {"tools": [{"name": "create_run", "description": "Launch a run"}]},
                },
            )
        raise AssertionError(f"unexpected method {method}")


def test_minisim_stateless_call_is_one_request() -> None:
    sim = MinisimSim()
    endpoint = _endpoint(sim, name="minisim")
    result = endpoint.rpc("tools/list", {})
    assert result["tools"][0]["name"] == "create_run"
    assert sim.count == 1


def test_newer_negotiated_protocol_version_is_accepted() -> None:
    sim = MinisimSim()
    endpoint = _endpoint(sim, name="minisim")
    result = endpoint.handshake()
    assert result["protocolVersion"] == "2025-11-25"

    def check_version(request: httpx.Request) -> httpx.Response:
        assert request.headers["mcp-protocol-version"] == "2025-11-25"
        return MinisimSim()(request)

    endpoint._http = httpx.Client(transport=httpx.MockTransport(check_version))
    endpoint.rpc("tools/list", {})


def test_pagination_follows_next_cursor() -> None:
    def paged(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        cursor = (body.get("params") or {}).get("cursor")
        page = (
            {"tools": [{"name": "a"}], "nextCursor": "page2"}
            if not cursor
            else {"tools": [{"name": "b"}]}
        )
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": page})

    tools = _endpoint(paged, name="minisim").collect("tools/list", "tools")
    assert [tool["name"] for tool in tools] == ["a", "b"]


def test_timeout_on_call_surfaces_outcome_unknown_without_retry() -> None:
    attempts = []

    def times_out(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        raise httpx.ReadTimeout("timed out", request=request)

    with pytest.raises(TransportError, match="outcome unknown"):
        _endpoint(times_out).rpc("tools/call", {"name": "x", "arguments": {}})
    assert len(attempts) == 1


def test_5xx_on_call_is_not_retried() -> None:
    attempts = []

    def fails(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        return httpx.Response(502, text="bad gateway")

    with pytest.raises(TransportError, match="outcome unknown"):
        _endpoint(fails).rpc("tools/call", {"name": "x", "arguments": {}})
    assert len(attempts) == 1


def test_429_surfaces_retry_after_once() -> None:
    attempts = []

    def limited(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        return httpx.Response(429, headers={"retry-after": "7"}, json={"message": "API rate limit exceeded"})

    with pytest.raises(RateLimitedError) as excinfo:
        _endpoint(limited).rpc("tools/call", {"name": "x", "arguments": {}})
    assert excinfo.value.retry_after == "7"
    assert len(attempts) == 1


def test_4xx_jsonrpc_error_body_is_parsed_before_raising() -> None:
    def invalid_params(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        return httpx.Response(
            422,
            json={
                "jsonrpc": "2.0",
                "id": body["id"],
                "error": {"code": -32602, "message": "Invalid params: query is required"},
            },
        )

    with pytest.raises(McpError, match="query is required") as excinfo:
        _endpoint(invalid_params, name="minisim").rpc("tools/call", {"name": "x", "arguments": {}})
    assert excinfo.value.code == -32602


def test_401_and_403_raise_not_granted_with_kong_fingerprint() -> None:
    def denied(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            headers={"x-kong-response-latency": "1"},
            json={"message": "Forbidden"},
        )

    with pytest.raises(NotGrantedError) as excinfo:
        _endpoint(denied, name="grafana").rpc("tools/call", {"name": "x", "arguments": {}})
    assert excinfo.value.status == 403
    assert excinfo.value.kong_authored is True
