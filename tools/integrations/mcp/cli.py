"""CLI for the generic MCP bridge.

Exit codes: 0 ok · 1 tool/protocol error · 2 usage/config · 3 not granted in
this channel · 4 transport/rate limit.
"""

from dotenv import load_dotenv

load_dotenv()

import json
import os
import sys
from typing import Any

import typer

try:
    from .client import McpBridgeClient, RegistryError, _load_registry
    from ._wire import NotGrantedError, RateLimitedError, TransportError
except ImportError:  # flat module context (pytest with CWD on sys.path)
    from client import McpBridgeClient, RegistryError, _load_registry  # type: ignore[no-redef]
    from _wire import NotGrantedError, RateLimitedError, TransportError  # type: ignore[no-redef]

_ERROR_EXIT_CODES = {"tool_error": 1, "not_granted_in_this_channel": 3}


def _configured_servers_hint() -> str:
    try:
        names = ", ".join(sorted(_load_registry(os.environ.get("CENTAUR_MCP_SERVERS", ""))))
        return f"Configured servers: {names}."
    except RegistryError as exc:
        return f"({exc})"


app = typer.Typer(
    name="mcp",
    help=(
        "Bridge to configured MCP servers: discover any server's tools at "
        f"call time and call them. {_configured_servers_hint()} "
        "Start with `mcp servers` for live granted/not-granted status."
    ),
    no_args_is_help=True,
)


def _finish(envelope: dict[str, Any]) -> None:
    print(json.dumps(envelope, indent=2, ensure_ascii=False, default=str))
    if envelope.get("ok"):
        raise typer.Exit(0)
    code = envelope.get("exit_code") or _ERROR_EXIT_CODES.get(str(envelope.get("error")), 1)
    raise typer.Exit(int(code))


def _run(action) -> None:
    client = McpBridgeClient()
    try:
        _finish(action(client))
    except NotGrantedError as exc:
        # Discovery verbs (tools/schema/info/resources) hit denials too: same
        # envelope as `call`, minus the denial-marker caching.
        _finish(client._denial(exc.server, exc))
    except RegistryError as exc:
        _finish({"ok": False, "error": "config", "message": str(exc), "exit_code": 2})
    except RateLimitedError as exc:
        _finish(
            {
                "ok": False,
                "error": "rate_limited",
                "message": f"{exc}: respect Retry-After, do not retry in a loop",
                "retry_after": exc.retry_after,
                "exit_code": 4,
            }
        )
    except TransportError as exc:
        _finish({"ok": False, "error": "transport", "message": str(exc), "exit_code": 4})


def _parse_args(args_json: str | None, pairs: list[str]) -> dict[str, Any]:
    if args_json and pairs:
        raise typer.BadParameter("use either --args or -p, not both")
    if args_json is not None:
        if args_json == "-":
            # Agent exec interfaces usually run commands with no stdin attached,
            # so an unpiped `--args -` reads "" and the JSON error that follows
            # is cryptic. Teach the fix instead (observed in prod, 2026-08-29).
            text = "" if sys.stdin.isatty() else sys.stdin.read()
            if not text.strip():
                raise typer.BadParameter(
                    "--args - reads JSON from piped stdin, but stdin was empty. "
                    "Pass the arguments inline instead: "
                    "--args '{\"key\": \"value\", ...}' or -p key=value"
                )
        else:
            text = args_json
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise typer.BadParameter(f"--args must be valid JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise typer.BadParameter("--args must decode to a JSON object")
        return parsed
    arguments: dict[str, Any] = {}
    for pair in pairs:
        key, separator, value = pair.partition("=")
        if not separator or not key:
            raise typer.BadParameter(f"-p expects key=value, got {pair!r}")
        try:
            arguments[key] = json.loads(value)
        except json.JSONDecodeError:
            arguments[key] = value
    return arguments


@app.command()
def servers() -> None:
    """List configured servers with live granted/not-granted status (probes each)."""
    _run(lambda client: client.servers())


@app.command()
def tools(
    server: str = typer.Argument(..., help="Server name from `mcp servers`"),
    filter: str | None = typer.Option(None, "--filter", "-f", help="Substring filter"),
    full: bool = typer.Option(False, "--full", help="Full tool entries, not compact"),
    refresh: bool = typer.Option(False, "--refresh", help="Bypass the inventory cache"),
) -> None:
    """List a server's tools (live tools/list, compact by default)."""
    _run(lambda client: client.tools(server, filter=filter, full=full, refresh=refresh))


@app.command()
def schema(
    server: str = typer.Argument(..., help="Server name from `mcp servers`"),
    tool: str = typer.Argument(..., help="Tool name from `mcp tools <server>`"),
) -> None:
    """Show one tool's full input/output schema and annotations."""
    _run(lambda client: client.schema(server, tool))


@app.command()
def call(
    server: str = typer.Argument(..., help="Server name from `mcp servers`"),
    tool: str = typer.Argument(..., help="Tool name from `mcp tools <server>`"),
    args_positional: str | None = typer.Argument(
        None,
        metavar="[ARGS_JSON]",
        help="Tool arguments as inline JSON (same as --args)",
    ),
    args: str | None = typer.Option(
        None, "--args", help="Arguments as inline JSON ('-' reads piped stdin: requires an actual pipe)"
    ),
    param: list[str] = typer.Option([], "--param", "-p", help="key=value argument (repeatable)"),  # noqa: B008
    timeout: float = typer.Option(120.0, "--timeout", help="Per-call timeout in seconds"),
    raw: bool = typer.Option(False, "--raw", help="Return the undecoded MCP result"),
) -> None:
    """Call one tool; the server's output nests under server_result.

    Arguments go inline as JSON, positionally or via --args:
    `mcp call clickhouse run_query '{"query": "SELECT 1"}'`
    """
    if args_positional is not None and args is not None:
        raise typer.BadParameter("pass the JSON positionally or via --args, not both")
    arguments = _parse_args(args if args is not None else args_positional, param)
    _run(lambda client: client.call(server, tool, args=arguments, timeout=timeout, raw=raw))


@app.command()
def info(
    server: str = typer.Argument(..., help="Server name from `mcp servers`"),
) -> None:
    """Server identity, protocol version, and its own usage documentation."""
    _run(lambda client: client.info(server))


@app.command()
def resources(
    server: str = typer.Argument(..., help="Server name from `mcp servers`"),
) -> None:
    """List a server's MCP resources."""
    _run(lambda client: client.resources(server))


@app.command("read-resource")
def read_resource(
    server: str = typer.Argument(..., help="Server name from `mcp servers`"),
    uri: str = typer.Argument(..., help="Resource uri from `mcp resources <server>`"),
) -> None:
    """Read one MCP resource by uri."""
    _run(lambda client: client.read_resource(server, uri))


if __name__ == "__main__":
    app()
