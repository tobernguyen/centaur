# mcp: a generic, registry-native MCP client tool

One native Centaur tool that makes any number of external MCP servers usable
from sandboxes, with dynamic discovery instead of hardcoding. Running in
production since 2026-08-28 (self-hosted deployment, iron-control managed
mode, six internal servers behind an authenticated gateway). It replaced our
CODEX_CONFIG_OVERLAY workaround from
[#1176](https://github.com/paradigmxyz/centaur/issues/1176) entirely.

```
mcp servers                                # every configured server + LIVE granted/not-granted status
mcp tools <server> [--filter S] [--full]   # dynamic tools/list, compact by default
mcp schema <server> <tool>                 # one tool's full input/output schema
mcp call <server> <tool> '{"arg": "v"}'    # call it (args inline as JSON, or --args / -p k=v)
mcp info <server>                          # serverInfo + negotiated version + server-shipped usage docs
mcp resources <server> / mcp read-resource <server> <uri>
```

## Why a tool instead of harness MCP config

- **Harness-agnostic.** Overlay-delivered MCP config is per-harness (codex TOML
  merges work; claude-code settings can enable but not define servers, see the
  #1176 discussion). A native tool works the same under every harness.
- **Discoverable.** The agent sees `mcp` in its tool registry, and `mcp servers`
  answers "what do I have here?" with live per-server status. This kills the
  failure class where the agent claims it has no access while access works.
- **No boot cost.** Harness MCP connects to every server at sandbox boot
  (about 10s for our six). The tool connects lazily, per call, roughly one
  HTTP request in steady state because session ids and inventories are cached
  on disk between invocations.
- **Observable.** Tool-shim JSONL analytics come free, plus one
  `component:"mcp_bridge"` event per call (server, tool, duration, outcome).
- **Fail-loud config.** The registry env is parsed strictly. A malformed or
  missing registry is a named error, never a silently empty server list.

## Configuration

The server registry rides one env var, `CENTAUR_MCP_SERVERS` (TOML), delivered
via the deployment's sandbox env (e.g. `SESSION_SANDBOX_EXTRA_ENV`):

```toml
[servers.clickhouse]
url = "https://mcp-gateway.internal.example/mcp/clickhouse"
note = "main data warehouse"   # help text only, never an access control
```

Adding a server is one reviewed stanza: no prompt edits, no code, no secrets.
Auth is the proxy's job. iron-proxy injects credentials on host+path, so
`[tool.centaur]` declares `secrets = []` and the client never sends an
Authorization header. Access control stays at the credential: a server the
channel's principal isn't granted shows `not_granted_in_this_channel`
(distinguished from auth outages by fingerprinting gateway-authored 401/403s).

A one-line system prompt hint helps unprompted discovery. Ours, verbatim shape:

> Start from `mcp servers`: it lists every internal MCP server with its live
> granted/not-granted status in this channel, then `mcp tools <server>` and
> `mcp call <server> <tool> '{"arg": "value", ...}'`.

## Transport notes

Speaks Streamable HTTP JSON-RPC, POST only, and handles the two live profiles
we've met in the wild: FastMCP (SSE responses, stateful sessions, including
its bogus session headers on 400s and JSON-encoded-string `structuredContent`)
and rmcp (plain JSON, stateless, may negotiate a newer protocolVersion).
Optimistic call first: with a cached or absent session it goes straight to the
request, then handshakes and retries exactly once on a session-shaped
rejection. Auto-retry happens only on signals proving the server did not
execute; timeouts and 5xx on calls surface as "outcome unknown". Cursor
pagination on list methods. Exit codes: 0 ok, 1 tool/protocol error,
2 usage/config, 3 not granted, 4 transport.

Server-provided text (instructions, descriptions, results) always nests under
`server_result` / `server_provided_docs` in the output envelope. Envelope
fields are tool-authored only.

## Tests

Offline and standalone (`cd tools/integrations/mcp && python -m pytest`):
46 tests replaying golden fixtures of both transport profiles, captured from
live servers. No dependency on `centaur_sdk`.
