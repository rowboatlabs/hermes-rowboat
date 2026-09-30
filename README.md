# Rowboat for Hermes

A [Hermes Agent](https://github.com/NousResearch/hermes-agent) platform plugin that connects a Hermes to [Rowboat](https://rowboatlabs.com) Spaces as a member of your team. People @mention it in a space; it answers in the thread, reads the thread for context, uses the Spaces tools, and shows what it is doing while it works.

It follows Hermes's guide for [adding a platform adapter](https://hermes-agent.nousresearch.com/docs/developer-guide/adding-platform-adapters) through the plugin path, and needs no packages beyond what every Hermes ships with. The connection is outbound only: a WebSocket from your Hermes to your Rowboat org, so it works behind NAT and on hosted Hermes.

## Setup

In Rowboat, open **Agents → Add agent → Hermes**. It creates the agent and shows the exact steps with your values filled in. The steps are:

**From a terminal where Hermes runs**

```sh
hermes config set ROWBOAT_URL 'https://acme.rowboatlabs.com'
hermes config set ROWBOAT_AGENT_KEY 'rbk_…'
hermes config set ROWBOAT_HOME_CHANNEL '<the id Rowboat shows>'
hermes config set ROWBOAT_ALLOW_ALL_USERS true
hermes config set mcp_servers.rowboat.url '${ROWBOAT_URL}/mcp'
hermes config set mcp_servers.rowboat.headers.Authorization 'Bearer ${ROWBOAT_AGENT_KEY}'
hermes config set display.platforms.rowboat.tool_progress off
hermes plugins install rowboatlabs/hermes-rowboat --enable
hermes gateway restart
```

**From the Hermes dashboard** (hosted Hermes, or no terminal)

1. **Plugins → Install from GitHub:** `rowboatlabs/hermes-rowboat`, with *Enable after install* on.
2. **Keys:** add `ROWBOAT_URL`, `ROWBOAT_AGENT_KEY`, `ROWBOAT_HOME_CHANNEL`, and `ROWBOAT_ALLOW_ALL_USERS` = `true`.
3. **MCP → Add server:** name `rowboat`, URL `<ROWBOAT_URL>/mcp`, auth *Bearer token* = the agent key.
4. **Config** (YAML mode): under the existing `display:`, add `platforms:` → `rowboat:` → `tool_progress: 'off'`.
5. **Restart Gateway.**

Then add the agent to a space in Rowboat and mention it.

## Settings

| Variable | |
|---|---|
| `ROWBOAT_URL` | Your Rowboat org's address. |
| `ROWBOAT_AGENT_KEY` | The agent's key (`rbk_…`). Rowboat shows it once, when the agent or a new key is made. |
| `ROWBOAT_HOME_CHANNEL` | Optional. Where Hermes sends what has no conversation of its own, such as a scheduled job's result. Rowboat sets it to your direct messages with the agent. Unset, Hermes asks for a home at the start of every new thread. |
| `ROWBOAT_ALLOW_ALL_USERS` | `true`: anyone Rowboat lets mention the agent may talk to it. Rowboat's own rule is anyone who shares a space with the agent. |
| `ROWBOAT_ALLOWED_USERS` | Optional, instead of the above: comma-separated Rowboat member ids allowed to talk to it. |

The `mcp_servers.rowboat` entry gives Hermes the Spaces tools (read threads, search, post, files, …) on the same key. `display.platforms.rowboat.tool_progress off` keeps Hermes's tool-progress messages out of threads; Rowboat shows the current step on the thread instead.

One Hermes profile is one Rowboat agent. The same agent can be added to any number of spaces in its org. To connect one Hermes to a second org, give that org its own Hermes [profile](https://hermes-agent.nousresearch.com/docs/user-guide/profiles) with its own agent key; two profiles on one key are refused.

## How it works

Rowboat decides when the agent is invoked (a mention), holds a queue per thread, and delivers each invocation to this plugin over its live connection, with a list every minute as the backstop. The plugin turns it into a Hermes turn:

- **One thread is one Hermes session**, shared by everyone in it, DMs included. The mention arrives with what the thread said since the agent last spoke there.
- **The answer is a message in the thread**, as the agent. Hermes's edits are edits.
- **Progress is Rowboat's:** Hermes's 👀 and ✅/❌ land on the message as the agent's reactions, its typing shows as typing in the thread, and its status phrase ("is running the tests…") shows on the thread. The invocation ends when Hermes releases the thread's session.
- **Stop in Rowboat is Hermes's own `/stop`.**

## Security

Anyone allowed to talk to the agent can make your Hermes act with its tools, on the machine it runs on. Choose who with `ROWBOAT_ALLOW_ALL_USERS` or `ROWBOAT_ALLOWED_USERS`, and consider a dedicated Hermes profile with restricted `platform_toolsets.rowboat` for a shared space. Messages from Rowboat can't run Hermes commands (`/model`, `/reset`, `/approve`, …): commands that need approval can't be approved from a space.

## Not yet

Media (images and files in either direction), Hermes's approval and clarify buttons (they fall back to text), and invocation options.

## Development

Drop the folder into `~/.hermes/plugins/rowboat-platform/`, run `hermes plugins enable rowboat-platform`, then `hermes gateway restart`. Check it with `hermes plugins validate .`.

Tests run on Hermes's own Python against a hermes-agent checkout, with the test tools kept outside this folder (the validator scans everything in it):

```sh
uv pip install --target /tmp/rowboat-testdeps --python ~/.hermes/hermes-agent/venv/bin/python "pytest>=8,<9" "pytest-asyncio>=0.24,<1"
PYTHONPATH=/tmp/rowboat-testdeps:$HOME/.hermes/hermes-agent ~/.hermes/hermes-agent/venv/bin/python -m pytest -p no:cacheprovider tests
```

## License

Apache-2.0.
