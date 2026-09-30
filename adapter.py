"""Rowboat Spaces platform adapter for Hermes Agent.

Connects this Hermes to a Rowboat org as one agent member, over the Rowboat
agent contract (Harbor spec §8, "Invoking agent members"): Spaces decides when
the agent is invoked (an @mention by someone it shares a space with), holds a
queue per thread, and delivers each invocation here. This adapter turns it
into a Hermes turn and reports back — working, done, failed or cancelled —
while Hermes answers in the thread as the agent.

- One Spaces conversation (a thread) is one Hermes chat: chat_id is
  ``<spaceId>/<threadRootId>`` with thread_id the root, so every thread,
  DMs included, has one session shared by everyone in it. A chat_id that is
  only ``<spaceId>`` (the home channel) posts at the top of that space.
- Who may talk to this Hermes is Hermes's own per-platform rule
  (ROWBOAT_ALLOWED_USERS: Rowboat member ids; ROWBOAT_ALLOW_ALL_USERS: anyone
  Spaces lets invoke the agent), on top of Spaces' rule for who may invoke it.
  Hermes commands (/model, /reload-mcp, …) are never run from Spaces, except
  for the agent's owner when ROWBOAT_OWNER_COMMANDS is on.
- What Hermes does that Spaces also has is done in Spaces too: its 👀/✅/❌
  land on the message as the agent's reactions, its typing is typing in the
  thread, its status phrase is the invocation's activity line.
- A turn is done when Hermes releases the thread's session, not on ✅ alone.

Settings (env, or ``platforms.rowboat.extra``): ROWBOAT_URL (the org's
address), ROWBOAT_AGENT_KEY (the agent's key), ROWBOAT_HOME_CHANNEL
(optional: where scheduled results go), ROWBOAT_ALLOWED_USERS /
ROWBOAT_ALLOW_ALL_USERS (who may talk to it), ROWBOAT_OWNER_COMMANDS (the
owner may run Hermes commands from Spaces).
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from gateway.config import Platform
from gateway.platforms._shared import extra_or_secret, get_scoped_secret, seed_extra_from_env
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent, MessageType, ProcessingOutcome
from gateway.platforms.helpers import MessageDeduplicator, cancel_task

logger = logging.getLogger(__name__)

PLATFORM = "rowboat"
IN_PROGRESS = "👀"
# Re-list pending invocations and re-check the key this often: the contract's
# guarantee beside the live frame (Harbor spec §8, Delivery).
LIST_EVERY_S = 60.0
# Hermes's typing tick is ~2 s; Spaces holds a typing lease for 45 s.
TYPING_EVERY_S = 10.0
# Report `working` at least this often so Spaces' 30-minute silence rule never fails a live turn.
HEARTBEAT_EVERY_S = 300.0
MAX_MESSAGE_LENGTH = 16_000
_TRUTHY = {"1", "true", "yes", "on"}
_TOKEN = re.compile(r"\[([^\]]*)\]\(#([a-z]+)(?::([^)\s]+))?\)")


def _plain(body: str, self_id: str) -> str:
    """Mention and space tokens read as their labels; the agent's own mention is dropped, as
    Hermes's Slack adapter does, so "@Hermes /help" reaches it as "/help"."""

    def repl(m: re.Match) -> str:
        return "" if m.group(2) == "member" and m.group(3) == self_id else m.group(1)

    return re.sub(r"[ \t]{2,}", " ", _TOKEN.sub(repl, body)).strip()


def _chat(chat_id: str) -> tuple[str, Optional[str]]:
    space, _, root = str(chat_id).partition("/")
    return space, (root or None)


@dataclass
class _Turn:
    invocation_id: str
    message_id: str
    chat_id: str
    source: Any
    replied: bool = False
    outcome: Optional[ProcessingOutcome] = None
    activity: Optional[str] = None
    typing_at: float = 0.0
    reported_at: float = field(default_factory=time.monotonic)


class RowboatAdapter(BasePlatformAdapter):
    """One agent member of one Rowboat org, reached over its agent key."""

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH
    supports_code_blocks = True
    supports_status_text = True
    # The base on_processing_complete swaps 👀 for these through _add/_remove_reaction.
    _ACK_EMOJI = IN_PROGRESS
    _OK_EMOJI = "✅"
    _FAIL_EMOJI = "❌"

    def __init__(self, config, **kwargs):
        super().__init__(config=config, platform=Platform(PLATFORM))
        extra = getattr(config, "extra", None)
        # Explicit env (profile-scoped) → this profile's config.extra → default (the guide's rule).
        self.base_url = (extra_or_secret(extra, "url", "ROWBOAT_URL") or "").strip().rstrip("/")
        self.agent_key = (extra_or_secret(extra, "agent_key", "ROWBOAT_AGENT_KEY") or "").strip()
        # Off unless asked for: people who can mention the agent are not its owner.
        self.owner_commands = str(extra_or_secret(extra, "owner_commands", "ROWBOAT_OWNER_COMMANDS") or "").strip().lower() in _TRUTHY
        self._http = None
        self._ws = None
        self._live_task: Optional[asyncio.Task] = None
        self._list_task: Optional[asyncio.Task] = None
        self._me: Dict[str, Any] = {}
        self._names: Dict[str, str] = {}
        self._spaces: Dict[str, Dict[str, Any]] = {}
        self._turns: Dict[str, _Turn] = {}  # session key → the turn Hermes is running there
        self._by_invocation: Dict[str, str] = {}  # invocation id → session key
        self._typing: set[str] = set()  # chats shown as typing, so idle is sent once
        # Invocations can arrive twice (the live frame and the minute's list); the shared deduplicator
        # also carries its ids over when Hermes swaps in a fresh adapter on reconnect.
        self._dedup = MessageDeduplicator(ttl_seconds=3600)
        self._deliver_lock = asyncio.Lock()
        self._tasks: set[asyncio.Task] = set()  # frame handlers and turn watchers, cancelled on disconnect

    @property
    def name(self) -> str:
        return "Rowboat"

    def _fail(self, code: str, message: str, *, retryable: bool) -> bool:
        self._set_fatal_error(code, message, retryable=retryable)
        return False

    # --- connection ---------------------------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not self.base_url or not self.agent_key:
            return self._fail("config_missing", "ROWBOAT_URL and ROWBOAT_AGENT_KEY must be set", retryable=False)
        identity = f"{self.base_url}#{hashlib.sha256(self.agent_key.encode()).hexdigest()[:16]}"
        # One key, one consumer: two profiles on the same key would both answer every mention.
        if not self._acquire_platform_lock(PLATFORM, identity, "Rowboat agent key"):
            return False
        import httpx

        self._http = httpx.AsyncClient(
            base_url=self.base_url, headers={"authorization": f"Bearer {self.agent_key}"}, timeout=30.0
        )
        try:
            res = await self._http.get("/v1/me")
        except Exception as e:  # noqa: BLE001
            return self._fail("connect_failed", f"Rowboat at {self.base_url} is unreachable: {e}", retryable=True)
        if res.status_code == 401:
            return self._fail("unauthorized", "The Rowboat agent key was not accepted (unknown or revoked)", retryable=False)
        if res.status_code >= 400:
            return self._fail("connect_failed", f"Rowboat answered {res.status_code} to /v1/me", retryable=True)
        self._me = (res.json() or {}).get("member") or {}
        if self._me.get("kind") != "agent":
            return self._fail("not_an_agent", "ROWBOAT_AGENT_KEY is not an agent key", retryable=False)
        # Spaces may offer Stop: Hermes's own /stop cancels a running turn (see _stop).
        await self._api("POST", "/v1/agent/capabilities", {"stop": True, "options": []})
        self._live_task = asyncio.create_task(self._live_loop())
        self._list_task = asyncio.create_task(self._list_loop())
        self._mark_connected()
        self._wire_plugin_handlers(None)
        logger.info("Rowboat: connected to %s as %s (%s)", self.base_url, self._me.get("displayName"), self._me.get("id"))
        return True

    async def disconnect(self) -> None:
        with contextlib.suppress(Exception):
            self._release_platform_lock()
        self._mark_disconnected()
        await cancel_task(self._live_task)
        await cancel_task(self._list_task)
        for task in list(self._tasks):
            await cancel_task(task)
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()
        self._ws = None
        if self._http is not None:
            with contextlib.suppress(Exception):
                await self._http.aclose()
        self._http = None

    async def _api(self, method: str, path: str, body: Optional[dict] = None, *, quiet: bool = True) -> Optional[dict]:
        """One call on the agent's key; None on any failure (logged), the body otherwise."""
        if self._http is None:
            return None
        try:
            res = await self._http.request(method, path, json=body) if body is not None else await self._http.request(method, path)
        except Exception as e:  # noqa: BLE001
            logger.warning("Rowboat: %s %s failed: %s", method, path, e)
            return None
        if res.status_code == 401:
            self._set_fatal_error("unauthorized", "The Rowboat agent key was revoked", retryable=False)
            return None
        if res.status_code >= 400:
            (logger.debug if quiet else logger.warning)("Rowboat: %s %s → %s %s", method, path, res.status_code, res.text[:200])
            return None
        with contextlib.suppress(Exception):
            return res.json()
        return {}

    async def _live_loop(self) -> None:
        """The live frame is the fast path: invocations and stops as they happen."""
        import websockets

        url = re.sub(r"^http", "ws", self.base_url) + "/v1/live"
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(url, additional_headers={"Authorization": f"Bearer {self.agent_key}"}) as ws:
                    self._ws = ws
                    backoff = 1.0
                    await self._list()  # anything that arrived while we were away
                    async for raw in ws:
                        # A delivery can wait (startup restore, the thread's context); a stop must not wait behind it.
                        with contextlib.suppress(Exception):
                            self._spawn(self._on_frame(json.loads(raw)))
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                logger.info("Rowboat: live connection lost (%s); retrying in %.0fs", e, backoff)
            self._ws = None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    async def _list_loop(self) -> None:
        """The list is the guarantee: pending invocations every minute."""
        while True:
            await asyncio.sleep(LIST_EVERY_S)
            await self._list()

    async def _list(self) -> None:
        listed = await self._api("GET", "/v1/agent/invocations")
        for invocation in (listed or {}).get("invocations", []):
            await self._deliver(invocation)

    async def _on_frame(self, frame: dict) -> None:
        kind = frame.get("kind")
        if kind == "invocation":
            await self._deliver(frame.get("invocation") or {})
        elif kind == "invocation_stop":
            await self._stop(str(frame.get("invocationId") or ""))

    # --- invocations in -----------------------------------------------------------

    async def _startup_gate(self) -> None:
        """Hermes queues what arrives while it restores sessions at startup and replays it later;
        a turn delivered into that window would read as finished before it ran."""
        runner = getattr(getattr(self, "_message_handler", None), "__self__", None)
        for _ in range(240):
            if not getattr(runner, "_startup_restore_in_progress", False):
                return
            await asyncio.sleep(0.5)

    async def _deliver(self, invocation: dict) -> None:
        async with self._deliver_lock:
            inv_id = str(invocation.get("id") or "")
            if invocation.get("state") != "pending" or not inv_id or self._dedup.contains(inv_id):
                return
            await self._startup_gate()
            conversation = invocation.get("conversation") or {}
            trigger = invocation.get("trigger") or {}
            where = invocation.get("where") or {}
            space, root = str(conversation.get("spaceId")), str(conversation.get("threadRootId"))
            chat_id = f"{space}/{root}"
            direct = where.get("spaceKind") == "direct"
            author = str(trigger.get("authorId") or "")
            source = self.build_source(
                chat_id=chat_id,
                chat_name=None if direct else where.get("spaceName"),
                chat_type="dm" if direct else "group",
                user_id=author,
                user_name=await self._name(author),
                thread_id=root,
                message_id=str(trigger.get("messageId") or ""),
            )
            key = self._source_session_key(source)
            if key in self._turns or key in self._active_sessions:
                return  # its thread is busy here; Spaces holds it pending, the next list delivers it
            if self._dedup.is_duplicate(inv_id):
                return
            if await self._api("POST", f"/v1/agent/invocations/{inv_id}/ack") is None:
                # Cancelled on its way (then it is never pending again), or a failed call: the list retries.
                self._dedup.discard(inv_id)
                return
            turn = _Turn(invocation_id=inv_id, message_id=str(trigger.get("messageId") or ""), chat_id=chat_id, source=source)
            self._turns[key] = turn
            self._by_invocation[inv_id] = key
            event = MessageEvent(
                text=_plain(str(trigger.get("body") or ""), str(self._me.get("id") or "")),
                message_type=MessageType.TEXT,
                source=source,
                message_id=turn.message_id,
                channel_context=await self._thread_context(space, root, turn.message_id),
                # Hermes commands only from the agent's owner, and only when the owner turned them on.
                allow_gateway_control=self.owner_commands and bool(author) and author == self._me.get("ownerId"),
                metadata={"rowboat_invocation": inv_id},
            )
            await self.handle_message(event)
            self._spawn(self._watch(key, turn))

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _watch(self, key: str, turn: _Turn) -> None:
        """The turn is over when Hermes releases the thread's session (no hook fires after that)."""
        await asyncio.sleep(0.2)
        while key in self._active_sessions:
            await asyncio.sleep(0.5)
        await self._finish(key, turn)

    async def _finish(self, key: str, turn: _Turn) -> None:
        if self._turns.get(key) is turn:
            del self._turns[key]
        self._by_invocation.pop(turn.invocation_id, None)
        await self._idle(turn.chat_id)
        if turn.outcome == ProcessingOutcome.CANCELLED:
            update = {"state": "cancelled"}
        elif turn.outcome == ProcessingOutcome.FAILURE:
            update = {"state": "failed", "error": "Hermes could not finish this turn"}
        elif turn.outcome == ProcessingOutcome.SUCCESS or turn.replied:
            update = {"state": "done"}
        else:
            update = {"state": "failed", "error": "Hermes did not take the message"}
        await self._api("POST", f"/v1/agent/invocations/{turn.invocation_id}/update", update, quiet=False)
        # The thread is free: anything queued behind this turn is pending in Spaces now.
        await self._list()

    async def _stop(self, invocation_id: str) -> None:
        key = self._by_invocation.get(invocation_id)
        turn = self._turns.get(key) if key else None
        if not turn or key not in self._active_sessions:
            return
        # Hermes's own /stop: a hard interrupt of that session's turn, then CANCELLED.
        await self.handle_message(
            MessageEvent(text="/stop", message_type=MessageType.COMMAND, source=turn.source, allow_gateway_control=True)
        )

    async def _thread_context(self, space: str, root: str, trigger_id: str) -> Optional[str]:
        """What the thread said since the agent last spoke there: Hermes only hears its mentions."""
        if trigger_id == root:
            return None
        page = await self._api("GET", f"/v1/spaces/{space}/threads/{root}?limit=100")
        if not page:
            return None
        thread = list(page.get("messages") or [])
        if not page.get("hasMore") and page.get("root"):
            thread.insert(0, page["root"])
        at = next((i for i, m in enumerate(thread) if m.get("id") == trigger_id), None)
        if at is None:
            return None
        before = thread[:at]
        own = [i for i, m in enumerate(before) if (m.get("author") or {}).get("memberId") == self._me.get("id")]
        lines = []
        for m in before[(own[-1] + 1) if own else 0:][-50:]:
            body = str(m.get("body") or "")
            if body:
                lines.append(f"{await self._name((m.get('author') or {}).get('memberId', ''))}: {_plain(body, str(self._me.get('id') or ''))}")
        return "[Earlier in this thread]\n" + "\n".join(lines) if lines else None

    async def _name(self, member_id: str) -> Optional[str]:
        if member_id and member_id not in self._names:
            roster = await self._api("GET", "/v1/members")
            self._names.update({m["id"]: m.get("displayName", "") for m in (roster or {}).get("members", [])})
        return self._names.get(member_id)

    # --- Hermes's actions out -------------------------------------------------------

    def _turn_for_chat(self, chat_id: str) -> Optional[_Turn]:
        return next((t for t in self._turns.values() if t.chat_id == chat_id), None)

    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None):
        space, root = _chat(chat_id)
        body = {"body": (content or "").strip() or "…", "actingMode": "direct", **({"threadRoot": root} if root else {})}
        posted = await self._api("POST", f"/v1/spaces/{space}/messages", body, quiet=False)
        if not posted:
            return SendResult(success=False, error="Rowboat did not accept the message", retryable=True)
        turn = self._turn_for_chat(chat_id)
        if turn:
            turn.replied = True
        return SendResult(success=True, message_id=(posted.get("message") or {}).get("id"))

    async def edit_message(self, chat_id: str, message_id: str, content: str, *, finalize: bool = False):
        space, _ = _chat(chat_id)
        body = {"body": (content or "").strip() or "…", "actingMode": "direct"}
        ok = await self._api("POST", f"/v1/spaces/{space}/messages/{message_id}/edit", body)
        return SendResult(success=ok is not None, message_id=message_id, **({} if ok is not None else {"error": "edit failed"}))

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Typing in the thread, and Hermes's status phrase as the invocation's activity line."""
        turn = self._turn_for_chat(chat_id)
        if not turn:
            return
        space, root = _chat(chat_id)
        now = time.monotonic()
        if now - turn.typing_at >= TYPING_EVERY_S:
            turn.typing_at = now
            self._typing.add(chat_id)
            await self._presence(space, root, "typing")
        phrase = (self._status_text.get(chat_id) or "").strip() or None
        changed = phrase is not None and phrase != turn.activity
        if changed or now - turn.reported_at >= HEARTBEAT_EVERY_S:
            turn.activity = phrase or turn.activity
            turn.reported_at = now
            update = {"state": "working", **({"activity": turn.activity[:200]} if turn.activity else {})}
            await self._api("POST", f"/v1/agent/invocations/{turn.invocation_id}/update", update)

    async def stop_typing(self, chat_id: str, metadata=None) -> None:
        await self._idle(chat_id)

    async def _idle(self, chat_id: str) -> None:
        """Hermes calls stop_typing several times as a turn winds down; Spaces needs one idle."""
        if chat_id in self._typing:
            self._typing.discard(chat_id)
            space, root = _chat(chat_id)
            await self._presence(space, root, "idle")
        turn = self._turn_for_chat(chat_id)
        if turn:
            turn.typing_at = 0.0

    async def _presence(self, space: str, root: Optional[str], state: str) -> None:
        if root and self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.send(json.dumps({"kind": "presence", "spaceId": space, "state": state, "threadRootId": root}))

    async def on_processing_start(self, event: MessageEvent) -> None:
        if event.message_id and event.source:
            await self._add_reaction(event.source.chat_id, event.message_id, IN_PROGRESS)

    async def on_processing_complete(self, event: MessageEvent, outcome: ProcessingOutcome) -> None:
        turn = self._turns.get(self._event_session_key(event)) if event.source else None
        if turn and event.message_id == turn.message_id:
            turn.outcome = outcome
        # The base swaps 👀 for ✅/❌ (and only removes it on CANCELLED).
        await super().on_processing_complete(event, outcome)

    async def _add_reaction(self, chat_id: str, message_id: str, emoji: str) -> None:
        await self._react(chat_id, message_id, emoji, "add")

    async def _remove_reaction(self, chat_id: str, message_id: str) -> None:
        await self._react(chat_id, message_id, IN_PROGRESS, "remove")

    async def _react(self, chat_id: str, message_id: str, emoji: str, action: str) -> None:
        space, _ = _chat(chat_id)
        await self._api("POST", f"/v1/spaces/{space}/messages/{message_id}/reactions", {"emoji": emoji, "action": action, "actingMode": "direct"})

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        space, _ = _chat(chat_id)
        if space not in self._spaces:
            listed = await self._api("GET", "/v1/spaces?includeDirect=true")
            self._spaces.update({s["id"]: s for s in (listed or {}).get("spaces", [])})
        info = self._spaces.get(space) or {}
        return {"name": info.get("name") or space, "type": "dm" if info.get("kind") == "direct" else "group"}


# --- registration -------------------------------------------------------------------


def _settings(config=None) -> tuple[str, str]:
    extra = getattr(config, "extra", None)
    return (
        (extra_or_secret(extra, "url", "ROWBOAT_URL") or "").strip(),
        (extra_or_secret(extra, "agent_key", "ROWBOAT_AGENT_KEY") or "").strip(),
    )


def check_requirements() -> bool:
    """Passive: configured, and the libraries every Hermes ships with are importable."""
    try:
        import httpx  # noqa: F401
        import websockets  # noqa: F401
    except ImportError:
        return False
    return bool(get_scoped_secret("ROWBOAT_URL", "").strip() and get_scoped_secret("ROWBOAT_AGENT_KEY", "").strip())


def validate_config(config) -> bool:
    url, key = _settings(config)
    return bool(url and key)


def is_connected(config) -> bool:
    return validate_config(config)


def _env_enablement() -> dict | None:
    """Seed ``extra`` and the home channel from the profile's env, so env-only setups are enabled."""
    if not (get_scoped_secret("ROWBOAT_URL", "").strip() and get_scoped_secret("ROWBOAT_AGENT_KEY", "").strip()):
        return None
    return seed_extra_from_env(
        (("ROWBOAT_URL", "url", None), ("ROWBOAT_AGENT_KEY", "agent_key", None)),
        home_env="ROWBOAT_HOME_CHANNEL",
    )


async def _standalone_send(pconfig, chat_id, message, *, thread_id=None, media_files=None, force_document=False):
    """Cron delivery from a process without the live gateway: one post on the agent's key."""
    import httpx

    url, key = _settings(pconfig)
    if not (url and key):
        return {"error": "ROWBOAT_URL and ROWBOAT_AGENT_KEY must be set"}
    space, root = _chat(chat_id)
    root = root or thread_id
    body = {"body": (message or "").strip() or "…", "actingMode": "direct", **({"threadRoot": root} if root else {})}
    async with httpx.AsyncClient(base_url=url.rstrip("/"), headers={"authorization": f"Bearer {key}"}, timeout=30.0) as client:
        res = await client.post(f"/v1/spaces/{space}/messages", json=body)
    if res.status_code >= 400:
        return {"error": f"Rowboat answered {res.status_code}: {res.text[:200]}"}
    return {"success": True, "message_id": (res.json().get("message") or {}).get("id")}


# The same settings SETUP.md has an agent save: the tools over MCP on the same key, and the quiet
# display defaults Hermes gives Slack. `${VAR}`s stay for Hermes to fill from .env.
_SETUP_CONFIG = (
    ("mcp_servers.rowboat.url", "${ROWBOAT_URL}/mcp"),
    ("mcp_servers.rowboat.headers.Authorization", "Bearer ${ROWBOAT_AGENT_KEY}"),
    ("display.platforms.rowboat.tool_progress", "off"),
    ("display.platforms.rowboat.show_reasoning", "false"),
    ("display.platforms.rowboat.long_running_notifications", "false"),
    ("display.platforms.rowboat.busy_ack_detail", "false"),
)


def interactive_setup() -> None:
    """``hermes gateway setup``: the org address, the agent key, a home channel, then the rest of the
    setup SETUP.md describes, so the wizard alone leaves a working connection."""
    from hermes_cli.config import save_env_value, set_config_value

    print("Connect this Hermes to Rowboat as an agent. Add the agent in Rowboat first (Agents → Add agent → Hermes);")
    print("it shows the org address and the key once, and the home channel.")
    for env, prompt in (("ROWBOAT_URL", "Rowboat org address"), ("ROWBOAT_AGENT_KEY", "Agent key (rbk_…)")):
        value = input(f"{prompt}: ").strip()
        if value:
            save_env_value(env, value)
    home = input("Home channel for scheduled results (optional): ").strip()
    if home:
        save_env_value("ROWBOAT_HOME_CHANNEL", home)
    save_env_value("ROWBOAT_ALLOW_ALL_USERS", "true")
    owner = input("Let the agent's owner run Hermes commands from Rowboat? [Y/n]: ").strip().lower()
    save_env_value("ROWBOAT_OWNER_COMMANDS", "false" if owner in {"n", "no"} else "true")
    for key, value in _SETUP_CONFIG:
        set_config_value(key, value)
    print("Saved. Restart the gateway for it to take effect: hermes gateway restart")


def register(ctx):
    """Plugin entry point: called by the Hermes plugin system."""
    ctx.register_platform(
        name=PLATFORM,
        label="Rowboat",
        adapter_factory=RowboatAdapter,
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["ROWBOAT_URL", "ROWBOAT_AGENT_KEY"],
        install_hint="No extra packages needed (httpx and websockets ship with Hermes)",
        setup_fn=interactive_setup,
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="ROWBOAT_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="ROWBOAT_ALLOWED_USERS",
        allow_all_env="ROWBOAT_ALLOW_ALL_USERS",
        max_message_length=MAX_MESSAGE_LENGTH,
        emoji="🚣",
        pii_safe=False,
        # People who can mention the agent in Spaces are not its owner.
        allow_update_command=False,
        platform_hint=(
            "You are a member of a Rowboat Space, a team chat. Each conversation is one thread and you "
            "reply in it; replies render as Markdown. Messages reach you when someone mentions you, with "
            "what the thread said since your last reply. To read further or act elsewhere in Spaces, use "
            "the rowboat tools if you have them."
        ),
    )
