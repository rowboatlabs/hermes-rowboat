"""Tests for the Rowboat platform adapter (the guide's list: construction, message events, the send
path against a mocked API, and the platform's own behavior)."""

import asyncio
import json

import httpx
import pytest

from gateway.config import PlatformConfig
from gateway.platforms.event import MessageType, ProcessingOutcome

from rowboat_platform import adapter as mod

AGENT = {"id": "AG1", "displayName": "Hermes", "kind": "agent", "ownerId": "ram"}
SPACE, ROOT, TRIGGER = "SP1", "M1", "M3"


class FakeRowboat:
    """The slice of the Rowboat API the adapter calls, recording every request."""

    def __init__(self):
        self.calls = []
        self.listed = []
        self.thread = {
            "root": {"id": ROOT, "author": {"memberId": "ram"}, "body": "Deploy is at 3pm"},
            "messages": [
                {"id": "M2", "author": {"memberId": "harsh"}, "body": "Migration first"},
                {"id": TRIGGER, "author": {"memberId": "harsh"}, "body": "[@Hermes](#member:AG1) plan?"},
            ],
            "hasMore": False,
        }

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        self.calls.append((request.method, request.url.path, body))
        path = request.url.path
        if path == "/v1/me":
            return httpx.Response(200, json={"member": AGENT})
        if path == "/v1/members":
            return httpx.Response(200, json={"members": [{"id": "ram", "displayName": "Ramnique"}, {"id": "harsh", "displayName": "Harsh"}]})
        if path.endswith(f"/threads/{ROOT}"):
            return httpx.Response(200, json=self.thread)
        if path == "/v1/agent/invocations":
            return httpx.Response(200, json={"invocations": self.listed})
        if path.endswith("/messages") and request.method == "POST":
            return httpx.Response(200, json={"message": {"id": "R1"}, "invocations": []})
        if path == "/v1/spaces":
            return httpx.Response(200, json={"spaces": [{"id": SPACE, "name": "Payments", "kind": "shared"}]})
        return httpx.Response(200, json={})

    def paths(self, method=None):
        return [p for m, p, _ in self.calls if method is None or m == method]


def invocation(**over):
    inv = {
        "id": "INV1",
        "state": "pending",
        "conversation": {"spaceId": SPACE, "threadRootId": ROOT},
        "trigger": {"messageId": TRIGGER, "authorId": "harsh", "body": "[@Hermes](#member:AG1) plan?"},
        "where": {"spaceKind": "shared", "spaceName": "Payments"},
    }
    inv.update(over)
    return inv


@pytest.fixture
def api():
    return FakeRowboat()


@pytest.fixture
async def adapter(api, monkeypatch):
    a = mod.RowboatAdapter(PlatformConfig(enabled=True, extra={"url": "http://rowboat.test", "agent_key": "rbk_test"}))
    a._http = httpx.AsyncClient(base_url="http://rowboat.test", transport=httpx.MockTransport(api))
    a._me = AGENT
    a.handled = []

    async def handle_message(event):  # stands in for the gateway runner: takes the turn, holds the session
        a.handled.append(event)
        a._active_sessions[a._event_session_key(event)] = asyncio.Event()

    monkeypatch.setattr(a, "handle_message", handle_message)
    yield a
    await a.disconnect()


# --- construction and registration -------------------------------------------------


def test_registers_a_platform_with_the_guides_hooks():
    captured = {}

    class Ctx:
        def register_platform(self, **kw):
            captured.update(kw)

    mod.register(Ctx())
    assert captured["name"] == "rowboat" and captured["adapter_factory"] is mod.RowboatAdapter
    assert captured["required_env"] == ["ROWBOAT_URL", "ROWBOAT_AGENT_KEY"]
    assert captured["cron_deliver_env_var"] == "ROWBOAT_HOME_CHANNEL"
    assert captured["allowed_users_env"] == "ROWBOAT_ALLOWED_USERS"
    assert captured["allow_all_env"] == "ROWBOAT_ALLOW_ALL_USERS"
    assert captured["allow_update_command"] is False
    for hook in ("check_fn", "validate_config", "is_connected", "env_enablement_fn", "setup_fn", "standalone_sender_fn"):
        assert callable(captured[hook]), hook


def test_reads_settings_env_first_then_extra(monkeypatch):
    monkeypatch.setenv("ROWBOAT_URL", "https://acme.rowboat.test/")
    a = mod.RowboatAdapter(PlatformConfig(enabled=True, extra={"url": "http://ignored", "agent_key": "rbk_x"}))
    assert a.base_url == "https://acme.rowboat.test" and a.agent_key == "rbk_x"


def test_env_enablement_needs_both_settings_and_seeds_home(monkeypatch):
    monkeypatch.delenv("ROWBOAT_URL", raising=False)
    monkeypatch.delenv("ROWBOAT_AGENT_KEY", raising=False)
    assert mod._env_enablement() is None and mod.check_requirements() is False
    monkeypatch.setenv("ROWBOAT_URL", "http://rowboat.test")
    monkeypatch.setenv("ROWBOAT_AGENT_KEY", "rbk_x")
    monkeypatch.setenv("ROWBOAT_HOME_CHANNEL", "DM1")
    seed = mod._env_enablement()
    assert seed["url"] == "http://rowboat.test" and seed["agent_key"] == "rbk_x"
    assert seed["home_channel"]["chat_id"] == "DM1"
    assert mod.check_requirements() is True


def test_mentions_read_as_labels_and_its_own_is_dropped():
    assert mod._plain("[@Hermes](#member:AG1) /help", "AG1") == "/help"
    assert mod._plain("[@Hermes](#member:AG1) ask [@Harsh](#member:h) about [#Pay](#space:s)", "AG1") == "ask @Harsh about #Pay"
    assert mod._chat("S/R") == ("S", "R") and mod._chat("S") == ("S", None)


# --- message events ------------------------------------------------------------------


async def test_an_invocation_becomes_one_turn_in_the_threads_session(adapter, api):
    await adapter._deliver(invocation())
    assert len(adapter.handled) == 1
    event = adapter.handled[0]
    assert event.text == "plan?" and event.message_type == MessageType.TEXT and event.message_id == TRIGGER
    assert event.source.chat_id == f"{SPACE}/{ROOT}" and event.source.thread_id == ROOT
    assert event.source.chat_type == "group" and event.source.chat_name == "Payments"
    assert event.source.user_id == "harsh" and event.source.user_name == "Harsh"
    assert event.allow_gateway_control is False
    # What the thread said before the mention, since the agent last spoke (it never has here).
    assert event.channel_context == "[Earlier in this thread]\nRamnique: Deploy is at 3pm\nHarsh: Migration first"
    assert ("POST", "/v1/agent/invocations/INV1/ack") in [(m, p) for m, p, _ in api.calls]


@pytest.mark.parametrize(
    ("flag", "author", "allowed"),
    [(True, "ram", True), (True, "harsh", False), (False, "ram", False)],
)
async def test_hermes_commands_only_from_the_owner_and_only_when_turned_on(adapter, flag, author, allowed):
    adapter.owner_commands = flag
    await adapter._deliver(invocation(trigger={"messageId": TRIGGER, "authorId": author, "body": "[@Hermes](#member:AG1) /reload-mcp"}))
    event = adapter.handled[0]
    assert event.text == "/reload-mcp" and event.allow_gateway_control is allowed


def test_owner_commands_flag_is_read_from_env(monkeypatch):
    monkeypatch.setenv("ROWBOAT_OWNER_COMMANDS", "true")
    assert mod.RowboatAdapter(PlatformConfig(enabled=True, extra={"url": "u", "agent_key": "k"})).owner_commands is True
    monkeypatch.delenv("ROWBOAT_OWNER_COMMANDS")
    assert mod.RowboatAdapter(PlatformConfig(enabled=True, extra={"url": "u", "agent_key": "k"})).owner_commands is False


async def test_context_starts_after_the_agents_own_last_reply(adapter, api):
    api.thread["messages"].insert(0, {"id": "M2a", "author": {"memberId": "AG1"}, "body": "Noted."})
    await adapter._deliver(invocation())
    assert adapter.handled[0].channel_context == "[Earlier in this thread]\nHarsh: Migration first"


async def test_a_dm_is_a_dm_chat(adapter):
    await adapter._deliver(invocation(where={"spaceKind": "direct", "spaceName": "Direct"}))
    source = adapter.handled[0].source
    assert source.chat_type == "dm" and source.chat_name is None


async def test_delivers_once_and_never_into_a_busy_thread(adapter):
    await adapter._deliver(invocation())
    await adapter._deliver(invocation())  # the live frame and the list both carry it
    await adapter._deliver(invocation(id="INV2"))  # queued behind the running turn in the same thread
    assert [e.message_id for e in adapter.handled] == [TRIGGER]
    assert not adapter._dedup.contains("INV2")  # left for the next list, not lost
    await adapter._deliver(invocation(id="INV3", state="working"))
    assert len(adapter.handled) == 1


# --- the send path ---------------------------------------------------------------------


async def test_replies_post_in_the_thread_and_home_posts_at_the_top(adapter, api):
    await adapter._deliver(invocation())
    sent = await adapter.send(f"{SPACE}/{ROOT}", "Migration, then deploy.")
    assert sent.success and sent.message_id == "R1"
    method, path, body = api.calls[-1]
    assert (method, path) == ("POST", f"/v1/spaces/{SPACE}/messages")
    assert body == {"body": "Migration, then deploy.", "actingMode": "direct", "threadRoot": ROOT}
    assert adapter._turn_for_chat(f"{SPACE}/{ROOT}").replied
    await adapter.send("DM1", "Your digest.")
    assert api.calls[-1][2] == {"body": "Your digest.", "actingMode": "direct"}


async def test_edits_go_to_the_edit_route(adapter, api):
    result = await adapter.edit_message(f"{SPACE}/{ROOT}", "R1", "Better.")
    assert result.success and api.calls[-1][1] == f"/v1/spaces/{SPACE}/messages/R1/edit"


# --- the platform's own behavior -----------------------------------------------------------


@pytest.mark.parametrize(
    ("outcome", "replied", "expected"),
    [
        (ProcessingOutcome.SUCCESS, True, {"state": "done"}),
        (ProcessingOutcome.FAILURE, True, {"state": "failed", "error": "Hermes could not finish this turn"}),
        (ProcessingOutcome.CANCELLED, False, {"state": "cancelled"}),
        (None, True, {"state": "done"}),
        (None, False, {"state": "failed", "error": "Hermes did not take the message"}),
    ],
)
async def test_the_session_release_reports_the_outcome(adapter, api, outcome, replied, expected):
    await adapter._deliver(invocation())
    key = next(iter(adapter._turns))
    turn = adapter._turns[key]
    turn.outcome, turn.replied = outcome, replied
    adapter._active_sessions.pop(key)  # Hermes releases the session
    await adapter._finish(key, turn)
    update = [b for m, p, b in api.calls if p == "/v1/agent/invocations/INV1/update"]
    assert update == [expected]
    assert key not in adapter._turns and api.paths("GET")[-1] == "/v1/agent/invocations"  # the next queued one


def updates(api, inv_id="INV1"):
    return [b for m, p, b in api.calls if p == f"/v1/agent/invocations/{inv_id}/update"]


@pytest.mark.parametrize(
    ("body", "resumed", "expected"),
    [
        # Hermes resumed the interrupted turn in the thread's session and answered.
        ("[@Hermes](#member:AG1) plan?", True, {"state": "done"}),
        # Nothing resumed it: lost with the restart, and the thread is freed now, not in 30 minutes.
        ("[@Hermes](#member:AG1) plan?", False, {"state": "failed", "error": "Hermes restarted before finishing this"}),
        # The owner's /restart: the restart was the point.
        ("[@Hermes](#member:AG1) /restart", False, {"state": "done"}),
    ],
)
async def test_after_a_restart_it_settles_what_the_previous_run_acknowledged(adapter, api, monkeypatch, body, resumed, expected):
    api.listed = [invocation(state="working", trigger={"messageId": TRIGGER, "authorId": "ram", "body": body})]
    gate = asyncio.Event()  # Hermes's startup restore, during which it resumes interrupted turns
    monkeypatch.setattr(adapter, "_startup_gate", gate.wait)
    adapter._adopting = True
    await adapter._list()
    assert adapter.handled == [] and "/v1/agent/invocations/INV1/ack" not in api.paths()  # followed, not re-run
    key = next(iter(adapter._turns))
    if resumed:
        adapter._active_sessions[key] = asyncio.Event()
        await adapter.send(f"{SPACE}/{ROOT}", "Here's the plan")
    gate.set()
    await asyncio.sleep(0.3)
    adapter._active_sessions.pop(key, None)
    await asyncio.wait_for(asyncio.gather(*list(adapter._tasks)), 2)
    assert updates(api) == [expected]


async def test_only_the_first_list_adopts_and_never_a_turn_it_holds(adapter, api):
    await adapter._deliver(invocation())  # this run's own turn, now working
    api.listed = [invocation(state="working"), invocation(id="INV2", state="working", conversation={"spaceId": SPACE, "threadRootId": "M9"})]
    adapter._adopting = True
    await adapter._list()
    assert sorted(adapter._by_invocation) == ["INV1", "INV2"]  # INV2 adopted; INV1 still the one delivered
    adapter._turns.clear(), adapter._by_invocation.clear()
    await adapter._list()  # a later list is not a restart
    assert adapter._by_invocation == {}


async def test_reactions_are_mirrored_on_the_mention(adapter, api):
    await adapter._deliver(invocation())
    event = adapter.handled[0]
    await adapter.on_processing_start(event)
    await adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)
    reactions = [b for m, p, b in api.calls if p.endswith(f"/messages/{TRIGGER}/reactions")]
    assert reactions == [
        {"emoji": "👀", "action": "add", "actingMode": "direct"},
        {"emoji": "👀", "action": "remove", "actingMode": "direct"},
        {"emoji": "✅", "action": "add", "actingMode": "direct"},
    ]
    assert adapter._turns[adapter._event_session_key(event)].outcome == ProcessingOutcome.SUCCESS


async def test_typing_is_presence_and_the_status_phrase_is_the_activity_line(adapter, api):
    sent = []

    class WS:
        async def send(self, frame):
            sent.append(json.loads(frame))

    adapter._ws = WS()
    await adapter._deliver(invocation())
    chat = f"{SPACE}/{ROOT}"
    adapter.set_status_text(chat, "is reading plan.md")
    await adapter.send_typing(chat)
    await adapter.send_typing(chat)  # Hermes ticks every ~2 s: one presence, one activity
    await adapter.stop_typing(chat)
    await adapter.stop_typing(chat)
    assert [f["state"] for f in sent] == ["typing", "idle"]
    assert sent[0] == {"kind": "presence", "spaceId": SPACE, "state": "typing", "threadRootId": ROOT}
    updates = [b for m, p, b in api.calls if p == "/v1/agent/invocations/INV1/update"]
    assert updates == [{"state": "working", "activity": "is reading plan.md"}]


async def test_stop_from_spaces_is_hermes_own_stop(adapter):
    await adapter._deliver(invocation())
    await adapter._on_frame({"kind": "invocation_stop", "invocationId": "INV1"})
    stop = adapter.handled[-1]
    assert stop.text == "/stop" and stop.message_type == MessageType.COMMAND
    assert stop.source.chat_id == f"{SPACE}/{ROOT}" and stop.allow_gateway_control is True


async def test_standalone_send_posts_on_the_agents_key(monkeypatch):
    seen = []

    def handler(request):
        seen.append((request.headers["authorization"], request.url.path, json.loads(request.content)))
        return httpx.Response(200, json={"message": {"id": "R9"}})

    real = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(handler), **kw))
    config = PlatformConfig(enabled=True, extra={"url": "http://rowboat.test", "agent_key": "rbk_test"})
    result = await mod._standalone_send(config, "DM1", "Nightly summary")
    assert result == {"success": True, "message_id": "R9"}
    assert seen == [("Bearer rbk_test", "/v1/spaces/DM1/messages", {"body": "Nightly summary", "actingMode": "direct"})]


def test_the_setup_wizard_leaves_a_complete_setup(monkeypatch):
    """`hermes gateway setup`: what it asks, plus everything SETUP.md has an agent save."""
    import hermes_cli.config as config

    saved_env, saved_config = {}, {}
    monkeypatch.setattr(config, "save_env_value", lambda k, v: saved_env.__setitem__(k, v))
    monkeypatch.setattr(config, "set_config_value", lambda k, v: saved_config.__setitem__(k, v))
    answers = iter(["https://acme.rowboat.test", "rbk_x", "DM1", ""])
    monkeypatch.setattr("builtins.input", lambda _prompt="": next(answers))
    mod.interactive_setup()
    assert saved_env == {
        "ROWBOAT_URL": "https://acme.rowboat.test",
        "ROWBOAT_AGENT_KEY": "rbk_x",
        "ROWBOAT_HOME_CHANNEL": "DM1",
        "ROWBOAT_ALLOW_ALL_USERS": "true",
        "ROWBOAT_OWNER_COMMANDS": "true",
    }
    assert saved_config["mcp_servers.rowboat.url"] == "${ROWBOAT_URL}/mcp"
    assert saved_config["mcp_servers.rowboat.headers.Authorization"] == "Bearer ${ROWBOAT_AGENT_KEY}"
    assert saved_config["display.platforms.rowboat.show_reasoning"] == "false"


def test_setup_md_saves_what_the_wizard_saves():
    """The agent's instructions and the wizard must not drift apart."""
    import pathlib

    doc = (pathlib.Path(mod.__file__).parent / "SETUP.md").read_text()
    for key, value in mod._SETUP_CONFIG:
        assert f"hermes config set {key} '{value}'" in doc or f"hermes config set {key} {value}" in doc, key
    for key in ("ROWBOAT_ALLOW_ALL_USERS", "ROWBOAT_OWNER_COMMANDS"):
        assert f"hermes config set {key} true" in doc, key
    # What the person saves before sending the message, and the agent only checks.
    for key in ("ROWBOAT_URL", "ROWBOAT_AGENT_KEY"):
        assert f"hermes config get {key}" in doc, key
    assert "hermes plugins install rowboatlabs/hermes-rowboat --enable" in doc
