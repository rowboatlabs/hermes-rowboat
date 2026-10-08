"""Tests for the Rowboat platform adapter (the guide's list: construction, message events, the send
path against a mocked API, and the platform's own behavior)."""

import asyncio
import json
import re
from types import SimpleNamespace

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
        self.decisions = []  # decided approvals the listing returns
        self.blobs = {}  # path -> (bytes, content type)
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
        if path in self.blobs:
            data, mime = self.blobs[path]
            return httpx.Response(200, content=data, headers={"content-type": mime})
        if path.endswith(f"/threads/{ROOT}"):
            return httpx.Response(200, json=self.thread)
        if path == "/v1/agent/invocations":
            return httpx.Response(200, json={"invocations": self.listed, "approvals": self.decisions})
        if path.endswith("/approvals") and request.method == "POST":
            return httpx.Response(200, json={"approval": {"id": "AP1", "state": "open"}, "message": {"id": "CARD1"}})
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


def test_mentions_stay_tokens_and_its_own_is_dropped_only_before_a_command():
    assert mod._readable("[@Hermes](#member:AG1) /help", "AG1") == "/help"
    assert mod._readable("[@Hermes](#member:AG1), /help", "AG1") == "/help"
    assert (
        mod._readable("[@Hermes](#member:AG1) ask [@Harsh](#member:h) about [#Pay](#space:s)", "AG1")
        == "[@Hermes](#member:AG1) ask [@Harsh](#member:h) about [#Pay](#space:s)"
    )
    # Mid-sentence it stays: dropped, it would leave a blank the agent tries to explain.
    assert (
        mod._readable("[@Ceecee](#member:c) and [@Hermes](#member:AG1) - introduce yourselves", "AG1")
        == "[@Ceecee](#member:c) and [@Hermes](#member:AG1) - introduce yourselves"
    )
    assert mod._chat("S/R") == ("S", "R") and mod._chat("S") == ("S", None)


# --- message events ------------------------------------------------------------------


async def test_an_invocation_becomes_one_turn_in_the_threads_session(adapter, api):
    await adapter._deliver(invocation())
    assert len(adapter.handled) == 1
    event = adapter.handled[0]
    assert event.text == "[@Hermes](#member:AG1) plan?" and event.message_type == MessageType.TEXT and event.message_id == TRIGGER
    assert event.source.chat_id == f"{SPACE}/{ROOT}" and event.source.thread_id == ROOT
    assert event.source.chat_type == "group" and event.source.chat_name == "Payments"
    assert event.source.user_id == "harsh" and event.source.user_name == "Harsh"
    assert event.allow_gateway_control is False
    # What the thread said before the mention, since the agent last spoke (it never has here), and who
    # asked, everyone as a token the agent can copy to mention them.
    assert event.channel_context == (
        "[Earlier in this thread]\n[@Ramnique](#member:ram): Deploy is at 3pm\n[@Harsh](#member:harsh): Migration first"
        "\n\n[You are [@Hermes](#member:AG1); the new message is from [@Harsh](#member:harsh)]"
    )
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
    assert adapter.handled[0].channel_context.startswith("[Earlier in this thread]\n[@Harsh](#member:harsh): Migration first\n\n")


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


async def test_the_startup_gate_finds_the_runner_behind_hermess_wrapped_handler(adapter):
    class Runner:
        _startup_restore_in_progress = True

        async def _handle_adapter_fatal_error(self, adapter):
            pass

    runner = Runner()

    async def wrapped(*args):  # what Hermes installs as the message handler: no __self__
        pass

    adapter._message_handler, adapter._fatal_error_handler = wrapped, runner._handle_adapter_fatal_error
    assert adapter._runner() is runner
    gate = asyncio.create_task(adapter._startup_gate())
    await asyncio.sleep(0.6)
    assert not gate.done()  # held while Hermes restores
    runner._startup_restore_in_progress = False
    await asyncio.wait_for(gate, 2)


@pytest.mark.parametrize(("resume_pending", "expected"), [(True, []), (False, [{"state": "done"}])])
async def test_a_turn_hermes_will_resume_after_shutting_down_stays_working(adapter, api, resume_pending, expected):
    class Entry:
        pass

    class Store:
        def lookup_by_session_key(self, key):
            entry = Entry()
            entry.resume_pending = resume_pending
            return entry

    class Runner:
        _startup_restore_in_progress = False
        _draining = True  # stopping, or restarting

        async def _handle_adapter_fatal_error(self, adapter):
            pass

    adapter._fatal_error_handler, adapter._session_store = Runner()._handle_adapter_fatal_error, Store()
    await adapter._deliver(invocation())
    key = next(iter(adapter._turns))
    turn = adapter._turns[key]
    turn.replied = True  # "Hermes is shutting down" is a post, not an answer
    adapter._active_sessions.pop(key)
    await adapter._finish(key, turn)
    assert updates(api) == expected  # marked: left for the next start; completed in the drain: reported


@pytest.mark.parametrize(
    ("member", "refused"),
    [
        ({"agentKind": "hermes", "agentConnection": "plugin"}, False),
        # Every agent made before Rowboat stored kinds became custom/contract: still served.
        ({"agentKind": "custom", "agentConnection": "contract"}, False),
        ({}, False),  # an older Rowboat, which says neither
        # An agent Rowboat runs itself: a plugin on its key would take its mentions.
        ({"agentKind": "claude-code", "agentConnection": "replicas"}, True),
    ],
)
def test_refuses_a_key_of_an_agent_rowboat_reaches_another_way(member, refused):
    reason = mod._agent_mismatch({"kind": "agent", **member})
    assert (reason is not None) == refused
    if refused:
        assert "Add a Hermes agent" in reason


ULID_SPACE = "01M3RZS5TR83AZ2N89ABPNNMAK"
PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16


async def test_a_dm_message_without_a_mention_reaches_hermes_as_written(adapter):
    # Rowboat invokes an agent on every message in a DM with it (spec §8, 2026-09-30).
    await adapter._deliver(invocation(where={"spaceKind": "direct", "spaceName": "Direct message"}, trigger={"messageId": TRIGGER, "authorId": "ram", "body": "what is on my plate today?"}))
    event = adapter.handled[0]
    assert event.text == "what is on my plate today?"
    assert event.source.chat_type == "dm"


async def test_the_invoking_messages_files_arrive_as_hermes_media(adapter, api, monkeypatch):
    img, doc = "a" * 64, "b" * 64
    api.blobs[f"/v1/spaces/{ULID_SPACE}/blobs/{img}"] = (PNG, "image/png")
    api.blobs[f"/v1/spaces/{ULID_SPACE}/blobs/{doc}"] = (b"line 1\n", "text/plain")
    cached = []

    async def image(data, ext=".jpg"):
        cached.append(("image", data, ext))
        return f"/cache/img{ext}"

    async def document(data, filename):
        cached.append(("document", data, filename))
        return f"/cache/doc_{filename}"

    monkeypatch.setattr(mod, "cache_image_from_bytes_async", image)
    monkeypatch.setattr(mod, "cache_document_from_bytes_async", document)
    body = (
        f"[@Hermes](#member:AG1) why is this failing? ![shot](https://acme.test/s/{ULID_SPACE}/b/{img}) "
        f"[deploy.log](https://acme.test/s/{ULID_SPACE}/b/{doc}?name=deploy.log)"
    )
    await adapter._deliver(invocation(conversation={"spaceId": ULID_SPACE, "threadRootId": TRIGGER}, trigger={"messageId": TRIGGER, "authorId": "harsh", "body": body}))
    event = adapter.handled[0]
    assert event.media_urls == ["/cache/img.png", "/cache/doc_deploy.log"]
    assert event.media_types == ["image/png", "text/plain"]
    assert event.message_type == MessageType.PHOTO
    # Other files are paths Hermes reads when it needs them, never inlined into the prompt.
    assert event.media_text_inlined == [False, False]
    assert event.text == "[@Hermes](#member:AG1) why is this failing? [attached: shot] [attached: deploy.log]"
    assert [c[0] for c in cached] == ["image", "document"]


async def test_earlier_files_in_the_thread_are_listed_with_where_to_fetch_them(adapter, api):
    doc = "c" * 64
    api.thread["messages"][0]["body"] = f"Log attached [build.log](https://acme.test/s/{ULID_SPACE}/b/{doc}?name=build.log)"
    await adapter._deliver(invocation(conversation={"spaceId": ULID_SPACE, "threadRootId": ROOT}))
    context = adapter.handled[0].channel_context
    assert f"[@Harsh](#member:harsh): Log attached [attached: build.log http://rowboat.test/v1/spaces/{ULID_SPACE}/blobs/{doc}]" in context
    assert adapter.handled[0].media_urls == []


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


# --- approvals (Rowboat spec §8 part 4) ---------------------------------------------


def _prompt(**over):
    from types import SimpleNamespace

    fields = dict(
        chat_id=f"{SPACE}/{ROOT}", session_key="sess-1", command="rm -rf ./build", description="It deletes files",
        actions=[("Approve once", "once", "primary"), ("Approve for session", "session", ""), ("Deny", "deny", "danger")],
        text="", smart_denied=False, metadata=None,
    )
    fields.update(over)
    return SimpleNamespace(**fields)


def test_approvals_count_as_buttons_so_hermes_posts_no_typed_steps():
    assert mod.RowboatAdapter.supports_exec_approval_buttons() is True


async def test_an_exec_approval_becomes_a_rowboat_approval_on_the_turns_invocation(adapter, api):
    await adapter._deliver(invocation())
    sent = await adapter._send_exec_approval_prompt(_prompt())
    assert sent.success and sent.message_id == "CARD1"
    method, path, body = [c for c in api.calls if c[1].endswith("/approvals")][0]
    assert (method, path) == ("POST", "/v1/agent/invocations/INV1/approvals")
    assert body["title"] == "Run a command" and body["detail"] == "rm -rf ./build" and body["reason"] == "It deletes files"
    assert body["choices"] == ["allow_once", "allow_session", "deny"] and body["requestKey"]


async def test_without_a_turn_it_hands_back_to_hermes(adapter, api):
    sent = await adapter._send_exec_approval_prompt(_prompt())
    assert sent.success is False
    assert not [c for c in api.calls if c[1].endswith("/approvals")]


async def test_a_decision_reaches_hermes_and_is_confirmed(adapter, api, monkeypatch):
    resolved = []
    monkeypatch.setattr("tools.approval.resolve_gateway_approval", lambda key, choice, **kw: resolved.append((key, choice, kw)) or 1)
    await adapter._deliver(invocation())
    await adapter._send_exec_approval_prompt(_prompt())
    await adapter._on_frame({"kind": "approval_decided", "approval": {"id": "AP1", "state": "denied", "decision": "deny", "note": "use make clean"}})
    assert resolved == [("sess-1", "deny", {"reason": "use make clean"})]
    assert ("POST", "/v1/agent/approvals/AP1/applied") in [(m, p) for m, p, _ in api.calls]
    # The same decision again (the list after the frame) hands nothing to Hermes twice.
    api.decisions = [{"id": "AP1", "state": "denied", "decision": "deny"}]
    await adapter._list()
    assert len(resolved) == 1


async def test_a_decision_made_while_away_arrives_with_the_list(adapter, api, monkeypatch):
    resolved = []
    monkeypatch.setattr("tools.approval.resolve_gateway_approval", lambda key, choice, **kw: resolved.append((key, choice)) or 1)
    await adapter._deliver(invocation())
    await adapter._send_exec_approval_prompt(_prompt())
    api.decisions = [{"id": "AP1", "state": "allowed", "decision": "allow_session"}]
    await adapter._list()
    assert resolved == [("sess-1", "session")]


async def test_hermes_timing_out_closes_the_approval_as_expired_instead_of_editing(adapter, api):
    await adapter._deliver(invocation())
    await adapter._send_exec_approval_prompt(_prompt())
    edited = await adapter.edit_message(f"{SPACE}/{ROOT}", "CARD1", "Approval timed out")
    assert edited.success
    assert ("POST", "/v1/agent/approvals/AP1/close", {"state": "expired"}) in api.calls
    assert not [p for p in api.paths("POST") if p.endswith("/edit")]



# --- invocation options: Model and Effort (Rowboat spec §8) ---------------------------

ROWS = [  # what Hermes's /model picker lists (list_picker_providers), current provider first
    {"slug": "anthropic", "name": "Anthropic", "is_current": True, "models": ["claude-sonnet-5", "claude-opus-5-5"]},
    {"slug": "openrouter", "name": "OpenRouter", "is_current": False, "models": ["anthropic/claude-opus-5.5", "openai/gpt-6"]},
]
EFFORTS = ["none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"]


@pytest.fixture
def runner():
    """Hermes's own gateway runner, bare, as Hermes's tests use it: the session state is its code."""
    from gateway.run import GatewayRunner

    r = object.__new__(GatewayRunner)
    r.evicted = []
    r._evict_cached_agent = r.evicted.append
    return r


@pytest.fixture
def catalog(monkeypatch):
    from hermes_cli.model_switch import ModelSwitchResult

    state = SimpleNamespace(
        route=SimpleNamespace(current_provider="anthropic", current_model="claude-opus-5-5", current_base_url="",
                              user_provs=None, custom_provs=None, excluded_provs=[]),
        rows=[dict(r) for r in ROWS], switched=[], result=None,
    )

    def switch_model(**kw):
        state.switched.append(kw)
        return state.result or ModelSwitchResult(
            success=True, new_model=kw["raw_input"], target_provider=kw["explicit_provider"], api_key="sk-test",
            base_url="https://openrouter.ai/api/v1", api_mode="chat_completions",
        )

    monkeypatch.setattr(mod, "_route", lambda: state.route)
    monkeypatch.setattr("hermes_cli.model_switch_providers.list_picker_providers",
                        lambda **kw: [dict(r, models=list(r["models"])) for r in state.rows])
    monkeypatch.setattr("hermes_cli.model_switch.switch_model", switch_model)
    return state


@pytest.fixture
async def hermes(adapter, runner, catalog):
    adapter.gateway_runner = runner
    await adapter._declare()
    return adapter


def declared(api):
    return [b for m, p, b in api.calls if p == "/v1/agent/capabilities"]


async def thread_key(adapter, runner, **over):
    _space, _root, _chat_id, source = await adapter._where(invocation(**over))
    return runner._session_key_for_source(source)


def reasoning(runner, key):
    state = runner._peek_session_state(key)
    return state.conversation.reasoning_override if state else None


async def test_declares_stop_and_hermess_models_and_reasoning_levels(hermes, api, catalog):
    [body] = declared(api)
    assert body["stop"] is True
    model, effort = body["options"]
    assert model == {"type": "select", "key": "model", "label": "Model", "choices": [
        {"id": "anthropic:claude-opus-5-5", "label": "claude-opus-5-5 · Anthropic"},  # the configured model first
        {"id": "anthropic:claude-sonnet-5", "label": "claude-sonnet-5 · Anthropic"},
        {"id": "openrouter:anthropic/claude-opus-5.5", "label": "anthropic/claude-opus-5.5 · OpenRouter"},
        {"id": "openrouter:openai/gpt-6", "label": "openai/gpt-6 · OpenRouter"},
    ]}
    assert (effort["type"], effort["key"], effort["label"]) == ("select", "effort", "Effort")
    assert [c["id"] for c in effort["choices"]] == EFFORTS
    assert [c["label"] for c in effort["choices"]] == ["None", "Minimal", "Low", "Medium", "High", "Extra high", "Max", "Ultra"]
    # Unchanged, nothing is sent again; a change (the owner's /model --global) is declared.
    await hermes._declare()
    assert len(declared(api)) == 1
    catalog.route.current_model = "claude-sonnet-5"
    await hermes._declare()
    assert len(declared(api)) == 2 and declared(api)[-1]["options"][0]["choices"][0]["id"] == "anthropic:claude-sonnet-5"


async def test_the_declaration_keeps_to_rowboats_limits(adapter, api, catalog):
    long_name = "A provider with a very long name " * 6
    catalog.rows = [
        {"slug": "nous", "name": "Nous Portal", "is_current": False, "models": [f"vendor/model-{i}" for i in range(60)]},
        {"slug": "openrouter", "name": "OpenRouter", "is_current": False, "models": [f"vendor/other-{i}" for i in range(60)]},
        {"slug": "custom:lab", "name": long_name, "is_current": True, "models": ["m-1", "x" * 300, "claude-opus-5-5"]},
    ]
    catalog.route.current_provider = "custom:lab"
    await adapter._declare()
    [body] = declared(api)
    assert len(body["options"]) <= 8
    model = body["options"][0]
    ids = [c["id"] for c in model["choices"]]
    assert len(ids) == 100 and len(set(ids)) == 100
    assert ids[:2] == ["custom:lab:claude-opus-5-5", "custom:lab:m-1"]  # every provider shows; the too-long id doesn't
    assert sum(i.startswith("nous:") for i in ids) == sum(i.startswith("openrouter:") for i in ids) == 49
    for option in body["options"]:
        assert re.fullmatch(r"[a-z][a-z0-9_]{0,31}", option["key"]) and 1 <= len(option["label"]) <= 64
        assert 1 <= len(option["choices"]) <= 100
        assert all(1 <= len(c["id"]) <= 256 and 1 <= len(c["label"]) <= 128 for c in option["choices"])


async def test_without_a_catalog_it_offers_effort_and_keeps_one_it_read(adapter, api, catalog, monkeypatch):
    def broken(**kw):
        raise RuntimeError("models.dev unreachable")

    good = mod._model_catalog
    monkeypatch.setattr(mod, "_model_catalog", lambda: broken())
    await adapter._declare()
    assert [o["key"] for o in declared(api)[-1]["options"]] == ["effort"]
    monkeypatch.setattr(mod, "_model_catalog", good)
    await adapter._declare()
    assert [o["key"] for o in declared(api)[-1]["options"]] == ["model", "effort"]
    monkeypatch.setattr(mod, "_model_catalog", lambda: broken())
    await adapter._declare()  # a failed read later keeps the Model option Rowboat has
    assert len(declared(api)) == 2


async def test_a_turn_runs_with_its_invocations_model_and_effort(hermes, api, runner, catalog):
    await hermes._deliver(invocation(options={"model": "openrouter:anthropic/claude-opus-5.5", "effort": "xhigh"}))
    event = hermes.handled[0]
    key = runner._session_key_for_source(event.source)
    # Set before the message went in, where Hermes's /model --once sets its own.
    override = runner._session_model_override(key)
    assert (override["model"], override["provider"], override["api_key"]) == ("anthropic/claude-opus-5.5", "openrouter", "sk-test")
    assert runner._peek_session_state(key).conversation.one_turn_restore == {"had_override": False, "override": None}
    assert runner._resolve_session_reasoning_config(session_key=key, model="anthropic/claude-opus-5.5") == {"enabled": True, "effort": "xhigh"}
    assert runner.evicted == [key]
    assert catalog.switched[0]["explicit_provider"] == "openrouter" and catalog.switched[0]["raw_input"] == "anthropic/claude-opus-5.5"
    # Hermes's turn finalizer puts the model back; the end of the turn puts the effort back.
    runner._restore_pending_one_turn_model_override(key, runner._begin_session_run_generation(key))
    await hermes.on_processing_complete(event, ProcessingOutcome.SUCCESS)
    assert runner._session_model_override(key) is None and reasoning(runner, key) is None
    # Nothing about it was said in the thread.
    assert not [p for p in api.paths("POST") if p.endswith("/messages")]


async def test_a_turn_without_options_runs_on_hermess_configuration_even_after_a_pick(hermes, runner):
    key = await thread_key(hermes, runner)
    own = {"enabled": True, "effort": "low"}  # the owner's /reasoning low, earlier in this thread
    runner._set_session_reasoning_override(key, own)
    await hermes._deliver(invocation(options={"model": "openrouter:openai/gpt-6", "effort": "max"}))
    assert runner._session_model_override(key)["model"] == "openai/gpt-6" and reasoning(runner, key)["effort"] == "max"
    await hermes.on_processing_complete(hermes.handled[0], ProcessingOutcome.SUCCESS)
    hermes._active_sessions.pop(hermes._event_session_key(hermes.handled[0]))  # Hermes releases the session
    await asyncio.wait_for(asyncio.gather(*list(hermes._tasks)), 2)
    await hermes._deliver(invocation(id="INV2"))
    assert len(hermes.handled) == 2
    assert runner._session_model_override(key) is None and reasoning(runner, key) == own


async def test_options_hermes_cannot_use_are_skipped_and_the_turn_runs(hermes, runner, catalog, caplog):
    from hermes_cli.model_switch import ModelSwitchResult

    await hermes._deliver(invocation(options={"model": "anthropic:claude-gone-4", "effort": "turbo"}))
    key = runner._session_key_for_source(hermes.handled[0].source)
    assert runner._session_model_override(key) is None and reasoning(runner, key) is None and runner.evicted == []
    assert "claude-gone-4" in caplog.text and "turbo" in caplog.text
    # Offered, but Hermes can no longer switch to it (its credentials removed, say).
    catalog.result = ModelSwitchResult(success=False, error_message="No credentials for openrouter")
    await hermes._deliver(invocation(id="INV2", options={"model": "openrouter:openai/gpt-6"},
                                     conversation={"spaceId": SPACE, "threadRootId": "M9"}))
    assert len(hermes.handled) == 2
    assert runner._session_model_override(runner._session_key_for_source(hermes.handled[1].source)) is None
    assert "No credentials for openrouter" in caplog.text


async def test_the_configured_model_as_the_owners_default_changes_nothing(hermes, runner):
    await hermes._deliver(invocation(options={"model": "anthropic:claude-opus-5-5", "effort": "high"}))
    key = runner._session_key_for_source(hermes.handled[0].source)
    assert runner._session_model_override(key) is None and runner.evicted == []  # the cached agent stays
    assert reasoning(runner, key) == {"enabled": True, "effort": "high"}


async def test_a_hermes_command_takes_no_options(hermes, runner):
    hermes.owner_commands = True
    body = "[@Hermes](#member:AG1) /model gpt-6 --once"
    await hermes._deliver(invocation(trigger={"messageId": TRIGGER, "authorId": "ram", "body": body}, options={"model": "openrouter:openai/gpt-6", "effort": "low"}))
    event = hermes.handled[0]
    key = runner._session_key_for_source(event.source)
    assert event.get_command() == "model"
    assert runner._session_model_override(key) is None and reasoning(runner, key) is None


async def test_a_turn_hermes_never_ran_is_put_back_when_its_session_is_released(hermes, api, runner):
    await hermes._deliver(invocation(options={"model": "openrouter:openai/gpt-6", "effort": "max"}))
    key = runner._session_key_for_source(hermes.handled[0].source)
    adapter_key = next(iter(hermes._turns))
    hermes._active_sessions.pop(adapter_key)  # turned away before it ran: no finalizer, no completion hook
    await asyncio.wait_for(asyncio.gather(*list(hermes._tasks)), 2)
    assert runner._session_model_override(key) is None and reasoning(runner, key) is None
    assert updates(api) == [{"state": "failed", "error": "Hermes did not take the message"}]
