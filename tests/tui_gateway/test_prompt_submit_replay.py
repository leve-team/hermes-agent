"""``prompt.submit`` replay of a stored user turn: ``image_paths`` + ``verbatim_text``.

A broker that rewinds the last user turn and sends it again needs the new turn to be the SAME turn:
the same model input and the same stored row, from the stored row alone, without taking images
another message staged on the session (``image.attach`` keeps them in ``attached_images`` until the
next submit consumes the whole list). The caller checks ``gateway.capabilities`` first: an older
core ignores unknown params, which would silently re-expand references and swallow staged images.
"""

from __future__ import annotations

import threading

import pytest

import agent.image_routing
import agent.model_metadata
from hermes_state import SessionDB
from tui_gateway import server

# A real 1x1 PNG: native-vision routing reads and sniffs the bytes.
_PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d4948445200000001000000010806000000"
    "1f15c4890000000d49444154789c6360000002000154a24f5d0000000049454e44ae426082")


class _ImmediateThread:
    def __init__(self, target=None, daemon=None, **_kwargs):
        self._target = target

    def start(self):
        self._target()

    def join(self, timeout=None):
        return None


class _RecordingAgent:
    """Stores the persisted user message exactly as the agent's flush does and records the
    model-bound message, so the test reads the stored row back from the database."""

    model = "replay-test-model"

    def __init__(self, db: SessionDB, session_key: str):
        self.db, self.session_id, self.model_inputs = db, session_key, []

    def clear_interrupt(self):
        return None

    def run_conversation(self, user_message, conversation_history=None, stream_callback=None,
                         persist_user_message=None, **_kwargs):
        self.model_inputs.append(user_message)
        self.db.append_message(self.session_id, "user", persist_user_message)
        self.db.append_message(self.session_id, "assistant", "ok")
        return {"final_response": "ok", "messages": [
            *(conversation_history or []),
            {"role": "user", "content": persist_user_message}, {"role": "assistant", "content": "ok"}]}


@pytest.fixture()
def replay_env(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "notes.txt").write_text("deadline 09:00~18:00\n", encoding="utf-8")
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    for name in ("photo.png", "other.png"):
        (uploads / name).write_bytes(_PNG)
    db = SessionDB(db_path=tmp_path / "state.db")
    db.create_session("replay-session", source="tui")
    recorder = _RecordingAgent(db, "replay-session")
    session = {
        "agent": recorder, "attached_images": [], "image_counter": 0, "history": [],
        "history_lock": threading.Lock(), "history_version": 0, "running": False,
        "session_key": "replay-session", "cwd": str(workspace), "inflight_turn": None,
    }
    old_db = server._db
    server._db = db
    server._sessions["replay-sid"] = session
    # image.attach imports cli, whose logging setup starts a listener thread: import it while
    # threads are still real (server.threading IS the threading module).
    import cli  # noqa: F401
    monkeypatch.setattr(server.threading, "Thread", _ImmediateThread)
    monkeypatch.setattr(server, "_get_usage", lambda _agent: {})
    monkeypatch.setattr(server, "render_message", lambda *_args: "")
    monkeypatch.setattr(server, "_emit", lambda *_args, **_kwargs: None)
    # Model metadata would otherwise be looked up over the network.
    monkeypatch.setattr(agent.model_metadata, "get_model_context_length", lambda *_a, **_k: 200_000)
    yield {"db": db, "agent": recorder, "session": session, "uploads": uploads, "workspace": workspace}
    server._sessions.pop("replay-sid", None)
    server._db = old_db
    db.close()


def _rpc(method: str, **params) -> dict:
    return server._methods[method]("rid", {"session_id": "replay-sid", **params})


def _last_user_row(db: SessionDB):
    return [m for m in db.get_messages_as_conversation("replay-session") if m["role"] == "user"][-1]["content"]


def _replay_request(stored) -> tuple[str, list[str]]:
    """What the broker does with a stored row: take the text part, split off the trailing
    ``@image:`` lines (unquoting ``format_reference_value``) as ``image_paths``."""
    text = stored if isinstance(stored, str) else stored[0]["text"]
    lines, paths = text.split("\n"), []
    while lines and lines[-1].startswith("@image:"):
        value = lines.pop()[len("@image:"):]
        paths.insert(0, value[1:-1] if value[:1] in "`\"'" and value[-1:] == value[:1] else value)
    return "\n".join(lines), paths


@pytest.mark.parametrize("image_mode", ["text", "native"])
def test_replay_reproduces_stored_turn_and_leaves_staged_images(replay_env, monkeypatch, image_mode):
    monkeypatch.setattr(agent.image_routing, "decide_image_input_mode", lambda *_a, **_k: image_mode)
    db, recorder, session, uploads = (replay_env[k] for k in ("db", "agent", "session", "uploads"))

    assert _rpc("image.attach", path=str(uploads / "photo.png"))["result"]["attached"]
    assert _rpc("prompt.submit", text="compare @file:notes.txt")["result"]["status"] == "streaming"
    original = _last_user_row(db)
    assert session["attached_images"] == []  # an ordinary submit consumes the staged list

    # The reply edited the referenced file, and another message staged its own image meanwhile.
    (replay_env["workspace"] / "notes.txt").write_text("rewritten\n", encoding="utf-8")
    other = _rpc("image.attach", path=str(uploads / "other.png"))["result"]["path"]
    capabilities = server._methods["gateway.capabilities"]("rid", {})["result"]
    assert {"image_paths", "verbatim_text"} <= set(capabilities["prompt_submit_replay"])

    text, image_paths = _replay_request(original)
    # The stored row holds the expanded file snapshot (with a sequence the input sanitizer would
    # rewrite) and the image; the replay is built from that row alone.
    assert "--- Attached Context ---" in text and "09:00~18:00" in text
    assert image_paths == [str((uploads / "photo.png").resolve())]
    replayed = _rpc("prompt.submit", text=text, image_paths=image_paths, verbatim_text=True)

    assert replayed["result"]["status"] == "streaming"
    assert _last_user_row(db) == original
    assert recorder.model_inputs[1] == recorder.model_inputs[0]
    assert session["attached_images"] == [other]


def _state(env) -> tuple:
    session = env["session"]
    return (list(session["attached_images"]), session["running"], session.get("queued_prompt"),
            session.get("inflight_turn"), len(env["db"].get_messages_as_conversation("replay-session")),
            len(env["agent"].model_inputs))


class _MemoryAcceptStore:
    """``submit_idempotency`` claim semantics over a dict: the fingerprint decides duplicate vs
    conflict (PostgreSQL itself is not available to this test)."""

    def __init__(self):
        self.fingerprints: dict = {}

    def claim_submit(self, session_key, client_msg_id, text_fingerprint, *, ui_session_id="", admit=lambda: None):
        from tui_gateway import submit_idempotency as idem
        known = self.fingerprints.get((session_key, client_msg_id))
        record = idem.Acceptance(session_key, client_msg_id, known or text_fingerprint, idem.STATE_ACCEPTED,
                                 ui_session_id, 0, None, "", 0.0, 1, 0.0, 0.0, None)
        if known is not None:
            return idem.Claim(idem.CONFLICT if known != text_fingerprint else idem.DUPLICATE, record)
        if (refusal := admit()) is not None:
            return idem.Claim(idem.REFUSED, record, refusal)
        self.fingerprints[(session_key, client_msg_id)] = text_fingerprint
        return idem.Claim(idem.NEW, record)


def _with_accept_store(env, monkeypatch, first: dict) -> None:
    from tui_gateway import submit_idempotency as idem
    store = _MemoryAcceptStore()
    monkeypatch.setattr(idem, "active", lambda: True)
    monkeypatch.setattr(idem, "claim_submit", store.claim_submit)
    for name in ("record_result", "finish_submit", "release_submit"):
        monkeypatch.setattr(idem, name, lambda *_a, **_k: True)
    assert _rpc("prompt.submit", **first)["result"]["status"] == "streaming"


def _flip_to_compute_host(env, monkeypatch) -> None:
    """Compute isolation answers "no" at the early check and "yes" when the submit decides inline vs
    compute host; the replay must still never reach the compute host."""
    answers = iter([False])
    monkeypatch.setattr(server, "_session_uses_compute_host", lambda *_a, **_k: next(answers, True))
    monkeypatch.setattr(server, "_submit_prompt_to_compute_host",
                        lambda *_a, **_k: pytest.fail("a replay reached the compute host"))


def _symlink(env, name: str, target: str) -> str:
    (env["uploads"] / name).symlink_to(env["uploads"] / target)
    return str(env["uploads"] / name)


_INVALID = ("error", 4004, "invalid_replay_params")
_PHOTO = "{up}/photo.png"


@pytest.mark.parametrize(("setup", "params", "expected"), [
    (None, {"image_paths": ["{up}/gone.png"]}, ("error", 4144, "attachment_missing")),
    (None, {"image_paths": ["{up}/secret.txt"]}, ("error", 4144, "attachment_not_image")),
    (lambda env, mp: _symlink(env, "secret.png", "secret.txt"),
     {"image_paths": ["{up}/secret.png"]}, ("error", 4144, "attachment_not_image")),
    (lambda env, mp: env["session"].update(running=True),
     {"image_paths": [_PHOTO], "verbatim_text": True}, ("error", 4143, "session_busy")),
    (_flip_to_compute_host, {"image_paths": [_PHOTO]}, ("error", 4145, "prompt_submit_replay_unsupported")),
    (lambda env, mp: _with_accept_store(env, mp, {"text": "again", "image_paths": [_PHOTO.format(up=env["uploads"])],
                                                  "client_msg_id": "m-1"}),
     {"image_paths": ["{up}/other.png"], "client_msg_id": "m-1"}, ("error", 4141, "client_msg_id_conflict")),
    (lambda env, mp: _with_accept_store(env, mp, {"text": "again", "client_msg_id": "m-1"}),
     {"image_paths": [_PHOTO], "client_msg_id": "m-1"}, ("error", 4141, "client_msg_id_conflict")),
    (lambda env, mp: _with_accept_store(env, mp, {"text": "again", "image_paths": [_PHOTO.format(up=env["uploads"])],
                                                  "client_msg_id": "m-1"}),
     {"image_paths": [_PHOTO], "client_msg_id": "m-1"}, ("result", "duplicate")),
    (lambda env, mp: _symlink(env, "alias", "photo.png"), {"image_paths": ["{up}/alias"]}, ("result", "streaming")),
    (None, {"image_paths": [_PHOTO, _PHOTO]}, ("result", "streaming")),
    (None, {"image_paths": None}, _INVALID),
    (None, {"image_paths": []}, _INVALID),
    (None, {"image_paths": _PHOTO}, _INVALID),
    (None, {"image_paths": ["photo.png"]}, _INVALID),
    (None, {"image_paths": [_PHOTO] * 1001}, ("error", 4004, "too_many_images")),
    (None, {"verbatim_text": "yes"}, _INVALID),
    (None, {"verbatim_text": None}, _INVALID),
    (None, {"verbatim_text": True, "text": ["part"]}, _INVALID),
])
def test_replay_submit_decides_before_touching_staged_images(replay_env, monkeypatch, setup, params, expected):
    """Refusals change no turn state; every decision leaves the images staged for another message alone,
    and an image path is accepted exactly when ``image.attach`` would accept it."""
    uploads, session = replay_env["uploads"], replay_env["session"]
    (uploads / "secret.txt").write_text("not an image", encoding="utf-8")
    if setup is not None:
        setup(replay_env, monkeypatch)
    staged = str(uploads / "other.png")
    session["attached_images"] = [staged]
    resolved = {k: [p.format(up=uploads) for p in v] if k == "image_paths" and isinstance(v, list)
                else v.format(up=uploads) if k == "image_paths" and isinstance(v, str) else v
                for k, v in params.items()}
    before = _state(replay_env)

    reply = _rpc("prompt.submit", **{"text": "again", **resolved})

    assert session["attached_images"] == [staged]
    if expected[0] == "error":
        assert (reply["error"]["code"], reply["error"]["data"]["reason"]) == expected[1:]
        assert _state(replay_env) == before
        if expected[1] == 4144:  # image.attach refuses every path the replay refused
            assert "error" in _rpc("image.attach", path=reply["error"]["data"]["path"])
    else:
        assert reply["result"]["status"] == expected[1]
        if expected[1] == "streaming":  # image.attach accepts every path the replay accepted
            assert all(_rpc("image.attach", path=p).get("result") for p in resolved["image_paths"])
