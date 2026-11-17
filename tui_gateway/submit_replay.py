"""Replay of a stored user turn through ``prompt.submit``: ``image_paths`` + ``verbatim_text``.

A caller that rewinds a user turn and sends it again needs the same model input and the same stored
row, rebuilt from the stored row alone. Two things in the ordinary submit stand in the way: images
come only from the session's staged ``attached_images`` (consumed whole by the next submit, whoever
sends it), and the text is re-prepared (input sanitizing, ``@`` reference expansion) although the
stored text is already the prepared text. ``image_paths`` names this turn's images without touching
the staged list (``[]``: the turn has none — send it for a replay of a turn without images, else the
staged images of another message are taken); ``verbatim_text`` takes the text as the turn's prepared
text. Parsing is pure; the
file check (``check_image_files``) is the only I/O and runs before the submit changes anything.

From a stored row: ``prompt.replay_source {session_id, row_id}`` returns the ``text`` and
``image_paths`` to send (``session_history._replay_source``, the inverse of the row writer). A row
written with images records their exact paths in ``display_metadata.image_paths``; an older row is
parsed and reported ``exact: false`` whenever its own text cannot be told from appended lines.
A turn without images writes no ``display_metadata.image_paths`` key (not an empty list).
``replay_supported`` / ``unsupported_reason`` there say whether a replay submit on this session would
get 4145 now, by the decision the submit makes (``methods_prompt._replay_unsupported_reason``).

What a replay does NOT promise:
- Only 4004/4144 come before anything else. Later refusals follow what every submit does first:
  the speech-interrupt latch (``interrupted``) precedes the session lookup; 4145 at the second
  decision also follows the active-session slot claim and the ``client_surface`` /
  ``voice_live_context`` overwrite; 4143 (busy) also follows the transport rebind and orphan-reap
  cancel. No turn runs, and the staged images and the queue are untouched.
- 4145 depends on per-session state (compute isolation), so it is decided per call:
  ``replay_supported`` is a forecast, and the state can still change before the submit (rare).
- The in-flight bubble and the crash marker carry the verbatim (expanded) text, ``--- Attached
  Context ---`` block included.
- Image routing (text references vs native parts) is decided again for the current model, so after a
  model change the stored row may switch between the two shapes."""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from typing import Any, Callable, Collection, Mapping, Optional

# Advertised by ``gateway.capabilities`` (``prompt_submit_replay``): the two ``prompt.submit`` params and
# the read-only RPC that derives them from a stored row. A module constant, not config: an older core
# ignores unknown params (re-expands references, takes the staged images), so a caller must not send
# them unless this build lists them.
REPLAY_PARAMS = ("image_paths", "verbatim_text")
REPLAY_SOURCE_METHOD = "prompt.replay_source"
# Later additions, named so a caller can tell this build has them: ``image_paths: []`` (a turn with no
# images that leaves the staged list alone) and ``prompt.replay_source``'s ``replay_supported``.
IMAGE_PATHS_EMPTY = "image_paths_empty"
REPLAY_SUPPORTED = "replay_supported"
PROMPT_SUBMIT_REPLAY = (*REPLAY_PARAMS, REPLAY_SOURCE_METHOD, IMAGE_PATHS_EMPTY, REPLAY_SUPPORTED)

INVALID_PARAMS_CODE = 4004
INVALID_PARAMS_REASON = "invalid_replay_params"
TOO_MANY_IMAGES_REASON = "too_many_images"
ATTACHMENT_CODE = 4144
ATTACHMENT_MISSING_REASON = "attachment_missing"
ATTACHMENT_NOT_IMAGE_REASON = "attachment_not_image"
UNSUPPORTED_CODE = 4145
UNSUPPORTED_REASON = "prompt_submit_replay_unsupported"
SOURCE_NOT_FOUND_CODE = 4146
SOURCE_NOT_FOUND_REASON = "replay_source_not_found"
# ``image.attach`` / ``pdf.attach`` put no limit on how many images one message stages (a PDF call
# stages up to 25 pages, calls repeat), so this only bounds the request: far above any real row.
MAX_IMAGE_PATHS = 1000
_MAX_PATH_CHARS = 4096


class ReplayParamError(ValueError):
    """Malformed replay params; nothing was read or changed."""

    def __init__(self, message: str, reason: str = INVALID_PARAMS_REASON):
        super().__init__(message)
        self.reason = reason


class AttachmentRefused(Exception):
    """An ``image_paths`` entry ``image.attach`` would refuse (``reason``); nothing was changed."""

    def __init__(self, path: str, reason: str):
        super().__init__(f"{reason.replace('_', ' ')}: {path}")
        self.path, self.reason = path, reason


@dataclass(frozen=True)
class SubmitReplay:
    """``image_paths``: this turn's images in order (a path may repeat, as with ``image.attach``);
    ``()`` = this turn has none; None = the staged ``attached_images`` (ordinary submit). Any list,
    the empty one included, leaves the staged list unread and uncleared. ``verbatim_text``: the text is the
    turn's prepared text (no sanitizing, stop phrase, skill or ``@`` expansion)."""

    image_paths: Optional[tuple[str, ...]]
    verbatim_text: bool

    def fingerprint_payload(self, text: Any) -> dict:
        """What a ``client_msg_id`` record fingerprints: the same id with other images or another text
        mode is a different message. Plain submits keep fingerprinting the bare text."""
        return {"prompt_submit_replay": {
            "text": text, "image_paths": None if self.image_paths is None else list(self.image_paths),
            "verbatim_text": self.verbatim_text}}

    def turn_kwargs(self) -> dict:
        """Keyword args for the turn functions; only what differs from an ordinary submit."""
        return {
            **({} if self.image_paths is None else {"image_paths": list(self.image_paths)}),
            **({"verbatim_text": True} if self.verbatim_text else {})}


def _image_path(value: Any) -> str:
    if not isinstance(value, str) or not value or len(value) > _MAX_PATH_CHARS or "\x00" in value:
        raise ReplayParamError("image_paths entries must be non-empty path strings")
    if not os.path.isabs(value):
        raise ReplayParamError(f"image_paths entries must be absolute paths: {value}")
    return value


def parse_submit_replay(params: Mapping[str, Any]) -> Optional[SubmitReplay]:
    """``SubmitReplay``, or None for an ordinary submit: neither key, or only ``verbatim_text: false``
    (then nothing else is checked either). A present key with any invalid value, ``null`` included, is
    an error; a replay's ``text`` must be a string (the stored row's text part)."""
    if not any(name in params for name in REPLAY_PARAMS):
        return None
    verbatim_text = params.get("verbatim_text", False)
    if type(verbatim_text) is not bool:
        raise ReplayParamError("verbatim_text must be a boolean")
    if "image_paths" not in params and not verbatim_text:
        return None
    image_paths = None
    if "image_paths" in params:
        raw = params["image_paths"]
        if not isinstance(raw, list):
            raise ReplayParamError("image_paths must be a list of paths")
        if len(raw) > MAX_IMAGE_PATHS:
            raise ReplayParamError(f"image_paths holds more than {MAX_IMAGE_PATHS} paths", TOO_MANY_IMAGES_REASON)
        image_paths = tuple(_image_path(value) for value in raw)
    if not isinstance(params.get("text", ""), str):
        raise ReplayParamError("text must be a string with image_paths or verbatim_text")
    return SubmitReplay(image_paths, verbatim_text)


def image_file_refusal(path: str, image_extensions: Collection[str]) -> Optional[str]:
    """Why ``image.attach`` would refuse ``path``, else None: the symlink-resolved target must be a
    readable regular file with an image extension (``image.attach`` checks the resolved path's suffix).
    The turn reads ``path`` itself, so a stored row keeps the path it was written with."""
    real = os.path.realpath(path)
    try:
        mode = os.stat(real).st_mode
    except OSError:
        return ATTACHMENT_MISSING_REASON
    if not stat.S_ISREG(mode) or not os.access(real, os.R_OK):
        return ATTACHMENT_MISSING_REASON
    if os.path.splitext(real)[1].lower() not in image_extensions:
        return ATTACHMENT_NOT_IMAGE_REASON
    return None


def missing_images(image_paths: list[str], image_extensions: Callable[[], Collection[str]]) -> list[str]:
    """The paths a replay would refuse (4144), in order, each once."""
    if not image_paths:
        return []
    extensions = image_extensions()
    return list(dict.fromkeys(p for p in image_paths if image_file_refusal(p, extensions) is not None))


def check_image_files(replay: SubmitReplay, image_extensions: Callable[[], Collection[str]]) -> None:
    """Raise ``AttachmentRefused`` for the first image ``image.attach`` would refuse, so no file is
    reachable here that is not reachable there. ``image_extensions`` is resolved only when there are
    images (it imports the CLI module)."""
    if not replay.image_paths:
        return
    extensions = image_extensions()
    for path in replay.image_paths:
        if (reason := image_file_refusal(path, extensions)) is not None:
            raise AttachmentRefused(path, reason)
