"""Terminal session manager for e2b sandboxes.

Manages persistent e2b Sandbox instances for the terminalExec capability,
with lease-based TTL and a janitor thread for cleanup.
"""

import base64
import json
import shlex
import threading
import time
import urllib.error
import urllib.request
import uuid

import structlog
from e2b import (
    FileNotFoundException,
    Sandbox,
    SandboxException,
    SandboxNotFoundException,
    TimeoutException,
)

logger = structlog.get_logger()

JANITOR_INTERVAL_SEC = 5.0
TERMINAL_RESOURCE_ACTION_VALIDATE = "validate"
TERMINAL_RESOURCE_ACTION_READ = "read"
TERMINAL_RESOURCE_ACTION_EXPORT = "export"
TERMINAL_RESOURCE_CODE_FILE_NOT_FOUND = "file_not_found"
TERMINAL_RESOURCE_CODE_PATH_IS_DIR = "path_is_directory"
TERMINAL_RESOURCE_CODE_FILE_TOO_LARGE = "file_too_large"
TERMINAL_RESOURCE_PROBE_SCRIPT = """
import argparse
import json
import mimetypes
import os
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--file-path", required=True)
args = parser.parse_args()

target = args.file_path
if not os.path.exists(target):
    print(json.dumps({"error": "file_not_found", "message": "file not found"}))
    sys.exit(10)
if os.path.isdir(target):
    print(json.dumps({"error": "path_is_directory", "message": "path is directory"}))
    sys.exit(11)

size_bytes = os.path.getsize(target)
mime_type, _ = mimetypes.guess_type(target)
if not mime_type:
    mime_type = "application/octet-stream"

print(json.dumps({"mime_type": mime_type, "size_bytes": size_bytes}))
"""


class TerminalExecError(Exception):
    """Domain error with an error code for the gRPC response."""

    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


class _TerminalSession:
    __slots__ = ("session_id", "sandbox", "lease_expires_at", "busy")

    def __init__(self, session_id: str, sandbox: Sandbox, lease_expires_at: float):
        self.session_id = session_id
        self.sandbox = sandbox
        self.lease_expires_at = lease_expires_at
        self.busy = True


class TerminalSessionManager:
    def __init__(
        self,
        e2b_template: str,
        e2b_timeout_sec: int,
        lease_min_sec: int,
        lease_max_sec: int,
        lease_default_sec: int,
        output_limit_bytes: int,
        export_max_bytes: int,
    ):
        self._e2b_template = e2b_template
        self._e2b_timeout_sec = e2b_timeout_sec
        self._lease_min_sec = lease_min_sec
        self._lease_max_sec = lease_max_sec
        self._lease_default_sec = lease_default_sec
        self._output_limit_bytes = output_limit_bytes
        self._export_max_bytes = export_max_bytes

        self._sessions: dict[str, _TerminalSession] = {}
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._janitor_thread = threading.Thread(
            target=self._janitor_loop, daemon=True, name="terminal-janitor"
        )
        self._janitor_thread.start()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute(
        self,
        command: str,
        session_id: str,
        create_if_missing: bool,
        lease_ttl_sec: int | None,
        deadline_unix_ms: int,
    ) -> dict:
        command = (command or "").strip()
        if not command:
            raise TerminalExecError("invalid_payload", "command is required")

        lease_duration = self._resolve_lease_duration(lease_ttl_sec)
        now = time.monotonic()
        lease_target = now + lease_duration

        session_id = (session_id or "").strip()
        created = False
        session: _TerminalSession | None = None

        if not session_id:
            # New session with auto-generated ID
            session_id = str(uuid.uuid4())
            sandbox = self._create_sandbox()
            try:
                session = _TerminalSession(session_id, sandbox, lease_target)
                created = True
                with self._lock:
                    self._sessions[session_id] = session
            except BaseException:
                _kill_sandbox_safe(sandbox, session_id)
                raise
        else:
            # Check if session exists first (without holding lock during creation)
            with self._lock:
                existing = self._sessions.get(session_id)
                if existing is not None:
                    if existing.busy:
                        raise TerminalExecError("session_busy", "session is busy")
                    existing.busy = True
                    if lease_target > existing.lease_expires_at:
                        existing.lease_expires_at = lease_target
                    session = existing

            if session is None:
                if not create_if_missing:
                    raise TerminalExecError(
                        "session_not_found", "session not found"
                    )
                # Create sandbox outside lock to avoid blocking other operations
                sandbox = self._create_sandbox()
                try:
                    session = _TerminalSession(session_id, sandbox, lease_target)
                    with self._lock:
                        # Check again in case another thread created it
                        if session_id in self._sessions:
                            _kill_sandbox_safe(sandbox, session_id)
                            raise TerminalExecError(
                                "session_conflict",
                                "session was created by another request",
                            )
                        self._sessions[session_id] = session
                        created = True
                except BaseException:
                    _kill_sandbox_safe(sandbox, session_id)
                    raise

        # Compute command timeout from deadline
        cmd_timeout: float | None = None
        if deadline_unix_ms > 0:
            remaining_ms = deadline_unix_ms - int(time.time() * 1000)
            if remaining_ms <= 0:
                self._destroy_session(session_id)
                raise TerminalExecError(
                    "deadline_exceeded", "command deadline exceeded"
                )
            cmd_timeout = remaining_ms / 1000.0

        try:
            result = session.sandbox.commands.run(command, timeout=cmd_timeout)
        except SandboxException as exc:
            if "not found" in str(exc).lower() or "404" in str(exc):
                self._destroy_session(session_id)
                raise TerminalExecError(
                    "session_not_found", "session not found"
                ) from exc
            self._mark_session_idle(session_id)
            raise TerminalExecError(
                "execution_failed", f"terminal execution failed: {exc}"
            ) from exc
        except Exception as exc:
            if "timeout" in type(exc).__name__.lower() or "timeout" in str(exc).lower():
                self._destroy_session(session_id)
                raise TerminalExecError(
                    "deadline_exceeded", "command deadline exceeded"
                ) from exc
            self._mark_session_idle(session_id)
            raise TerminalExecError(
                "execution_failed", f"terminal execution failed: {exc}"
            ) from exc

        stdout, stdout_truncated = _truncate_by_bytes(
            result.stdout or "", self._output_limit_bytes
        )
        stderr, stderr_truncated = _truncate_by_bytes(
            result.stderr or "", self._output_limit_bytes
        )

        lease_expires_at, ok = self._mark_session_idle(session_id)
        if not ok:
            raise TerminalExecError("session_not_found", "session not found")

        # Convert monotonic lease to wall-clock unix ms
        lease_wall_ms = int(
            (time.time() + (lease_expires_at - time.monotonic())) * 1000
        )

        return {
            "session_id": session_id,
            "created": created,
            "stdout": stdout,
            "stderr": stderr,
            "exit_code": result.exit_code,
            "stdout_truncated": stdout_truncated,
            "stderr_truncated": stderr_truncated,
            "lease_expires_unix_ms": lease_wall_ms,
        }

    def resolve_resource(
        self,
        session_id: str,
        file_path: str,
        action: str,
        signed_url: str,
        deadline_unix_ms: int,
    ) -> dict:
        session_id = (session_id or "").strip()
        file_path = (file_path or "").strip()
        if not session_id or not file_path:
            raise TerminalExecError("invalid_payload", "session_id and file_path are required")

        normalized_action = _normalize_terminal_resource_action(action)
        if not normalized_action:
            raise TerminalExecError("invalid_payload", "action must be validate, read, or export")
        signed_url = (signed_url or "").strip()
        if normalized_action == TERMINAL_RESOURCE_ACTION_EXPORT and not signed_url:
            raise TerminalExecError("invalid_payload", "signed_url is required for export")

        session = self._acquire_session(session_id)

        try:
            result = self._resolve_resource_in_session(
                session_id=session_id,
                sandbox=session.sandbox,
                file_path=file_path,
                action=normalized_action,
                signed_url=signed_url,
                deadline_unix_ms=deadline_unix_ms,
            )
        except TerminalExecError as exc:
            if exc.code in ("deadline_exceeded", "session_not_found"):
                self._destroy_session(session_id)
            else:
                self._mark_session_idle(session_id)
            raise
        except Exception as exc:
            if _is_timeout_error(exc):
                self._destroy_session(session_id)
                raise TerminalExecError("deadline_exceeded", "command deadline exceeded") from exc
            if _is_sandbox_missing_error(exc):
                self._destroy_session(session_id)
                raise TerminalExecError("session_not_found", "session not found") from exc
            self._mark_session_idle(session_id)
            raise TerminalExecError(
                "execution_failed", f"terminalResource execution failed: {exc}"
            ) from exc

        if not self._mark_session_idle(session_id)[1]:
            raise TerminalExecError("session_not_found", "session not found")
        return result

    def shutdown(self) -> None:
        self._stop_event.set()
        self._janitor_thread.join(timeout=10.0)

        with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()

        for session in sessions:
            _kill_sandbox_safe(session.sandbox, session.session_id)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_lease_duration(self, lease_ttl_sec: int | None) -> float:
        sec = self._lease_default_sec if lease_ttl_sec is None else lease_ttl_sec
        if sec < self._lease_min_sec or sec > self._lease_max_sec:
            raise TerminalExecError(
                "invalid_payload",
                f"lease_ttl_sec must be between {self._lease_min_sec} and {self._lease_max_sec}",
            )
        return float(sec)

    def _create_sandbox(self) -> Sandbox:
        try:
            return Sandbox.create(
                template=self._e2b_template,
                timeout=self._e2b_timeout_sec,
            )
        except Exception as exc:
            raise TerminalExecError(
                "execution_failed", f"failed to create sandbox: {exc}"
            ) from exc

    def _acquire_session(self, session_id: str) -> _TerminalSession:
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                raise TerminalExecError("session_not_found", "session not found")
            if session.busy:
                raise TerminalExecError("session_busy", "session is busy")
            session.busy = True
            return session

    def _resolve_resource_in_session(
        self,
        session_id: str,
        sandbox: Sandbox,
        file_path: str,
        action: str,
        signed_url: str,
        deadline_unix_ms: int,
    ) -> dict:
        metadata = self._probe_terminal_resource(
            sandbox=sandbox,
            file_path=file_path,
            deadline_unix_ms=deadline_unix_ms,
        )
        result = {
            "session_id": session_id,
            "file_path": file_path,
            "mime_type": metadata["mime_type"],
            "size_bytes": metadata["size_bytes"],
        }

        if action == TERMINAL_RESOURCE_ACTION_VALIDATE:
            return result

        if action == TERMINAL_RESOURCE_ACTION_READ:
            if self._output_limit_bytes > 0 and metadata["size_bytes"] > self._output_limit_bytes:
                raise TerminalExecError(
                    TERMINAL_RESOURCE_CODE_FILE_TOO_LARGE,
                    "file exceeds read limit",
                )
            content = self._read_terminal_resource_bytes(
                sandbox=sandbox,
                file_path=file_path,
                deadline_unix_ms=deadline_unix_ms,
            )
            result["blob"] = base64.b64encode(bytes(content)).decode("ascii")
            return result

        if self._export_max_bytes > 0 and metadata["size_bytes"] > self._export_max_bytes:
            raise TerminalExecError(
                TERMINAL_RESOURCE_CODE_FILE_TOO_LARGE,
                "file exceeds export limit",
            )
        content = self._read_terminal_resource_stream(
            sandbox=sandbox,
            file_path=file_path,
            deadline_unix_ms=deadline_unix_ms,
        )
        _upload_to_signed_url(
            signed_url=signed_url,
            content=_DeadlineBoundChunks(content, deadline_unix_ms),
            content_length=metadata["size_bytes"],
            deadline_unix_ms=deadline_unix_ms,
        )
        return result

    def _probe_terminal_resource(
        self,
        sandbox: Sandbox,
        file_path: str,
        deadline_unix_ms: int,
    ) -> dict:
        command = (
            "python3 -c "
            + shlex.quote(TERMINAL_RESOURCE_PROBE_SCRIPT)
            + " --file-path "
            + shlex.quote(file_path)
        )
        try:
            result = sandbox.commands.run(command, timeout=_resolve_request_timeout(deadline_unix_ms))
        except SandboxNotFoundException as exc:
            raise TerminalExecError("session_not_found", "session not found") from exc
        except TimeoutException as exc:
            raise TerminalExecError("deadline_exceeded", "command deadline exceeded") from exc
        except SandboxException as exc:
            raise TerminalExecError(
                "execution_failed", f"terminal resource probe failed: {exc}"
            ) from exc

        if result.exit_code != 0:
            decoded = _try_decode_json_object(result.stdout or "")
            if decoded and decoded.get("error"):
                raise TerminalExecError(
                    str(decoded["error"]),
                    _terminal_resource_error_message(
                        str(decoded["error"]),
                        str(decoded.get("message") or ""),
                    ),
                )
            raise TerminalExecError(
                "execution_failed",
                _terminal_resource_probe_failure_message(
                    result.exit_code,
                    result.stdout or "",
                    result.stderr or "",
                ),
            )

        decoded = _try_decode_json_object(result.stdout or "")
        if not decoded:
            raise TerminalExecError("execution_failed", "invalid terminalResource result: empty output")
        if decoded.get("error"):
            raise TerminalExecError(
                str(decoded["error"]),
                _terminal_resource_error_message(
                    str(decoded["error"]),
                    str(decoded.get("message") or ""),
                ),
            )

        mime_type = str(decoded.get("mime_type") or "").strip() or "application/octet-stream"
        size_bytes = int(decoded.get("size_bytes") or 0)
        return {
            "mime_type": mime_type,
            "size_bytes": size_bytes,
        }

    def _read_terminal_resource_bytes(
        self,
        sandbox: Sandbox,
        file_path: str,
        deadline_unix_ms: int,
    ) -> bytes:
        try:
            return bytes(
                sandbox.files.read(
                    file_path,
                    format="bytes",
                    request_timeout=_resolve_request_timeout(deadline_unix_ms),
                )
            )
        except Exception as exc:
            raise _translate_terminal_resource_access_error(exc, "read") from exc

    def _read_terminal_resource_stream(
        self,
        sandbox: Sandbox,
        file_path: str,
        deadline_unix_ms: int,
    ):
        try:
            return sandbox.files.read(
                file_path,
                format="stream",
                request_timeout=_resolve_request_timeout(deadline_unix_ms),
            )
        except Exception as exc:
            raise _translate_terminal_resource_access_error(exc, "export stream") from exc

    def _mark_session_idle(self, session_id: str) -> tuple[float, bool]:
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                return 0.0, False
            session.busy = False
            return session.lease_expires_at, True

    def _destroy_session(self, session_id: str) -> None:
        with self._lock:
            session = self._sessions.pop(session_id, None)
        if session is not None:
            _kill_sandbox_safe(session.sandbox, session_id)

    def _janitor_loop(self) -> None:
        while not self._stop_event.wait(JANITOR_INTERVAL_SEC):
            self._cleanup_expired()

    def _cleanup_expired(self) -> None:
        now = time.monotonic()
        expired: list[_TerminalSession] = []

        with self._lock:
            for sid in list(self._sessions):
                session = self._sessions[sid]
                if session.busy:
                    continue
                if session.lease_expires_at <= now:
                    expired.append(session)
                    del self._sessions[sid]

        for session in expired:
            logger.info(
                "janitor: cleaning expired session", session_id=session.session_id
            )
            _kill_sandbox_safe(session.sandbox, session.session_id)


def _kill_sandbox_safe(sandbox: Sandbox, session_id: str) -> None:
    try:
        sandbox.kill()
    except Exception as exc:
        logger.warning(
            "failed to kill sandbox", session_id=session_id, error=str(exc)
        )


def _truncate_by_bytes(value: str, max_bytes: int) -> tuple[str, bool]:
    if max_bytes <= 0:
        return value, False
    encoded = value.encode("utf-8", errors="replace")
    if len(encoded) <= max_bytes:
        return value, False
    truncated = encoded[:max_bytes].decode("utf-8", errors="ignore")
    return truncated, True


def _normalize_terminal_resource_action(action: str) -> str:
    normalized = (action or "").strip().lower()
    if not normalized:
        return TERMINAL_RESOURCE_ACTION_VALIDATE
    if normalized in {
        TERMINAL_RESOURCE_ACTION_VALIDATE,
        TERMINAL_RESOURCE_ACTION_READ,
        TERMINAL_RESOURCE_ACTION_EXPORT,
    }:
        return normalized
    return ""


def _resolve_request_timeout(deadline_unix_ms: int) -> float | None:
    if deadline_unix_ms <= 0:
        return None
    remaining_ms = deadline_unix_ms - int(time.time() * 1000)
    if remaining_ms <= 0:
        raise TerminalExecError("deadline_exceeded", "command deadline exceeded")
    return remaining_ms / 1000.0


def _try_decode_json_object(value: str) -> dict | None:
    trimmed = value.strip()
    if not trimmed:
        return None
    try:
        decoded = json.loads(trimmed)
    except json.JSONDecodeError:
        return None
    return decoded if isinstance(decoded, dict) else None


def _terminal_resource_error_message(code: str, fallback: str) -> str:
    if fallback.strip():
        return fallback.strip()
    if code == TERMINAL_RESOURCE_CODE_FILE_NOT_FOUND:
        return "file not found"
    if code == TERMINAL_RESOURCE_CODE_PATH_IS_DIR:
        return "path is directory"
    if code == TERMINAL_RESOURCE_CODE_FILE_TOO_LARGE:
        return "file exceeds read limit"
    return "terminal resource operation failed"


def _terminal_resource_probe_failure_message(exit_code: int, stdout: str, stderr: str) -> str:
    stderr_value = stderr.strip()
    stdout_value = stdout.strip()
    if stderr_value:
        return f"terminal resource probe failed: exit_code={exit_code}, stderr={stderr_value}"
    if len(stdout_value) > 256:
        stdout_value = stdout_value[:256] + "..."
    if stdout_value:
        return f"terminal resource probe failed: exit_code={exit_code}, stdout={stdout_value}"
    return f"terminal resource probe failed: exit_code={exit_code}"


def _is_timeout_error(exc: Exception) -> bool:
    for candidate in _iter_exception_chain(exc):
        if isinstance(candidate, (TimeoutException, TimeoutError)):
            return True
        text = f"{type(candidate).__name__} {candidate}".lower()
        if "timeout" in text or "deadline exceeded" in text:
            return True
    return False


def _is_sandbox_missing_error(exc: Exception) -> bool:
    for candidate in _iter_exception_chain(exc):
        if isinstance(candidate, SandboxNotFoundException):
            return True
        if isinstance(candidate, SandboxException):
            text = f"{type(candidate).__name__} {candidate}".lower()
            if "sandbox" in text and "not found" in text:
                return True
    return False


def _is_file_not_found_error(exc: Exception) -> bool:
    return any(isinstance(candidate, (FileNotFoundException, FileNotFoundError)) for candidate in _iter_exception_chain(exc))


def _is_path_is_directory_error(exc: Exception) -> bool:
    return any(isinstance(candidate, IsADirectoryError) for candidate in _iter_exception_chain(exc))


def _translate_terminal_resource_access_error(exc: Exception, operation: str) -> TerminalExecError:
    if _is_timeout_error(exc):
        return TerminalExecError("deadline_exceeded", "command deadline exceeded")
    if _is_sandbox_missing_error(exc):
        return TerminalExecError("session_not_found", "session not found")
    if _is_file_not_found_error(exc):
        return TerminalExecError(TERMINAL_RESOURCE_CODE_FILE_NOT_FOUND, "file not found")
    if _is_path_is_directory_error(exc):
        return TerminalExecError(TERMINAL_RESOURCE_CODE_PATH_IS_DIR, "path is directory")
    return TerminalExecError("execution_failed", f"terminal resource {operation} failed: {exc}")


def _iter_exception_chain(exc: Exception):
    seen: set[int] = set()
    current: Exception | None = exc
    while current is not None and id(current) not in seen:
        yield current
        seen.add(id(current))
        next_exc = current.__cause__
        if next_exc is None:
            next_exc = current.__context__
        current = next_exc


def _upload_to_signed_url(
    signed_url: str,
    content,
    content_length: int,
    deadline_unix_ms: int,
) -> None:
    data = _StreamingBody(content)
    request = urllib.request.Request(signed_url, data=data, method="PUT")
    request.add_header("Content-Length", str(content_length))
    try:
        with urllib.request.urlopen(request, timeout=_resolve_request_timeout(deadline_unix_ms)) as response:
            status = getattr(response, "status", response.getcode())
            if status < 200 or status >= 300:
                body = response.read(1024).decode("utf-8", errors="replace").strip()
                raise RuntimeError(body or f"HTTP {status}")
    except TerminalExecError:
        raise
    except urllib.error.HTTPError as exc:
        body = exc.read(1024).decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"upload export file failed: {body or exc.reason}") from exc
    except urllib.error.URLError as exc:
        if _is_timeout_error(exc):
            raise TerminalExecError("deadline_exceeded", "command deadline exceeded") from exc
        raise RuntimeError(f"upload export file: {exc.reason}") from exc
    except TimeoutError as exc:
        raise TerminalExecError("deadline_exceeded", "command deadline exceeded") from exc


class _StreamingBody:
    def __init__(self, chunks) -> None:
        self._chunks = iter(chunks)

    def read(self, size: int = -1) -> bytes:
        if size == 0:
            return b""
        try:
            chunk = next(self._chunks)
        except StopIteration:
            return b""
        return bytes(chunk)


class _DeadlineBoundChunks:
    def __init__(self, chunks, deadline_unix_ms: int) -> None:
        self._chunks = iter(chunks)
        self._deadline_unix_ms = deadline_unix_ms

    def __iter__(self):
        return self

    def __next__(self):
        _resolve_request_timeout(self._deadline_unix_ms)
        return next(self._chunks)
