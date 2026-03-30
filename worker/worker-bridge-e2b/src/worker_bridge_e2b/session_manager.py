"""Terminal session manager for e2b sandboxes.

Manages persistent e2b Sandbox instances for the terminalExec capability,
with lease-based TTL and a janitor thread for cleanup.
"""

import threading
import time
import uuid

import structlog
from e2b import Sandbox, SandboxException

logger = structlog.get_logger()

JANITOR_INTERVAL_SEC = 5.0


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
    ):
        self._e2b_template = e2b_template
        self._e2b_timeout_sec = e2b_timeout_sec
        self._lease_min_sec = lease_min_sec
        self._lease_max_sec = lease_max_sec
        self._lease_default_sec = lease_default_sec
        self._output_limit_bytes = output_limit_bytes

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
            # New session
            session_id = str(uuid.uuid4())
            sandbox = self._create_sandbox()
            session = _TerminalSession(session_id, sandbox, lease_target)
            created = True
            with self._lock:
                self._sessions[session_id] = session
        else:
            with self._lock:
                existing = self._sessions.get(session_id)
                if existing is None:
                    if not create_if_missing:
                        raise TerminalExecError(
                            "session_not_found", "session not found"
                        )
                    # Create with caller-supplied session_id
                    sandbox = self._create_sandbox()
                    session = _TerminalSession(session_id, sandbox, lease_target)
                    created = True
                    self._sessions[session_id] = session
                else:
                    if existing.busy:
                        raise TerminalExecError("session_busy", "session is busy")
                    existing.busy = True
                    if lease_target > existing.lease_expires_at:
                        existing.lease_expires_at = lease_target
                    session = existing

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
