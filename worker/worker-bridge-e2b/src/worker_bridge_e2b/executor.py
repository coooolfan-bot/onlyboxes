"""e2b capability executors.

Each function receives the raw payload_json bytes from CommandDispatch
and returns (result_json_bytes, error_code, error_message).
error_code/error_message are empty strings on success.
"""

import contextlib
import json
import time
from typing import TYPE_CHECKING

import structlog
from e2b import Sandbox, SandboxException

from worker_bridge_e2b.session_manager import TerminalExecError, TerminalSessionManager

if TYPE_CHECKING:
    from worker_bridge_e2b.config import Config

logger = structlog.get_logger()

type ExecutorResult = tuple[bytes, str, str]

_config: Config | None = None
_session_manager: TerminalSessionManager | None = None


def init(cfg: Config) -> None:
    global _config, _session_manager
    _config = cfg
    _session_manager = TerminalSessionManager(
        e2b_template=cfg.e2b_terminal_exec_template,
        e2b_timeout_sec=cfg.e2b_sandbox_timeout_sec,
        lease_min_sec=cfg.terminal_lease_min_sec,
        lease_max_sec=cfg.terminal_lease_max_sec,
        lease_default_sec=cfg.terminal_lease_default_sec,
        output_limit_bytes=cfg.terminal_output_limit_bytes,
        export_max_bytes=cfg.terminal_export_max_bytes,
    )
    logger.info("executor initialized")


def shutdown() -> None:
    global _session_manager
    if _session_manager is not None:
        _session_manager.shutdown()
        _session_manager = None
    logger.info("executor shut down")


def execute_echo(payload: bytes, deadline_unix_ms: int) -> ExecutorResult:
    data = json.loads(payload)
    message = data.get("message", "")
    result = json.dumps({"message": message}).encode()
    return result, "", ""


def execute_python_exec(payload: bytes, deadline_unix_ms: int) -> ExecutorResult:
    data = json.loads(payload)
    code = (data.get("code") or "").strip()
    if not code:
        return b"{}", "invalid_payload", "code is required"

    if deadline_unix_ms > 0 and time.time() * 1000 > deadline_unix_ms:
        return b"{}", "deadline_exceeded", "command deadline exceeded"

    # Compute command timeout from deadline
    if deadline_unix_ms > 0:
        cmd_timeout = max(1.0, (deadline_unix_ms - time.time() * 1000) / 1000.0)
    else:
        cmd_timeout = float(_config.e2b_sandbox_timeout_sec) if _config else 300.0

    sandbox: Sandbox | None = None
    try:
        sandbox = Sandbox.create(
            template=_config.e2b_python_exec_template if _config else "base",
            timeout=_config.e2b_sandbox_timeout_sec if _config else 300,
        )
        sandbox.files.write("/tmp/code.py", code)
        result = sandbox.commands.run("uv run /tmp/code.py", timeout=cmd_timeout)

        result_json = json.dumps(
            {
                "output": result.stdout or "",
                "stderr": result.stderr or "",
                "exit_code": result.exit_code,
            }
        ).encode()
        return result_json, "", ""

    except SandboxException as exc:
        return b"{}", "execution_failed", f"pythonExec execution failed: {exc}"
    except Exception as exc:
        if "timeout" in type(exc).__name__.lower() or "timeout" in str(exc).lower():
            return b"{}", "deadline_exceeded", "command deadline exceeded"
        return b"{}", "execution_failed", f"pythonExec execution failed: {exc}"
    finally:
        if sandbox is not None:
            with contextlib.suppress(Exception):
                sandbox.kill()


def execute_terminal_exec(payload: bytes, deadline_unix_ms: int) -> ExecutorResult:
    if _session_manager is None:
        return b"{}", "execution_failed", "terminal executor is unavailable"

    data = json.loads(payload)
    command = data.get("command", "")
    session_id = data.get("session_id", "")
    create_if_missing = data.get("create_if_missing", False)
    lease_ttl_sec = data.get("lease_ttl_sec")

    try:
        result = _session_manager.execute(
            command=command,
            session_id=session_id,
            create_if_missing=create_if_missing,
            lease_ttl_sec=lease_ttl_sec,
            deadline_unix_ms=deadline_unix_ms,
        )
        return json.dumps(result).encode(), "", ""
    except TerminalExecError as exc:
        return b"{}", exc.code, exc.message
    except Exception as exc:
        return b"{}", "execution_failed", f"terminalExec execution failed: {exc}"


def execute_terminal_resource(payload: bytes, deadline_unix_ms: int) -> ExecutorResult:
    if _session_manager is None:
        return b"{}", "execution_failed", "terminal executor is unavailable"

    if deadline_unix_ms > 0 and time.time() * 1000 > deadline_unix_ms:
        return b"{}", "deadline_exceeded", "command deadline exceeded"

    if len(payload) == 0:
        return b"{}", "invalid_payload", "terminalResource payload is required"

    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return b"{}", "invalid_payload", "payload_json is not valid terminalResource payload"
    if not isinstance(data, dict):
        return b"{}", "invalid_payload", "payload_json is not valid terminalResource payload"

    session_id = data.get("session_id", "")
    file_path = data.get("file_path", "")
    action = data.get("action", "")
    signed_url = data.get("signed_url", "")

    if not str(session_id).strip() or not str(file_path).strip():
        return b"{}", "invalid_payload", "terminalResource session_id and file_path are required"
    if str(action).strip().lower() == "export" and not str(signed_url).strip():
        return b"{}", "invalid_payload", "terminalResource signed_url is required for export"

    try:
        result = _session_manager.resolve_resource(
            session_id=str(session_id),
            file_path=str(file_path),
            action=str(action),
            signed_url=str(signed_url),
            deadline_unix_ms=deadline_unix_ms,
        )
        return json.dumps(result).encode(), "", ""
    except TerminalExecError as exc:
        return b"{}", exc.code, exc.message
    except Exception as exc:
        return b"{}", "execution_failed", f"terminalResource execution failed: {exc}"
