"""e2b capability executors.

Each function receives the raw payload_json bytes from CommandDispatch
and returns (result_json_bytes, error_code, error_message).
error_code/error_message are empty strings on success.
"""

from __future__ import annotations

import contextlib
import json
import time
from collections.abc import Mapping
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


def active_session_count() -> int:
    if _session_manager is None:
        return 0
    return _session_manager.active_session_count()


def execute_echo(payload: bytes, deadline_unix_ms: int) -> ExecutorResult:
    if _deadline_exceeded(deadline_unix_ms):
        return b"{}", "deadline_exceeded", "command deadline exceeded"

    data, err_code, err_message = _decode_object_payload(payload, "echo")
    if err_code:
        return b"{}", err_code, err_message

    message = data.get("message", "")
    if not isinstance(message, str) or not message.strip():
        return b"{}", "invalid_payload", "echo payload is required"
    result = json.dumps({"message": message}).encode()
    return result, "", ""


def execute_python_exec(payload: bytes, deadline_unix_ms: int) -> ExecutorResult:
    data, err_code, err_message = _decode_object_payload(payload, "pythonExec")
    if err_code:
        return b"{}", err_code, err_message

    code = data.get("code")
    if not isinstance(code, str) or not code.strip():
        return b"{}", "invalid_payload", "pythonExec code is required"

    if _deadline_exceeded(deadline_unix_ms):
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

    if _deadline_exceeded(deadline_unix_ms):
        return b"{}", "deadline_exceeded", "command deadline exceeded"

    data, err_code, err_message = _decode_object_payload(payload, "terminalExec")
    if err_code:
        return b"{}", err_code, err_message

    command = data.get("command", "")
    if not isinstance(command, str) or not command.strip():
        return b"{}", "invalid_payload", "terminalExec command is required"

    session_id = data.get("session_id", "")
    if session_id is None:
        session_id = ""
    if not isinstance(session_id, str):
        return b"{}", "invalid_payload", "terminalExec session_id must be a string"

    create_if_missing = data.get("create_if_missing", False)
    if not isinstance(create_if_missing, bool):
        return b"{}", "invalid_payload", "terminalExec create_if_missing must be a boolean"

    lease_ttl_sec = data.get("lease_ttl_sec")
    if lease_ttl_sec is not None and (
        not isinstance(lease_ttl_sec, int) or isinstance(lease_ttl_sec, bool)
    ):
        return b"{}", "invalid_payload", "terminalExec lease_ttl_sec must be an integer"

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

    if _deadline_exceeded(deadline_unix_ms):
        return b"{}", "deadline_exceeded", "command deadline exceeded"

    data, err_code, err_message = _decode_object_payload(payload, "terminalResource")
    if err_code:
        return b"{}", err_code, err_message

    session_id = data.get("session_id", "")
    file_path = data.get("file_path", "")
    action = data.get("action", "")
    signed_url = data.get("signed_url", "")
    headers, err_code, err_message = _decode_headers(data.get("headers"))
    if err_code:
        return b"{}", err_code, err_message

    if not isinstance(session_id, str) or not isinstance(file_path, str):
        return b"{}", "invalid_payload", "terminalResource session_id and file_path must be strings"
    if not isinstance(action, str):
        return b"{}", "invalid_payload", "terminalResource action must be a string"
    if not isinstance(signed_url, str):
        return b"{}", "invalid_payload", "terminalResource signed_url must be a string"

    session_id = session_id.strip()
    file_path = file_path.strip()
    action = action.strip()
    signed_url = signed_url.strip()

    if not session_id or not file_path:
        return b"{}", "invalid_payload", "terminalResource session_id and file_path are required"
    if action.lower() == "export" and not signed_url:
        return b"{}", "invalid_payload", "terminalResource signed_url is required for export"

    try:
        result = _session_manager.resolve_resource(
            session_id=session_id,
            file_path=file_path,
            action=action,
            signed_url=signed_url,
            headers=headers,
            deadline_unix_ms=deadline_unix_ms,
        )
        return json.dumps(result).encode(), "", ""
    except TerminalExecError as exc:
        return b"{}", exc.code, exc.message
    except Exception as exc:
        return b"{}", "execution_failed", f"terminalResource execution failed: {exc}"


def _deadline_exceeded(deadline_unix_ms: int) -> bool:
    return deadline_unix_ms > 0 and time.time() * 1000 > deadline_unix_ms


def _decode_object_payload(payload: bytes, capability: str) -> tuple[dict, str, str]:
    if len(payload) == 0:
        return {}, "invalid_payload", f"{capability} payload is required"
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return {}, "invalid_payload", f"payload_json is not valid {capability} payload"
    if not isinstance(data, dict):
        return {}, "invalid_payload", f"payload_json is not valid {capability} payload"
    return data, "", ""


def _decode_headers(raw: object) -> tuple[dict[str, str], str, str]:
    if raw is None:
        return {}, "", ""
    if not isinstance(raw, Mapping):
        return {}, "invalid_payload", "terminalResource headers must be an object"

    headers: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not isinstance(value, str):
            return {}, "invalid_payload", "terminalResource headers must contain string values"
        trimmed_key = key.strip()
        if trimmed_key:
            headers[trimmed_key] = value
    return headers, "", ""
