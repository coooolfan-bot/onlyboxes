import base64
import json
import time
from types import SimpleNamespace

import pytest
from e2b import FileNotFoundException, SandboxNotFoundException

import worker_bridge_e2b.session_manager as session_manager_module
from worker_bridge_e2b import executor, runner
from worker_bridge_e2b.config import Config
from worker_bridge_e2b.session_manager import (
    TERMINAL_RESOURCE_ACTION_EXPORT,
    TERMINAL_RESOURCE_ACTION_READ,
    TERMINAL_RESOURCE_ACTION_VALIDATE,
    TERMINAL_RESOURCE_CODE_FILE_NOT_FOUND,
    TERMINAL_RESOURCE_CODE_FILE_TOO_LARGE,
    TERMINAL_RESOURCE_CODE_PATH_IS_DIR,
    TerminalExecError,
    TerminalSessionManager,
    _TerminalSession,
)


class FakeCommandResult:
    def __init__(self, stdout: str = "", stderr: str = "", exit_code: int = 0):
        self.stdout = stdout
        self.stderr = stderr
        self.exit_code = exit_code


class FakeCommands:
    def __init__(self, run_fn):
        self._run_fn = run_fn

    def run(self, command: str, timeout: float | None = None):
        return self._run_fn(command, timeout)


class FakeFiles:
    def __init__(self, read_fn):
        self._read_fn = read_fn

    def read(self, path: str, format: str = "text", request_timeout: float | None = None):
        return self._read_fn(path, format, request_timeout)


class FakeSandbox:
    def __init__(self, run_fn, read_fn=None):
        self.commands = FakeCommands(run_fn)
        self.files = FakeFiles(read_fn or (lambda path, format, request_timeout: b""))
        self.killed = False

    def kill(self):
        self.killed = True


@pytest.fixture
def manager():
    value = TerminalSessionManager(
        e2b_template="base",
        e2b_timeout_sec=300,
        lease_min_sec=60,
        lease_max_sec=1800,
        lease_default_sec=60,
        output_limit_bytes=1024,
        export_max_bytes=0,
    )
    yield value
    value.shutdown()


@pytest.fixture
def restore_executor_state():
    original_session_manager = executor._session_manager
    original_config = executor._config
    yield
    executor._session_manager = original_session_manager
    executor._config = original_config


def make_config() -> Config:
    return Config(
        console_grpc_target="127.0.0.1:50051",
        console_tls=False,
        worker_id="worker-1",
        worker_secret="secret",
        heartbeat_interval_sec=5,
        heartbeat_jitter_pct=20,
        node_name="node-1",
        executor_kind="e2b",
        version="dev",
        labels={},
        e2b_api_key="key",
        e2b_python_exec_template="base",
        e2b_terminal_exec_template="base",
        e2b_sandbox_timeout_sec=300,
        echo_max_inflight=4,
        python_exec_max_inflight=4,
        terminal_exec_max_inflight=4,
        terminal_resource_max_inflight=7,
        terminal_lease_min_sec=60,
        terminal_lease_max_sec=1800,
        terminal_lease_default_sec=60,
        terminal_output_limit_bytes=1024,
        terminal_export_max_bytes=0,
        log_level="info",
        log_format="json",
    )


def register_session(
    manager: TerminalSessionManager, session_id: str, sandbox: FakeSandbox, busy: bool = False
):
    session = _TerminalSession(session_id, sandbox, time.monotonic() + 60)
    session.busy = busy
    with manager._lock:
        manager._sessions[session_id] = session
    return session


def test_build_hello_declares_terminal_resource():
    hello = runner._build_hello(make_config()).hello
    capabilities = {item.name: item.max_inflight for item in hello.capabilities}
    assert capabilities["terminalResource"] == 7


def test_execute_terminal_resource_success(restore_executor_state):
    executor._session_manager = SimpleNamespace(
        resolve_resource=lambda **kwargs: {
            "session_id": kwargs["session_id"],
            "file_path": kwargs["file_path"],
            "mime_type": "text/plain",
            "size_bytes": 5,
            "blob": base64.b64encode(b"hello").decode("ascii"),
        }
    )

    payload, err_code, err_message = executor.execute_terminal_resource(
        b'{"session_id":"sess-1","file_path":"/tmp/hello.txt","action":"read"}',
        0,
    )

    assert err_code == ""
    assert err_message == ""
    assert json.loads(payload) == {
        "session_id": "sess-1",
        "file_path": "/tmp/hello.txt",
        "mime_type": "text/plain",
        "size_bytes": 5,
        "blob": base64.b64encode(b"hello").decode("ascii"),
    }


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"", "terminalResource payload is required"),
        (b"[]", "payload_json is not valid terminalResource payload"),
        (b"{", "payload_json is not valid terminalResource payload"),
        (b'{"session_id":"sess-1"}', "terminalResource session_id and file_path are required"),
        (
            b'{"session_id":"sess-1","file_path":"/tmp/hello.txt","action":"export"}',
            "terminalResource signed_url is required for export",
        ),
    ],
)
def test_execute_terminal_resource_invalid_payloads(
    restore_executor_state, payload: bytes, message: str
):
    executor._session_manager = SimpleNamespace(resolve_resource=lambda **kwargs: None)

    result_payload, err_code, err_message = executor.execute_terminal_resource(payload, 0)

    assert result_payload == b"{}"
    assert err_code == "invalid_payload"
    assert err_message == message


def test_terminal_session_manager_validate_and_read(manager: TerminalSessionManager):
    sandbox = FakeSandbox(
        run_fn=lambda command, timeout: FakeCommandResult(
            stdout='{"mime_type":"text/plain","size_bytes":5}'
        ),
        read_fn=lambda path, format, request_timeout: b"hello",
    )
    register_session(manager, "sess-1", sandbox)

    validate = manager.resolve_resource(
        session_id="sess-1",
        file_path="/tmp/hello.txt",
        action=TERMINAL_RESOURCE_ACTION_VALIDATE,
        signed_url="",
        deadline_unix_ms=0,
    )
    read = manager.resolve_resource(
        session_id="sess-1",
        file_path="/tmp/hello.txt",
        action=TERMINAL_RESOURCE_ACTION_READ,
        signed_url="",
        deadline_unix_ms=0,
    )

    assert validate == {
        "session_id": "sess-1",
        "file_path": "/tmp/hello.txt",
        "mime_type": "text/plain",
        "size_bytes": 5,
    }
    assert read == {
        "session_id": "sess-1",
        "file_path": "/tmp/hello.txt",
        "mime_type": "text/plain",
        "size_bytes": 5,
        "blob": base64.b64encode(b"hello").decode("ascii"),
    }


def test_terminal_session_manager_export(
    manager: TerminalSessionManager, monkeypatch: pytest.MonkeyPatch
):
    uploaded = {}

    def fake_upload(signed_url: str, content, content_length: int, deadline_unix_ms: int):
        uploaded["signed_url"] = signed_url
        uploaded["content"] = b"".join(bytes(chunk) for chunk in content)
        uploaded["content_length"] = content_length
        uploaded["deadline_unix_ms"] = deadline_unix_ms

    monkeypatch.setattr(session_manager_module, "_upload_to_signed_url", fake_upload)

    sandbox = FakeSandbox(
        run_fn=lambda command, timeout: FakeCommandResult(
            stdout='{"mime_type":"text/plain","size_bytes":5}'
        ),
        read_fn=lambda path, format, request_timeout: iter([b"he", b"llo"]),
    )
    register_session(manager, "sess-export", sandbox)

    result = manager.resolve_resource(
        session_id="sess-export",
        file_path="/tmp/hello.txt",
        action=TERMINAL_RESOURCE_ACTION_EXPORT,
        signed_url="https://uploads.example.com/put",
        deadline_unix_ms=0,
    )

    assert result == {
        "session_id": "sess-export",
        "file_path": "/tmp/hello.txt",
        "mime_type": "text/plain",
        "size_bytes": 5,
    }
    assert uploaded == {
        "signed_url": "https://uploads.example.com/put",
        "content": b"hello",
        "content_length": 5,
        "deadline_unix_ms": 0,
    }


@pytest.mark.parametrize(
    ("stdout", "exit_code", "code", "message"),
    [
        (
            '{"error":"file_not_found","message":"file not found"}',
            10,
            TERMINAL_RESOURCE_CODE_FILE_NOT_FOUND,
            "file not found",
        ),
        (
            '{"error":"path_is_directory","message":"path is directory"}',
            11,
            TERMINAL_RESOURCE_CODE_PATH_IS_DIR,
            "path is directory",
        ),
    ],
)
def test_terminal_session_manager_domain_errors(
    manager: TerminalSessionManager,
    stdout: str,
    exit_code: int,
    code: str,
    message: str,
):
    sandbox = FakeSandbox(
        run_fn=lambda command, timeout: FakeCommandResult(stdout=stdout, exit_code=exit_code)
    )
    register_session(manager, "sess-err", sandbox)

    with pytest.raises(TerminalExecError) as exc_info:
        manager.resolve_resource(
            session_id="sess-err",
            file_path="/tmp/missing.txt",
            action=TERMINAL_RESOURCE_ACTION_READ,
            signed_url="",
            deadline_unix_ms=0,
        )

    assert exc_info.value.code == code
    assert exc_info.value.message == message


@pytest.mark.parametrize(
    ("action", "output_limit_bytes", "export_max_bytes", "message"),
    [
        (TERMINAL_RESOURCE_ACTION_READ, 3, 0, "file exceeds read limit"),
        (TERMINAL_RESOURCE_ACTION_EXPORT, 1024, 3, "file exceeds export limit"),
    ],
)
def test_terminal_session_manager_oversized_file(
    action: str,
    output_limit_bytes: int,
    export_max_bytes: int,
    message: str,
):
    manager = TerminalSessionManager(
        e2b_template="base",
        e2b_timeout_sec=300,
        lease_min_sec=60,
        lease_max_sec=1800,
        lease_default_sec=60,
        output_limit_bytes=output_limit_bytes,
        export_max_bytes=export_max_bytes,
    )
    try:
        sandbox = FakeSandbox(
            run_fn=lambda command, timeout: FakeCommandResult(
                stdout='{"mime_type":"application/octet-stream","size_bytes":10}'
            ),
            read_fn=lambda path, format, request_timeout: pytest.fail(
                "files.read should not be called"
            ),
        )
        register_session(manager, "sess-large", sandbox)

        with pytest.raises(TerminalExecError) as exc_info:
            manager.resolve_resource(
                session_id="sess-large",
                file_path="/tmp/large.bin",
                action=action,
                signed_url="https://uploads.example.com/put",
                deadline_unix_ms=0,
            )

        assert exc_info.value.code == TERMINAL_RESOURCE_CODE_FILE_TOO_LARGE
        assert exc_info.value.message == message
    finally:
        manager.shutdown()


def test_terminal_session_manager_missing_and_busy_sessions(manager: TerminalSessionManager):
    with pytest.raises(TerminalExecError) as missing_exc:
        manager.resolve_resource(
            session_id="missing",
            file_path="/tmp/hello.txt",
            action=TERMINAL_RESOURCE_ACTION_VALIDATE,
            signed_url="",
            deadline_unix_ms=0,
        )
    assert missing_exc.value.code == "session_not_found"

    sandbox = FakeSandbox(
        run_fn=lambda command, timeout: FakeCommandResult(
            stdout='{"mime_type":"text/plain","size_bytes":5}'
        )
    )
    register_session(manager, "busy", sandbox, busy=True)

    with pytest.raises(TerminalExecError) as busy_exc:
        manager.resolve_resource(
            session_id="busy",
            file_path="/tmp/hello.txt",
            action=TERMINAL_RESOURCE_ACTION_VALIDATE,
            signed_url="",
            deadline_unix_ms=0,
        )
    assert busy_exc.value.code == "session_busy"


def test_terminal_session_manager_timeout_destroys_session(manager: TerminalSessionManager):
    sandbox = FakeSandbox(
        run_fn=lambda command, timeout: (_ for _ in ()).throw(TimeoutError("timeout"))
    )
    session = register_session(manager, "sess-timeout", sandbox)

    with pytest.raises(TerminalExecError) as exc_info:
        manager.resolve_resource(
            session_id="sess-timeout",
            file_path="/tmp/hello.txt",
            action=TERMINAL_RESOURCE_ACTION_VALIDATE,
            signed_url="",
            deadline_unix_ms=0,
        )

    assert exc_info.value.code == "deadline_exceeded"
    assert session.sandbox.killed is True
    with manager._lock:
        assert "sess-timeout" not in manager._sessions


def test_terminal_session_manager_recomputes_deadline_before_read(
    manager: TerminalSessionManager,
    monkeypatch: pytest.MonkeyPatch,
):
    now_sec = {"value": 0.0}

    def fake_time():
        return now_sec["value"]

    monkeypatch.setattr(session_manager_module.time, "time", fake_time)

    sandbox = FakeSandbox(
        run_fn=lambda command, timeout: (
            now_sec.__setitem__("value", 5.1),
            FakeCommandResult(stdout='{"mime_type":"text/plain","size_bytes":5}'),
        )[1],
        read_fn=lambda path, format, request_timeout: b"hello",
    )
    session = register_session(manager, "sess-read-deadline", sandbox)

    with pytest.raises(TerminalExecError) as exc_info:
        manager.resolve_resource(
            session_id="sess-read-deadline",
            file_path="/tmp/hello.txt",
            action=TERMINAL_RESOURCE_ACTION_READ,
            signed_url="",
            deadline_unix_ms=5000,
        )

    assert exc_info.value.code == "deadline_exceeded"
    assert session.sandbox.killed is True
    with manager._lock:
        assert "sess-read-deadline" not in manager._sessions


def test_terminal_session_manager_recomputes_deadline_before_export_upload(
    manager: TerminalSessionManager,
    monkeypatch: pytest.MonkeyPatch,
):
    now_sec = {"value": 0.0}
    uploaded = {}

    def fake_time():
        return now_sec["value"]

    def fake_upload(signed_url: str, content, content_length: int, deadline_unix_ms: int):
        uploaded["signed_url"] = signed_url
        uploaded["content"] = b"".join(bytes(chunk) for chunk in content)
        uploaded["content_length"] = content_length
        uploaded["remaining_sec"] = (deadline_unix_ms / 1000.0) - now_sec["value"]

    monkeypatch.setattr(session_manager_module.time, "time", fake_time)
    monkeypatch.setattr(session_manager_module, "_upload_to_signed_url", fake_upload)

    sandbox = FakeSandbox(
        run_fn=lambda command, timeout: (
            now_sec.__setitem__("value", 4.8),
            FakeCommandResult(stdout='{"mime_type":"text/plain","size_bytes":5}'),
        )[1],
        read_fn=lambda path, format, request_timeout: iter([b"he", b"llo"]),
    )
    register_session(manager, "sess-export-deadline", sandbox)

    result = manager.resolve_resource(
        session_id="sess-export-deadline",
        file_path="/tmp/hello.txt",
        action=TERMINAL_RESOURCE_ACTION_EXPORT,
        signed_url="https://uploads.example.com/put",
        deadline_unix_ms=5000,
    )

    assert result == {
        "session_id": "sess-export-deadline",
        "file_path": "/tmp/hello.txt",
        "mime_type": "text/plain",
        "size_bytes": 5,
    }
    assert uploaded == {
        "signed_url": "https://uploads.example.com/put",
        "content": b"hello",
        "content_length": 5,
        "remaining_sec": pytest.approx(0.2, abs=0.01),
    }


def test_terminal_session_manager_upload_not_found_does_not_destroy_session(
    manager: TerminalSessionManager,
    monkeypatch: pytest.MonkeyPatch,
):
    def fake_upload(signed_url: str, content, content_length: int, deadline_unix_ms: int):
        raise RuntimeError("upload export file failed: Not Found")

    monkeypatch.setattr(session_manager_module, "_upload_to_signed_url", fake_upload)

    sandbox = FakeSandbox(
        run_fn=lambda command, timeout: FakeCommandResult(
            stdout='{"mime_type":"text/plain","size_bytes":5}'
        ),
        read_fn=lambda path, format, request_timeout: iter([b"hello"]),
    )
    session = register_session(manager, "sess-upload-404", sandbox)

    with pytest.raises(TerminalExecError) as exc_info:
        manager.resolve_resource(
            session_id="sess-upload-404",
            file_path="/tmp/hello.txt",
            action=TERMINAL_RESOURCE_ACTION_EXPORT,
            signed_url="https://uploads.example.com/put",
            deadline_unix_ms=0,
        )

    assert exc_info.value.code == "execution_failed"
    assert "upload export file failed: Not Found" in exc_info.value.message
    assert session.sandbox.killed is False
    with manager._lock:
        assert manager._sessions["sess-upload-404"].busy is False


def test_terminal_session_manager_file_not_found_during_read_keeps_session(
    manager: TerminalSessionManager,
):
    sandbox = FakeSandbox(
        run_fn=lambda command, timeout: FakeCommandResult(
            stdout='{"mime_type":"text/plain","size_bytes":5}'
        ),
        read_fn=lambda path, format, request_timeout: (_ for _ in ()).throw(
            FileNotFoundException("file not found")
        ),
    )
    session = register_session(manager, "sess-read-missing", sandbox)

    with pytest.raises(TerminalExecError) as exc_info:
        manager.resolve_resource(
            session_id="sess-read-missing",
            file_path="/tmp/hello.txt",
            action=TERMINAL_RESOURCE_ACTION_READ,
            signed_url="",
            deadline_unix_ms=0,
        )

    assert exc_info.value.code == TERMINAL_RESOURCE_CODE_FILE_NOT_FOUND
    assert exc_info.value.message == "file not found"
    assert session.sandbox.killed is False
    with manager._lock:
        assert manager._sessions["sess-read-missing"].busy is False


def test_terminal_session_manager_sandbox_missing_during_read_destroys_session(
    manager: TerminalSessionManager,
):
    sandbox = FakeSandbox(
        run_fn=lambda command, timeout: FakeCommandResult(
            stdout='{"mime_type":"text/plain","size_bytes":5}'
        ),
        read_fn=lambda path, format, request_timeout: (_ for _ in ()).throw(
            SandboxNotFoundException("sandbox not found")
        ),
    )
    session = register_session(manager, "sess-read-sandbox-missing", sandbox)

    with pytest.raises(TerminalExecError) as exc_info:
        manager.resolve_resource(
            session_id="sess-read-sandbox-missing",
            file_path="/tmp/hello.txt",
            action=TERMINAL_RESOURCE_ACTION_READ,
            signed_url="",
            deadline_unix_ms=0,
        )

    assert exc_info.value.code == "session_not_found"
    assert exc_info.value.message == "session not found"
    assert session.sandbox.killed is True
    with manager._lock:
        assert "sess-read-sandbox-missing" not in manager._sessions
