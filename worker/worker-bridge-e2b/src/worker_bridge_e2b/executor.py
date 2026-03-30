"""e2b capability executors.

Each function receives the raw payload_json bytes from CommandDispatch
and returns (result_json_bytes, error_code, error_message).
error_code/error_message are empty strings on success.
"""

import json
import time

type ExecutorResult = tuple[bytes, str, str]


def execute_echo(payload: bytes, deadline_unix_ms: int) -> ExecutorResult:
    data = json.loads(payload)
    message = data.get("message", "")
    result = json.dumps({"message": message}).encode()
    return result, "", ""


def execute_python_exec(payload: bytes, deadline_unix_ms: int) -> ExecutorResult:
    # TODO: create/reuse an e2b sandbox and run the code
    raise NotImplementedError


def execute_terminal_exec(payload: bytes, deadline_unix_ms: int) -> ExecutorResult:
    # TODO: run a shell command in an e2b sandbox, manage session leases
    raise NotImplementedError
