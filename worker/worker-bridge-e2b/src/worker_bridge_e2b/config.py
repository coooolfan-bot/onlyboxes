import os
from dataclasses import dataclass, field

DEFAULT_CONSOLE_TARGET = "127.0.0.1:50051"
DEFAULT_HEARTBEAT_INTERVAL_SEC = 5
DEFAULT_HEARTBEAT_JITTER_PCT = 20
DEFAULT_MAX_INFLIGHT = 4
DEFAULT_LOG_LEVEL = "info"
DEFAULT_LOG_FORMAT = "json"
DEFAULT_EXECUTOR_KIND = "e2b"


@dataclass
class Config:
    console_grpc_target: str
    console_tls: bool
    worker_id: str
    worker_secret: str
    heartbeat_interval_sec: int
    heartbeat_jitter_pct: int
    node_name: str
    executor_kind: str
    version: str
    labels: dict[str, str]
    e2b_api_key: str
    e2b_sandbox_template: str
    e2b_sandbox_timeout_sec: int
    echo_max_inflight: int
    python_exec_max_inflight: int
    terminal_exec_max_inflight: int
    log_level: str
    log_format: str

    @classmethod
    def load(cls) -> "Config":
        return cls(
            console_grpc_target=_get_env("WORKER_CONSOLE_GRPC_TARGET", DEFAULT_CONSOLE_TARGET),
            console_tls=os.environ.get("WORKER_CONSOLE_INSECURE", "") != "true",
            worker_id=os.environ.get("WORKER_ID", "").strip(),
            worker_secret=os.environ.get("WORKER_SECRET", "").strip(),
            heartbeat_interval_sec=_parse_positive_int("WORKER_HEARTBEAT_INTERVAL_SEC", DEFAULT_HEARTBEAT_INTERVAL_SEC),
            heartbeat_jitter_pct=_parse_percent("WORKER_HEARTBEAT_JITTER_PCT", DEFAULT_HEARTBEAT_JITTER_PCT),
            node_name=os.environ.get("WORKER_NODE_NAME", "").strip(),
            executor_kind=DEFAULT_EXECUTOR_KIND,
            version=_get_env("WORKER_VERSION", "dev"),
            labels=_parse_labels(os.environ.get("WORKER_LABELS", "")),
            e2b_api_key=os.environ.get("E2B_API_KEY", "").strip(),
            e2b_sandbox_template=_get_env("E2B_SANDBOX_TEMPLATE", "base"),
            e2b_sandbox_timeout_sec=_parse_positive_int("E2B_SANDBOX_TIMEOUT_SEC", 300),
            echo_max_inflight=_parse_positive_int("WORKER_ECHO_MAX_INFLIGHT", DEFAULT_MAX_INFLIGHT),
            python_exec_max_inflight=_parse_positive_int("WORKER_PYTHON_EXEC_MAX_INFLIGHT", DEFAULT_MAX_INFLIGHT),
            terminal_exec_max_inflight=_parse_positive_int("WORKER_TERMINAL_EXEC_MAX_INFLIGHT", DEFAULT_MAX_INFLIGHT),
            log_level=_parse_log_level("WORKER_LOG_LEVEL", DEFAULT_LOG_LEVEL),
            log_format=_parse_log_format("WORKER_LOG_FORMAT", DEFAULT_LOG_FORMAT),
        )


def _get_env(key: str, default: str) -> str:
    value = os.environ.get(key, "").strip()
    return value if value else default


def _parse_positive_int(key: str, default: int) -> int:
    try:
        value = int(os.environ.get(key, ""))
        if value > 0:
            return value
    except (ValueError, TypeError):
        pass
    return default


def _parse_percent(key: str, default: int) -> int:
    try:
        value = int(os.environ.get(key, ""))
        if 0 <= value <= 100:
            return value
    except (ValueError, TypeError):
        pass
    return default


def _parse_log_level(key: str, default: str) -> str:
    value = os.environ.get(key, "").strip().lower()
    if value in ("debug", "info", "warn", "error"):
        return value
    return default


def _parse_log_format(key: str, default: str) -> str:
    value = os.environ.get(key, "").strip().lower()
    if value in ("json", "text"):
        return value
    return default


def _parse_labels(raw: str) -> dict[str, str]:
    labels = {}
    for part in raw.split(","):
        entry = part.strip()
        if not entry or "=" not in entry:
            continue
        key, _, value = entry.partition("=")
        key = key.strip()
        if key:
            labels[key] = value.strip()
    return labels
