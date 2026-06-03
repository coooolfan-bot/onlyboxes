from __future__ import annotations

import os
from dataclasses import dataclass

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
    call_timeout_sec: int
    node_name: str
    executor_kind: str
    version: str
    labels: dict[str, str]
    e2b_api_key: str
    e2b_python_exec_template: str
    e2b_terminal_exec_template: str
    e2b_sandbox_timeout_sec: int
    echo_max_inflight: int
    python_exec_max_inflight: int
    terminal_exec_max_inflight: int
    terminal_resource_max_inflight: int
    terminal_lease_min_sec: int
    terminal_lease_max_sec: int
    terminal_lease_default_sec: int
    terminal_output_limit_bytes: int
    terminal_export_max_bytes: int
    log_level: str
    log_format: str

    @classmethod
    def load(cls) -> Config:
        heartbeat_interval_sec = _parse_positive_int(
            "WORKER_HEARTBEAT_INTERVAL_SEC", DEFAULT_HEARTBEAT_INTERVAL_SEC
        )
        cfg = cls(
            console_grpc_target=_get_env("WORKER_CONSOLE_GRPC_TARGET", DEFAULT_CONSOLE_TARGET),
            console_tls=os.environ.get("WORKER_CONSOLE_INSECURE", "") != "true",
            worker_id=os.environ.get("WORKER_ID", "").strip(),
            worker_secret=os.environ.get("WORKER_SECRET", "").strip(),
            heartbeat_interval_sec=heartbeat_interval_sec,
            heartbeat_jitter_pct=_parse_percent(
                "WORKER_HEARTBEAT_JITTER_PCT", DEFAULT_HEARTBEAT_JITTER_PCT
            ),
            call_timeout_sec=_parse_positive_int(
                "WORKER_CALL_TIMEOUT_SEC", _default_call_timeout_sec(heartbeat_interval_sec)
            ),
            node_name=os.environ.get("WORKER_NODE_NAME", "").strip(),
            executor_kind=DEFAULT_EXECUTOR_KIND,
            version=_get_env("WORKER_VERSION", "dev"),
            labels=_parse_labels(os.environ.get("WORKER_LABELS", "")),
            e2b_api_key=os.environ.get("E2B_API_KEY", "").strip(),
            e2b_python_exec_template=_get_env(
                "E2B_PYTHON_EXEC_TEMPLATE", "coolfan1024/python-exec"
            ),
            e2b_terminal_exec_template=_get_env(
                "E2B_TERMINAL_EXEC_TEMPLATE", "coolfan1024/terminal-exec"
            ),
            e2b_sandbox_timeout_sec=_parse_positive_int("E2B_SANDBOX_TIMEOUT_SEC", 300),
            echo_max_inflight=_parse_positive_int("WORKER_ECHO_MAX_INFLIGHT", DEFAULT_MAX_INFLIGHT),
            python_exec_max_inflight=_parse_positive_int(
                "WORKER_PYTHON_EXEC_MAX_INFLIGHT", DEFAULT_MAX_INFLIGHT
            ),
            terminal_exec_max_inflight=_parse_positive_int(
                "WORKER_TERMINAL_EXEC_MAX_INFLIGHT", DEFAULT_MAX_INFLIGHT
            ),
            terminal_resource_max_inflight=_parse_positive_int(
                "WORKER_TERMINAL_RESOURCE_MAX_INFLIGHT", DEFAULT_MAX_INFLIGHT
            ),
            terminal_lease_min_sec=_parse_positive_int("WORKER_TERMINAL_LEASE_MIN_SEC", 60),
            terminal_lease_max_sec=_parse_positive_int("WORKER_TERMINAL_LEASE_MAX_SEC", 1800),
            terminal_lease_default_sec=_parse_positive_int("WORKER_TERMINAL_LEASE_DEFAULT_SEC", 60),
            terminal_output_limit_bytes=_parse_positive_int(
                "WORKER_TERMINAL_OUTPUT_LIMIT_BYTES", 1048576
            ),
            terminal_export_max_bytes=_parse_non_negative_int(
                "WORKER_TERMINAL_EXPORT_MAX_BYTES", 0
            ),
            log_level=_parse_log_level("WORKER_LOG_LEVEL", DEFAULT_LOG_LEVEL),
            log_format=_parse_log_format("WORKER_LOG_FORMAT", DEFAULT_LOG_FORMAT),
        )
        # Clamp lease values: max >= min, default in [min, max]
        if cfg.terminal_lease_max_sec < cfg.terminal_lease_min_sec:
            cfg.terminal_lease_max_sec = cfg.terminal_lease_min_sec
        if cfg.terminal_lease_default_sec < cfg.terminal_lease_min_sec:
            cfg.terminal_lease_default_sec = cfg.terminal_lease_min_sec
        if cfg.terminal_lease_default_sec > cfg.terminal_lease_max_sec:
            cfg.terminal_lease_default_sec = cfg.terminal_lease_max_sec
        return cfg


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


def _parse_non_negative_int(key: str, default: int) -> int:
    try:
        value = int(os.environ.get(key, ""))
        if value >= 0:
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


def _default_call_timeout_sec(heartbeat_interval_sec: int) -> int:
    heartbeat = (
        heartbeat_interval_sec if heartbeat_interval_sec > 0 else DEFAULT_HEARTBEAT_INTERVAL_SEC
    )
    return (heartbeat * 5 + 1) // 2
