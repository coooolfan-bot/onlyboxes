worker-bridge-e2b
=================

Onlyboxes worker bridge for e2b sandboxes.

Connects to the Onlyboxes console via gRPC and exposes e2b sandboxes
as a standard worker supporting echo, pythonExec, terminalExec, and
terminalResource.

Setup
-----

    uv sync   # requires Python 3.12+
    bash scripts/gen_proto.sh   # regenerate protobuf stubs

Run
---

    WORKER_CONSOLE_GRPC_TARGET=127.0.0.1:50051 \
    WORKER_CONSOLE_INSECURE=true \
    WORKER_ID=<node_id> \
    WORKER_SECRET=<secret> \
    E2B_API_KEY=<e2b_api_key> \
    E2B_PYTHON_EXEC_TEMPLATE=<template_for_python> \
    E2B_TERMINAL_EXEC_TEMPLATE=<template_for_terminal> \
    uv run worker-bridge-e2b

Capabilities
------------

### echo

Returns the input message unchanged.

### pythonExec

Runs Python code in a one-shot e2b sandbox. Each invocation creates a
fresh sandbox, writes the code to `/tmp/code.py`, and executes it via
`uv run /tmp/code.py` (supports PEP 723 inline dependencies). The
sandbox is destroyed after execution.

Sandbox template is configured via `E2B_PYTHON_EXEC_TEMPLATE`. The
template should have `uv` pre-installed (e.g. built from
`ghcr.io/astral-sh/uv:python3.12-bookworm-slim`).

### terminalExec

Runs shell commands in persistent e2b sandbox sessions with lease-based
TTL. Sessions are created on first use and reused for subsequent commands
sharing the same `session_id`. Idle sessions are automatically cleaned up
by a janitor thread when their lease expires.

Sandbox template is configured via `E2B_TERMINAL_EXEC_TEMPLATE`. This
can be a general-purpose template with the tools your terminal sessions
need.

### terminalResource

Validates, reads, or exports files from an existing terminalExec session.
`read` returns base64 content inline. `export` uploads the file to the
provided signed URL and forwards filtered upload headers from the console.

Environment Variables
---------------------

### Required

    WORKER_ID                    worker node ID
    WORKER_SECRET                worker secret
    E2B_API_KEY                  e2b API key

### Connection

    WORKER_CONSOLE_GRPC_TARGET   console gRPC address (default: 127.0.0.1:50051)
    WORKER_CONSOLE_INSECURE      set "true" to disable TLS (default: TLS enabled)
    WORKER_CALL_TIMEOUT_SEC      gRPC call/heartbeat ack timeout (default: ceil(2.5 * heartbeat))

### Worker identity

    WORKER_NODE_NAME             display name
    WORKER_VERSION               version string (default: dev)
    WORKER_LABELS                comma-separated key=value labels

### e2b sandbox

    E2B_PYTHON_EXEC_TEMPLATE     sandbox template for pythonExec (default: coolfan1024/python-exec)
    E2B_TERMINAL_EXEC_TEMPLATE   sandbox template for terminalExec (default: coolfan1024/terminal-exec)
    E2B_SANDBOX_TIMEOUT_SEC      sandbox lifetime in seconds (default: 300)

### Concurrency

    WORKER_ECHO_MAX_INFLIGHT              (default: 4)
    WORKER_PYTHON_EXEC_MAX_INFLIGHT       (default: 4)
    WORKER_TERMINAL_EXEC_MAX_INFLIGHT     (default: 4)
    WORKER_TERMINAL_RESOURCE_MAX_INFLIGHT (default: 4)

### Terminal session leases

    WORKER_TERMINAL_LEASE_MIN_SEC         (default: 60)
    WORKER_TERMINAL_LEASE_MAX_SEC         (default: 1800)
    WORKER_TERMINAL_LEASE_DEFAULT_SEC     (default: 60)
    WORKER_TERMINAL_OUTPUT_LIMIT_BYTES    (default: 1048576)
    WORKER_TERMINAL_EXPORT_MAX_BYTES      (default: 0, unlimited)

### Logging

    WORKER_LOG_LEVEL             debug|info|warn|error (default: info)
    WORKER_LOG_FORMAT            json|text (default: json)

### Heartbeat

    WORKER_HEARTBEAT_INTERVAL_SEC  (default: 5)
    WORKER_HEARTBEAT_JITTER_PCT    (default: 20)
