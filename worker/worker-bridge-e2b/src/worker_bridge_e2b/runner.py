"""gRPC session loop: connect -> hello -> heartbeat + dispatch handling."""

import asyncio
import json
import random
import time
import structlog

import grpc
from worker_bridge_e2b.config import Config
from worker_bridge_e2b.proto.registry.v1 import registry_pb2 as pb
from worker_bridge_e2b.proto.registry.v1 import registry_pb2_grpc as pb_grpc
from worker_bridge_e2b import executor

logger = structlog.get_logger()

INITIAL_RECONNECT_DELAY_SEC = 1.0
MAX_RECONNECT_DELAY_SEC = 15.0

ECHO_CAPABILITY = "echo"
PYTHON_EXEC_CAPABILITY = "pythonexec"
TERMINAL_EXEC_CAPABILITY = "terminalexec"


async def run(cfg: Config, stop_event: asyncio.Event) -> None:
    """Main loop: reconnect on failure until stop_event is set."""
    executor.init(cfg)
    try:
        reconnect_delay = INITIAL_RECONNECT_DELAY_SEC
        while not stop_event.is_set():
            try:
                await _run_session(cfg, stop_event)
                return
            except asyncio.CancelledError:
                return
            except grpc.RpcError as exc:
                if exc.code() == grpc.StatusCode.FAILED_PRECONDITION:
                    logger.warning("registry session replaced, reconnecting immediately")
                    reconnect_delay = INITIAL_RECONNECT_DELAY_SEC
                else:
                    logger.warning("registry session interrupted", error=str(exc))
            except Exception as exc:
                logger.warning("registry session error", error=str(exc))

            try:
                jitter = random.uniform(0, reconnect_delay * 0.2)
                await asyncio.wait_for(stop_event.wait(), timeout=reconnect_delay + jitter)
                return
            except asyncio.TimeoutError:
                pass
            reconnect_delay = min(reconnect_delay * 2, MAX_RECONNECT_DELAY_SEC)
    finally:
        executor.shutdown()


async def _run_session(cfg: Config, stop_event: asyncio.Event) -> None:
    creds = grpc.ssl_channel_credentials() if cfg.console_tls else grpc.local_channel_credentials()
    async with grpc.aio.secure_channel(cfg.console_grpc_target, creds) as channel:
        stub = pb_grpc.WorkerRegistryServiceStub(channel)

        request_queue: asyncio.Queue[pb.ConnectRequest] = asyncio.Queue()
        session_id: str | None = None

        async def request_iter():
            yield _build_hello(cfg)
            while not stop_event.is_set():
                try:
                    req = await asyncio.wait_for(request_queue.get(), timeout=1.0)
                    yield req
                except asyncio.TimeoutError:
                    continue

        stream = stub.Connect(request_iter())

        heartbeat_task = asyncio.create_task(
            _heartbeat_loop(cfg, request_queue, stop_event, lambda: session_id)
        )
        try:
            async for response in stream:
                which = response.WhichOneof("payload")
                if which == "connect_ack":
                    session_id = response.connect_ack.session_id
                    logger.info("connected", session_id=session_id)
                elif which == "heartbeat_ack":
                    pass
                elif which == "command_dispatch":
                    asyncio.create_task(
                        _handle_dispatch(response.command_dispatch, request_queue, cfg)
                    )
        finally:
            heartbeat_task.cancel()


async def _heartbeat_loop(
    cfg: Config,
    queue: asyncio.Queue,
    stop_event: asyncio.Event,
    get_session_id,
) -> None:
    while not stop_event.is_set():
        interval = cfg.heartbeat_interval_sec
        jitter = random.uniform(0, interval * cfg.heartbeat_jitter_pct / 100)
        await asyncio.sleep(interval + jitter)
        if stop_event.is_set():
            break
        hb = pb.ConnectRequest(
            heartbeat=pb.HeartbeatFrame(
                node_id=cfg.worker_id,
                session_id=get_session_id() or "",
            )
        )
        await queue.put(hb)


async def _handle_dispatch(
    dispatch: pb.CommandDispatch,
    queue: asyncio.Queue,
    cfg: Config,
) -> None:
    capability = dispatch.capability.lower()
    log = logger.bind(command_id=dispatch.command_id, capability=capability)

    try:
        loop = asyncio.get_event_loop()
        if capability == ECHO_CAPABILITY:
            result_payload, err_code, err_msg = await loop.run_in_executor(
                None, executor.execute_echo, dispatch.payload_json, dispatch.deadline_unix_ms
            )
        elif capability == PYTHON_EXEC_CAPABILITY:
            result_payload, err_code, err_msg = await loop.run_in_executor(
                None, executor.execute_python_exec, dispatch.payload_json, dispatch.deadline_unix_ms
            )
        elif capability == TERMINAL_EXEC_CAPABILITY:
            result_payload, err_code, err_msg = await loop.run_in_executor(
                None, executor.execute_terminal_exec, dispatch.payload_json, dispatch.deadline_unix_ms
            )
        else:
            err_code = "unsupported_capability"
            err_msg = f"capability not supported: {capability}"
            result_payload = b"{}"
            log.warning("unsupported capability")
    except NotImplementedError:
        err_code = "not_implemented"
        err_msg = f"capability {capability} is not yet implemented"
        result_payload = b"{}"
        log.warning("capability not implemented")
    except Exception as exc:
        err_code = "execution_failed"
        err_msg = str(exc)
        result_payload = b"{}"
        log.error("dispatch execution failed", error=err_msg)

    cmd_error = pb.CommandError(code=err_code, message=err_msg) if err_code else None
    result = pb.ConnectRequest(
        command_result=pb.CommandResult(
            command_id=dispatch.command_id,
            error=cmd_error,
            payload_json=result_payload,
            completed_unix_ms=int(time.time() * 1000),
        )
    )
    await queue.put(result)
    log.info("command result sent")


def _build_hello(cfg: Config) -> pb.ConnectRequest:
    capabilities = [
        pb.CapabilityDeclaration(name="echo", max_inflight=cfg.echo_max_inflight),
        pb.CapabilityDeclaration(name="pythonExec", max_inflight=cfg.python_exec_max_inflight),
        pb.CapabilityDeclaration(name="terminalExec", max_inflight=cfg.terminal_exec_max_inflight),
    ]
    hello = pb.ConnectHello(
        node_id=cfg.worker_id,
        node_name=cfg.node_name,
        executor_kind=cfg.executor_kind,
        version=cfg.version,
        capabilities=capabilities,
        worker_secret=cfg.worker_secret,
    )
    hello.labels.update(cfg.labels)
    return pb.ConnectRequest(hello=hello)
