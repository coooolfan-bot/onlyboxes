"""gRPC session loop: connect -> hello -> heartbeat + dispatch handling."""

from __future__ import annotations

import asyncio
import contextlib
import random
import time
from typing import TYPE_CHECKING

import grpc
import structlog

from worker_bridge_e2b import executor
from worker_bridge_e2b.proto.registry.v1 import registry_pb2 as pb
from worker_bridge_e2b.proto.registry.v1 import registry_pb2_grpc as pb_grpc

if TYPE_CHECKING:
    from worker_bridge_e2b.config import Config

logger = structlog.get_logger()

INITIAL_RECONNECT_DELAY_SEC = 1.0
MAX_RECONNECT_DELAY_SEC = 15.0
REQUEST_QUEUE_SIZE = 64
HEARTBEAT_ACK_QUEUE_SIZE = 16

ECHO_CAPABILITY = "echo"
PYTHON_EXEC_CAPABILITY = "pythonexec"
TERMINAL_EXEC_CAPABILITY = "terminalexec"
TERMINAL_RESOURCE_CAPABILITY = "terminalresource"


class SessionEndedError(Exception):
    """Reconnectable registry session failure."""


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
            except TimeoutError:
                pass
            reconnect_delay = min(reconnect_delay * 2, MAX_RECONNECT_DELAY_SEC)
    finally:
        executor.shutdown()


async def _run_session(cfg: Config, stop_event: asyncio.Event) -> None:
    async with _open_channel(cfg) as channel:
        stub = pb_grpc.WorkerRegistryServiceStub(channel)

        request_queue: asyncio.Queue[pb.ConnectRequest] = asyncio.Queue(maxsize=REQUEST_QUEUE_SIZE)
        heartbeat_ack_queue: asyncio.Queue[pb.HeartbeatAck] = asyncio.Queue(
            maxsize=HEARTBEAT_ACK_QUEUE_SIZE
        )
        dispatch_tasks: set[asyncio.Task] = set()

        async def request_iter():
            yield _build_hello(cfg)
            while not stop_event.is_set():
                req = await request_queue.get()
                yield req

        stream = stub.Connect(request_iter())
        response_iter = stream.__aiter__()

        first = await _read_response(response_iter, cfg.call_timeout_sec, "connect_ack")
        if first.WhichOneof("payload") != "connect_ack":
            raise SessionEndedError("unexpected first response frame")
        session_id = first.connect_ack.session_id.strip()
        if not session_id:
            raise SessionEndedError("connect_ack.session_id is required")

        heartbeat_interval_sec = _duration_from_server(
            first.connect_ack.heartbeat_interval_sec, cfg.heartbeat_interval_sec
        )
        logger.info("connected", session_id=session_id)

        heartbeat_task = asyncio.create_task(
            _heartbeat_loop(
                cfg,
                request_queue,
                heartbeat_ack_queue,
                stop_event,
                session_id,
                heartbeat_interval_sec,
            )
        )
        response_task: asyncio.Task | None = None
        try:
            while not stop_event.is_set():
                response_task = asyncio.create_task(anext(response_iter))
                done, _ = await asyncio.wait(
                    {response_task, heartbeat_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if heartbeat_task in done:
                    if stop_event.is_set():
                        return
                    await heartbeat_task
                    raise SessionEndedError("heartbeat loop stopped")

                try:
                    response = response_task.result()
                except StopAsyncIteration as exc:
                    raise SessionEndedError("registry stream closed") from exc

                which = response.WhichOneof("payload")
                if which == "heartbeat_ack":
                    _enqueue_heartbeat_ack(heartbeat_ack_queue, response.heartbeat_ack)
                elif which == "command_dispatch":
                    task = asyncio.create_task(
                        _handle_dispatch(response.command_dispatch, request_queue, cfg)
                    )
                    dispatch_tasks.add(task)
                    task.add_done_callback(dispatch_tasks.discard)
                    task.add_done_callback(_log_task_exception)
                else:
                    raise SessionEndedError("unexpected response frame")
        finally:
            with contextlib.suppress(Exception):
                stream.cancel()
            heartbeat_task.cancel()
            if response_task is not None and not response_task.done():
                response_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await response_task
            for task in dispatch_tasks:
                task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
            if dispatch_tasks:
                await asyncio.gather(*dispatch_tasks, return_exceptions=True)


def _open_channel(cfg: Config):
    if cfg.console_tls:
        return grpc.aio.secure_channel(cfg.console_grpc_target, grpc.ssl_channel_credentials())
    return grpc.aio.insecure_channel(cfg.console_grpc_target)


async def _read_response(response_iter, timeout_sec: int, expected: str) -> pb.ConnectResponse:
    try:
        return await asyncio.wait_for(anext(response_iter), timeout=timeout_sec)
    except StopAsyncIteration as exc:
        raise SessionEndedError(f"stream closed before {expected}") from exc
    except TimeoutError as exc:
        raise SessionEndedError(f"receive {expected} timed out") from exc


async def _heartbeat_loop(
    cfg: Config,
    queue: asyncio.Queue,
    heartbeat_ack_queue: asyncio.Queue,
    stop_event: asyncio.Event,
    session_id: str,
    heartbeat_interval_sec: int,
) -> None:
    interval_sec = heartbeat_interval_sec
    consecutive_ack_timeouts = 0

    while not stop_event.is_set():
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=_jitter_duration_sec(interval_sec, cfg.heartbeat_jitter_pct),
            )
            return
        except TimeoutError:
            pass

        hb = pb.ConnectRequest(
            heartbeat=pb.HeartbeatFrame(
                node_id=cfg.worker_id,
                session_id=session_id,
                active_session_count=executor.active_session_count(),
            )
        )
        await _enqueue_request(queue, hb, cfg.call_timeout_sec)

        try:
            ack = await asyncio.wait_for(heartbeat_ack_queue.get(), timeout=cfg.call_timeout_sec)
        except TimeoutError as exc:
            consecutive_ack_timeouts += 1
            if consecutive_ack_timeouts >= 2:
                raise SessionEndedError("heartbeat ack deadline exceeded") from exc
            continue

        consecutive_ack_timeouts = 0
        interval_sec = _duration_from_server(ack.heartbeat_interval_sec, interval_sec)


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
                None,
                executor.execute_terminal_exec,
                dispatch.payload_json,
                dispatch.deadline_unix_ms,
            )
        elif capability == TERMINAL_RESOURCE_CAPABILITY:
            result_payload, err_code, err_msg = await loop.run_in_executor(
                None,
                executor.execute_terminal_resource,
                dispatch.payload_json,
                dispatch.deadline_unix_ms,
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
    await _enqueue_request(queue, result, cfg.call_timeout_sec)
    log.info("command result sent")


def _build_hello(cfg: Config) -> pb.ConnectRequest:
    capabilities = [
        pb.CapabilityDeclaration(name="echo", max_inflight=cfg.echo_max_inflight),
        pb.CapabilityDeclaration(name="pythonExec", max_inflight=cfg.python_exec_max_inflight),
        pb.CapabilityDeclaration(name="terminalExec", max_inflight=cfg.terminal_exec_max_inflight),
        pb.CapabilityDeclaration(
            name="terminalResource", max_inflight=cfg.terminal_resource_max_inflight
        ),
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


async def _enqueue_request(
    queue: asyncio.Queue,
    request: pb.ConnectRequest,
    timeout_sec: int,
) -> None:
    await asyncio.wait_for(queue.put(request), timeout=timeout_sec)


def _enqueue_heartbeat_ack(queue: asyncio.Queue, ack: pb.HeartbeatAck) -> None:
    try:
        queue.put_nowait(ack)
        return
    except asyncio.QueueFull:
        with contextlib.suppress(asyncio.QueueEmpty):
            queue.get_nowait()
    queue.put_nowait(ack)


def _duration_from_server(seconds: int, fallback: int) -> int:
    if seconds > 0:
        return seconds
    if fallback > 0:
        return fallback
    return 5


def _jitter_duration_sec(base_sec: int, jitter_pct: int) -> float:
    base = float(base_sec if base_sec > 0 else 5)
    pct = max(0, min(jitter_pct, 100))
    if pct == 0:
        return base
    min_value = max(1.0, base * (100 - pct) / 100)
    max_value = max(min_value, base * (100 + pct) / 100)
    return random.uniform(min_value, max_value)


def _log_task_exception(task: asyncio.Task) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("dispatch task failed", error=str(exc))
