import asyncio
import signal
import sys

import structlog

from worker_bridge_e2b import logging as wlogging
from worker_bridge_e2b.config import Config
from worker_bridge_e2b.runner import run

logger = structlog.get_logger()


def main() -> None:
    cfg = Config.load()
    wlogging.configure(cfg.log_level, cfg.log_format)

    if not cfg.worker_id:
        logger.error("WORKER_ID is required")
        sys.exit(1)
    if not cfg.worker_secret:
        logger.error("WORKER_SECRET is required")
        sys.exit(1)
    if not cfg.e2b_api_key:
        logger.error("E2B_API_KEY is required")
        sys.exit(1)

    logger.info(
        "worker-bridge-e2b starting",
        node_id=cfg.worker_id,
        console=cfg.console_grpc_target,
        tls=cfg.console_tls,
        version=cfg.version,
        python_exec_template=cfg.e2b_python_exec_template,
        terminal_exec_template=cfg.e2b_terminal_exec_template,
    )

    asyncio.run(_run_async(cfg))


async def _run_async(cfg: Config) -> None:
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    await run(cfg, stop_event)
    logger.info("worker stopped")


if __name__ == "__main__":
    main()
