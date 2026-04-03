import logging

import structlog


def configure(level: str, fmt: str) -> None:
    log_level = getattr(logging, level.upper(), logging.INFO)

    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
    ]

    if fmt == "json":
        processors.append(structlog.processors.JSONRenderer())  # ty: ignore[invalid-argument-type]
    else:
        processors.append(structlog.dev.ConsoleRenderer())  # ty: ignore[invalid-argument-type]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )

    logging.basicConfig(level=log_level)
