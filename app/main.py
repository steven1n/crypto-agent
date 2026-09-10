"""Logging and backward-compatible public capture entry point."""

import logging

import structlog

from config.settings import Settings


def configure_logging(level: str) -> None:
    logging.basicConfig(level=level, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
    )


async def run(settings: Settings) -> None:
    from app.capture import run_capture

    await run_capture(settings)


def main() -> None:
    from app.capture import main as capture_main

    capture_main()


if __name__ == "__main__":
    main()
