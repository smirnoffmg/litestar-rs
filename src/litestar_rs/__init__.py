"""Distributed task queue on Redis Streams with first-class Litestar integration.

Delivery is at-least-once: handlers must be idempotent.
"""

from litestar_rs.core import (
    Codec,
    CollectingEnqueuer,
    ConfigurationError,
    CronJob,
    EagerEnqueuer,
    Enqueuer,
    Envelope,
    JsonCodec,
    LitestarRsError,
    MalformedEnvelope,
    PayloadTooLarge,
    Pending,
    Record,
    RedisResultStore,
    RedisScheduler,
    RedisStreamsTransport,
    ResultStore,
    RetryPolicy,
    Scheduler,
    StreamTransport,
    TaskHandler,
    TaskResult,
    WorkerConfig,
    run,
    run_with_signals,
)

__version__ = "0.1.0"

__all__ = [
    "Codec",
    "CollectingEnqueuer",
    "ConfigurationError",
    "CronJob",
    "EagerEnqueuer",
    "Enqueuer",
    "Envelope",
    "JsonCodec",
    "LitestarRsError",
    "MalformedEnvelope",
    "PayloadTooLarge",
    "Pending",
    "Record",
    "RedisResultStore",
    "RedisScheduler",
    "RedisStreamsTransport",
    "ResultStore",
    "RetryPolicy",
    "Scheduler",
    "StreamTransport",
    "TaskHandler",
    "TaskResult",
    "WorkerConfig",
    "__version__",
    "run",
    "run_with_signals",
]
