"""Task registration, bound to the application's own serialization and DI.

A task is an ordinary function. Its parameters are split in two at startup:
whatever the application provides as a dependency is injected in the worker, and
the rest travels in the payload. There is no context dictionary and no way to
smuggle dependencies through one.
"""

from __future__ import annotations

import inspect
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from functools import partial
from typing import Any, overload
from uuid import uuid4

import anyio
import msgspec
from litestar.di import Provide
from litestar.serialization import decode_json, encode_json, get_serializer
from litestar.types import TypeDecodersSequence, TypeEncodersMap

from smallage.core.deferred import current_enqueuer
from smallage.core.envelope import Envelope, TaskResult
from smallage.core.errors import ConfigurationError
from smallage.core.protocols import (
    Enqueuer,
    PayloadStore,
    ResultStore,
    TaskHandler,
)
from smallage.litestar.di import DependencyPlan, plan_dependencies, resolved
from smallage.litestar.tracing import (
    TraceparentSource,
    current_traceparent,
    no_traceparent,
)

DEFAULT_QUEUE = "default"
DEFAULT_THREAD_LIMIT = 20
DEFAULT_OFFLOAD_OVER_BYTES = 128 * 1024


@dataclass(frozen=True, slots=True)
class BoundTask:
    """A task once the application's dependencies and codecs are known."""

    name: str
    queue: str
    function: Callable[..., Awaitable[None]]
    payload_type: type[msgspec.Struct]
    payload_fields: tuple[str, ...]
    injected: tuple[str, ...]
    """Dependencies this task's own signature names.

    Not the same as the plan: resolving `session` may require `settings`, and the
    task asked for neither of those on its behalf.
    """
    plan: DependencyPlan
    is_async: bool
    timeout_s: float | None


class Task[**P]:
    """What ``@registry.task`` gives back.

    Calling it runs the function directly, with its own signature intact, which
    is what keeps type checking useful. ``enqueue`` takes the payload parameters
    only -- the injected ones come from the application at execution time.
    """

    def __init__(
        self,
        registry: TaskRegistry,
        name: str,
        queue: str,
        function: Any,
        timeout_s: float | None = None,
    ) -> None:
        self.registry = registry
        self.name = name
        self.queue = queue
        self.function = function
        self.timeout_s = timeout_s
        self.__doc__ = function.__doc__
        self.__name__ = getattr(function, "__name__", name)

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Any:
        if inspect.iscoroutinefunction(self.function):
            return await self.function(*args, **kwargs)
        return self.function(*args, **kwargs)

    async def enqueue(
        self,
        *,
        result_ttl_ms: int | None = None,
        dedup: str | None = None,
        **payload: Any,
    ) -> str:
        """Queue the task and hand back its job id.

        The id is what a result is looked up by, so it is returned even when
        nobody asked for one.
        """
        return await self.registry.enqueue(
            self.name, payload, result_ttl_ms=result_ttl_ms, dedup=dedup
        )


class TaskRegistry:
    """Tasks an application declares, and their execution once bound to it."""

    def __init__(self) -> None:
        self._declared: dict[str, Task[Any]] = {}
        self._bound: dict[str, BoundTask] = {}
        self._enqueuer: Enqueuer | None = None
        self._traceparent: TraceparentSource = no_traceparent
        self._results: ResultStore | None = None
        self._payloads: PayloadStore | None = None
        self._threads = anyio.CapacityLimiter(DEFAULT_THREAD_LIMIT)
        self._offload_over_bytes = DEFAULT_OFFLOAD_OVER_BYTES
        self._serializer: Callable[[Any], Any] | None = None
        self._decoders: TypeDecodersSequence | None = None

    @overload
    def task[**P](self, function: Callable[P, Any], /) -> Task[P]: ...

    @overload
    def task[**P](
        self,
        /,
        *,
        name: str | None = None,
        queue: str = DEFAULT_QUEUE,
        timeout_s: float | None = None,
    ) -> Callable[[Callable[P, Any]], Task[P]]: ...

    def task[**P](
        self,
        function: Callable[P, Any] | None = None,
        /,
        *,
        name: str | None = None,
        queue: str = DEFAULT_QUEUE,
        timeout_s: float | None = None,
    ) -> Task[P] | Callable[[Callable[P, Any]], Task[P]]:
        def register(fn: Callable[P, Any]) -> Task[P]:
            task_name = name or fn.__name__
            if task_name in self._declared:
                raise ConfigurationError(f"task {task_name!r} is registered twice")
            declared = Task[P](self, task_name, queue, fn, timeout_s)
            self._declared[task_name] = declared
            return declared

        return register if function is None else register(function)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(self._declared)

    def bind(
        self,
        dependencies: Mapping[str, Provide],
        *,
        enqueuer: Enqueuer,
        type_encoders: TypeEncodersMap | None = None,
        type_decoders: TypeDecodersSequence | None = None,
        traceparent: TraceparentSource = no_traceparent,
        results: ResultStore | None = None,
        payloads: PayloadStore | None = None,
        thread_limit: int = DEFAULT_THREAD_LIMIT,
        offload_over_bytes: int = DEFAULT_OFFLOAD_OVER_BYTES,
    ) -> None:
        """Settle every task against the application. Failures land here, on boot."""
        self._enqueuer = enqueuer
        self._traceparent = traceparent
        self._results = results
        self._payloads = payloads
        self._threads = anyio.CapacityLimiter(thread_limit)
        self._offload_over_bytes = offload_over_bytes
        self._serializer = get_serializer(type_encoders)
        self._decoders = type_decoders
        self._bound = {
            name: _bind(task, dependencies) for name, task in self._declared.items()
        }

    def bound(self, name: str) -> BoundTask:
        task = self._bound.get(name)
        if task is None:
            raise ConfigurationError(
                f"task {name!r} is not bound; the plugin binds tasks at app startup"
            )
        return task

    async def result(self, job_id: str) -> TaskResult | None:
        if self._results is None:
            raise ConfigurationError("this application has no result store configured")
        return await self._results.get(job_id)

    async def enqueue(
        self,
        name: str,
        payload: Mapping[str, Any],
        *,
        result_ttl_ms: int | None = None,
        dedup: str | None = None,
    ) -> str:
        task = self.bound(name)
        # A unit of work may have bound its own, so that publication can wait for
        # the transaction that justified it.
        enqueuer = current_enqueuer.get() or self._enqueuer
        if enqueuer is None:  # pragma: no cover - bind() always sets it
            raise ConfigurationError("registry has no enqueuer")
        # Building the payload struct here is what makes a bad argument a caller's
        # problem rather than a worker's.
        try:
            arguments = task.payload_type(**payload)
        except TypeError as exc:
            raise ConfigurationError(f"bad arguments for task {name!r}: {exc}") from exc
        job_id = uuid4().hex
        encoded = encode_json(arguments, self._serializer)
        reference: str | None = None
        if len(encoded) > self._offload_over_bytes and self._payloads is not None:
            # Redis keeps the stream in memory; a large payload belongs elsewhere.
            reference = await self._payloads.put(job_id, encoded)
            encoded = b""
        envelope = Envelope(
            id=job_id,
            task=name,
            payload=encoded,
            payload_ref=reference,
            enqueued_at=time.time_ns() // 1_000_000,
            # Beside the payload, never inside it: restoring the span must not
            # require decoding a payload the worker may not understand.
            traceparent=self._traceparent(),
            result_ttl_ms=result_ttl_ms,
            dedup=dedup,
        )
        await enqueuer.enqueue(envelope, queue=task.queue)
        return job_id

    async def execute(self, envelope: Envelope) -> Any:
        """Run a task from its stream entry: decode, inject, call."""
        task = self.bound(envelope.task)
        raw = envelope.payload
        if envelope.payload_ref is not None:
            if self._payloads is None:
                raise ConfigurationError(
                    f"job {envelope.id!r} carries a payload reference but this "
                    "application has no payload store"
                )
            raw = await self._payloads.get(envelope.payload_ref)
        arguments = decode_json(raw, task.payload_type, self._decoders)
        payload = {field: getattr(arguments, field) for field in task.payload_fields}
        token = current_traceparent.set(envelope.traceparent)
        try:
            async with resolved(task.plan) as resolved_values:
                given = {name: resolved_values[name] for name in task.injected}
                return await self._call(task, {**payload, **given})
        finally:
            current_traceparent.reset(token)

    async def _call(self, task: BoundTask, arguments: dict[str, Any]) -> Any:
        if not task.is_async:
            # Off the event loop, under a limiter this project sizes itself: the
            # asyncio default silently caps throughput and starves the heartbeat.
            return await anyio.to_thread.run_sync(
                partial(task.function, **arguments), limiter=self._threads
            )
        if task.timeout_s is None:
            return await task.function(**arguments)
        with anyio.fail_after(task.timeout_s):
            return await task.function(**arguments)

    def handlers(self) -> dict[str, TaskHandler]:
        """What the worker dispatches on: one entry per bound task."""
        return dict.fromkeys(self._bound, self.execute)


def _bind(task: Task[Any], dependencies: Mapping[str, Provide]) -> BoundTask:
    # eval_str resolves annotations in modules using postponed evaluation.
    signature = inspect.signature(task.function, eval_str=True)
    injected: list[str] = []
    fields: list[tuple[str, Any] | tuple[str, Any, Any]] = []

    for parameter in signature.parameters.values():
        if parameter.name in dependencies:
            injected.append(parameter.name)
            continue
        if parameter.annotation is inspect.Parameter.empty:
            raise ConfigurationError(
                f"task {task.name!r} takes {parameter.name!r} without an "
                "annotation; payload arguments must be typed to be validated"
            )
        if parameter.default is inspect.Parameter.empty:
            fields.append((parameter.name, parameter.annotation))
        else:
            fields.append((parameter.name, parameter.annotation, parameter.default))

    is_async = inspect.iscoroutinefunction(task.function)
    if task.timeout_s is not None and not is_async:
        # asyncio.timeout cancels at the next await; a thread has none, and
        # threads cannot be killed. Promising a timeout here would be a lie.
        raise ConfigurationError(
            f"task {task.name!r} is synchronous and cannot be given a timeout; "
            "timeouts are guaranteed for async tasks only"
        )

    return BoundTask(
        name=task.name,
        queue=task.queue,
        function=task.function,
        payload_type=msgspec.defstruct(f"{task.name}_payload", fields),
        payload_fields=tuple(field[0] for field in fields),
        injected=tuple(injected),
        plan=plan_dependencies(injected, dependencies, task=task.name),
        is_async=is_async,
        timeout_s=task.timeout_s,
    )
