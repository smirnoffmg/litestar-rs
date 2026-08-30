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
from typing import Any, overload
from uuid import uuid4

import msgspec
from litestar.di import Provide
from litestar.serialization import decode_json, encode_json, get_serializer
from litestar.types import TypeDecodersSequence, TypeEncodersMap

from litestar_rs.core.envelope import Envelope
from litestar_rs.core.errors import ConfigurationError
from litestar_rs.core.protocols import Enqueuer
from litestar_rs.plugin.di import DependencyPlan, plan_dependencies, resolved

DEFAULT_QUEUE = "default"


@dataclass(frozen=True, slots=True)
class BoundTask:
    """A task once the application's dependencies and codecs are known."""

    name: str
    queue: str
    function: Callable[..., Awaitable[None]]
    payload_type: type[msgspec.Struct]
    payload_fields: tuple[str, ...]
    plan: DependencyPlan


class Task[**P]:
    """What ``@registry.task`` gives back.

    Calling it runs the function directly, with its own signature intact, which
    is what keeps type checking useful. ``enqueue`` takes the payload parameters
    only -- the injected ones come from the application at execution time.
    """

    def __init__(
        self, registry: TaskRegistry, name: str, queue: str, function: Any
    ) -> None:
        self.registry = registry
        self.name = name
        self.queue = queue
        self.function = function
        self.__doc__ = function.__doc__
        self.__name__ = getattr(function, "__name__", name)

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> None:
        await self.function(*args, **kwargs)

    async def enqueue(self, **payload: Any) -> bytes:
        return await self.registry.enqueue(self.name, payload)


class TaskRegistry:
    """Tasks an application declares, and their execution once bound to it."""

    def __init__(self) -> None:
        self._declared: dict[str, Task[Any]] = {}
        self._bound: dict[str, BoundTask] = {}
        self._enqueuer: Enqueuer | None = None
        self._serializer: Callable[[Any], Any] | None = None
        self._decoders: TypeDecodersSequence | None = None

    @overload
    def task[**P](self, function: Callable[P, Awaitable[None]], /) -> Task[P]: ...

    @overload
    def task[**P](
        self, /, *, name: str | None = None, queue: str = DEFAULT_QUEUE
    ) -> Callable[[Callable[P, Awaitable[None]]], Task[P]]: ...

    def task[**P](
        self,
        function: Callable[P, Awaitable[None]] | None = None,
        /,
        *,
        name: str | None = None,
        queue: str = DEFAULT_QUEUE,
    ) -> Task[P] | Callable[[Callable[P, Awaitable[None]]], Task[P]]:
        def register(fn: Callable[P, Awaitable[None]]) -> Task[P]:
            task_name = name or fn.__name__
            if task_name in self._declared:
                raise ConfigurationError(f"task {task_name!r} is registered twice")
            declared = Task[P](self, task_name, queue, fn)
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
    ) -> None:
        """Settle every task against the application. Failures land here, on boot."""
        self._enqueuer = enqueuer
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

    async def enqueue(self, name: str, payload: Mapping[str, Any]) -> bytes:
        task = self.bound(name)
        if self._enqueuer is None:  # pragma: no cover - bind() always sets it
            raise ConfigurationError("registry has no enqueuer")
        # Building the payload struct here is what makes a bad argument a caller's
        # problem rather than a worker's.
        try:
            arguments = task.payload_type(**payload)
        except TypeError as exc:
            raise ConfigurationError(f"bad arguments for task {name!r}: {exc}") from exc
        envelope = Envelope(
            id=uuid4().hex,
            task=name,
            payload=encode_json(arguments, self._serializer),
            enqueued_at=time.time_ns() // 1_000_000,
        )
        return await self._enqueuer.enqueue(envelope, queue=task.queue)

    async def execute(self, envelope: Envelope) -> None:
        """Run a task from its stream entry: decode, inject, call."""
        task = self.bound(envelope.task)
        arguments = decode_json(envelope.payload, task.payload_type, self._decoders)
        payload = {field: getattr(arguments, field) for field in task.payload_fields}
        async with resolved(task.plan) as injected:
            await task.function(**payload, **injected)

    def handlers(self) -> dict[str, Callable[[Envelope], Awaitable[None]]]:
        return {name: self.execute for name in self._bound}


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

    return BoundTask(
        name=task.name,
        queue=task.queue,
        function=task.function,
        payload_type=msgspec.defstruct(f"{task.name}_payload", fields),
        payload_fields=tuple(field[0] for field in fields),
        plan=plan_dependencies(injected, dependencies, task=task.name),
    )
