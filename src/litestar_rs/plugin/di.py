"""Resolving an application's dependency graph outside a request.

A task has no request, no headers and no query string, so a provider that needs
any of them cannot be satisfied here. Rather than fail in the worker on the first
job, the graph is planned once at startup and a provider that cannot work in task
scope is reported then, by name, along with the parameter that gave it away.

Only the public surface of ``Provide`` is used: calling it applies ``use_cache``
and ``sync_to_thread``, and the two generator flags say whether the result needs
to be entered and torn down.
"""

import inspect
from collections.abc import AsyncIterator, Callable, Iterable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from litestar.di import Provide

from litestar_rs.core.errors import ConfigurationError


@dataclass(frozen=True, slots=True)
class DependencyPlan:
    """Providers a task needs, in the order they must be resolved."""

    order: tuple[str, ...]
    providers: Mapping[str, Provide]
    arguments: Mapping[str, tuple[str, ...]]

    def __bool__(self) -> bool:
        return bool(self.order)


def _underlying(provide: Provide) -> Callable[..., Any]:
    """The provider as written.

    ``sync_to_thread`` replaces the callable with a wrapper that keeps the
    original as ``func``; its own signature says nothing useful.
    """
    dependency = provide.dependency
    return getattr(dependency, "func", dependency)


def _parameters(provide: Provide) -> list[inspect.Parameter]:
    # eval_str resolves annotations in modules using postponed evaluation.
    signature = inspect.signature(_underlying(provide), eval_str=True)
    return list(signature.parameters.values())


def plan_dependencies(
    required: Iterable[str],
    dependencies: Mapping[str, Provide],
    *,
    task: str,
) -> DependencyPlan:
    """Work out what to resolve, in what order, and prove it is resolvable.

    Raises ``ConfigurationError`` for an unknown dependency, a provider that
    needs something only a request can supply, or a cycle.
    """
    order: list[str] = []
    arguments: dict[str, tuple[str, ...]] = {}
    visiting: list[str] = []

    def visit(key: str) -> None:
        if key in arguments:
            return
        if key in visiting:
            cycle = " -> ".join([*visiting[visiting.index(key) :], key])
            raise ConfigurationError(f"task {task!r} has a dependency cycle: {cycle}")
        provide = dependencies.get(key)
        if provide is None:
            raise ConfigurationError(
                f"task {task!r} needs dependency {key!r}, which the application "
                "does not provide"
            )

        visiting.append(key)
        needed: list[str] = []
        for parameter in _parameters(provide):
            if parameter.name in dependencies:
                visit(parameter.name)
                needed.append(parameter.name)
            elif parameter.default is inspect.Parameter.empty:
                raise ConfigurationError(
                    f"provider {key!r}, needed by task {task!r}, takes "
                    f"{parameter.name!r}, which only a request can supply; "
                    "a task has no request"
                )
        visiting.pop()

        arguments[key] = tuple(needed)
        order.append(key)

    for key in required:
        visit(key)

    return DependencyPlan(
        order=tuple(order),
        providers={key: dependencies[key] for key in order},
        arguments=arguments,
    )


@asynccontextmanager
async def resolved(plan: DependencyPlan) -> AsyncIterator[dict[str, Any]]:
    """Resolve the plan, hand over the values, and tear generators down after.

    Teardown runs in reverse, so a provider always closes before whatever it was
    built from.
    """
    values: dict[str, Any] = {}
    started: list[Any] = []
    try:
        for key in plan.order:
            provide = plan.providers[key]
            arguments = {name: values[name] for name in plan.arguments[key]}
            value = await provide(**arguments)
            if provide.has_sync_generator_dependency:
                started.append(value)
                value = next(value)
            elif provide.has_async_generator_dependency:
                started.append(value)
                value = await anext(value)
            values[key] = value
        yield values
    finally:
        for generator in reversed(started):
            if hasattr(generator, "__anext__"):
                await anext(generator, None)
            else:
                next(generator, None)
