"""Plugin configuration is checked when it is built, not when it is used."""

import pytest

from smallage.core.errors import ConfigurationError
from smallage.litestar.config import QueueConfig
from smallage.litestar.health import QueueHealth
from smallage.litestar.registry import TaskRegistry

pytestmark = pytest.mark.unit


def test_an_empty_redis_url_is_refused() -> None:
    with pytest.raises(ConfigurationError, match="redis_url"):
        QueueConfig(registry=TaskRegistry(), redis_url="")


def test_a_worker_runs_the_application_lifespan_unless_told_otherwise() -> None:
    """The opt-out is a property of the application, so it lives in the config."""
    assert QueueConfig(registry=TaskRegistry()).run_app_lifespan is True
    assert (
        QueueConfig(registry=TaskRegistry(), run_app_lifespan=False).run_app_lifespan
        is False
    )


def test_the_consumer_name_defaults_to_being_derived() -> None:
    """`consumer_prefix` plus a suffix, unless the deployment names the worker."""
    assert QueueConfig(registry=TaskRegistry()).consumer is None
    assert QueueConfig(registry=TaskRegistry(), consumer="w-1").consumer == "w-1"


def test_an_application_may_own_no_queues() -> None:
    """A pure broker deployment otherwise names a queue nobody writes to; the
    transport is what refuses a worker that would read nothing."""
    config = QueueConfig(registry=TaskRegistry(), queues=())

    assert config.queues == ()


def test_health_says_unhealthy_when_redis_cannot_report_lag() -> None:
    """A missing depth reading is not a zero-depth queue."""
    reported = QueueHealth(
        namespace="lrs", group="workers", queues=(), lag=0, stats={}, healthy=True
    )
    unknown = QueueHealth(
        namespace="lrs", group="workers", queues=(), lag=None, stats={}, healthy=False
    )

    assert reported.healthy is True
    assert unknown.healthy is False


def test_healthy_reaches_the_wire() -> None:
    """It used to be a property, which serialises to nothing at all."""
    from litestar.serialization import encode_json

    body = encode_json(
        QueueHealth(
            namespace="lrs",
            group="workers",
            queues=(),
            lag=None,
            stats={},
            healthy=False,
        )
    )

    assert b'"healthy":false' in body


def test_every_transport_setting_is_reachable_from_the_configuration() -> None:
    """A knob the plugin cannot reach is a knob nobody using the plugin has.

    Broker handlers were configurable in name only once already; this is what
    says so before somebody goes looking for a setting that is not wired.
    """
    import dataclasses
    import inspect

    from smallage import RedisStreamsTransport

    transport_settings = {
        name
        for name, parameter in inspect.signature(
            RedisStreamsTransport.__init__
        ).parameters.items()
        if parameter.kind is inspect.Parameter.KEYWORD_ONLY
    }
    # The plugin builds the clients and the consumer name itself.
    built_by_the_plugin = {"reader", "control", "consumer"}
    configurable = {field.name for field in dataclasses.fields(QueueConfig)}
    # `external` is derived from `brokers`, which carries the handlers with it.
    configurable.add("external")

    missing = transport_settings - built_by_the_plugin - configurable
    assert not missing, f"not reachable through QueueConfig: {sorted(missing)}"


def test_every_registry_setting_is_reachable_from_the_configuration() -> None:
    """The payload store was bindable and not configurable, so offloading a large
    argument through the plugin did nothing at all."""
    import dataclasses
    import inspect

    bind_settings = {
        name
        for name, parameter in inspect.signature(TaskRegistry.bind).parameters.items()
        if parameter.kind is inspect.Parameter.KEYWORD_ONLY
    }
    # The plugin is the enqueuer and the result store, and supplies the app's own
    # codecs and trace source.
    supplied_by_the_plugin = {
        "enqueuer",
        "results",
        "type_encoders",
        "type_decoders",
        "traceparent",
    }
    configurable = {field.name for field in dataclasses.fields(QueueConfig)}

    missing = bind_settings - supplied_by_the_plugin - configurable
    assert not missing, f"not reachable through QueueConfig: {sorted(missing)}"
