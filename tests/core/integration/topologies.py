"""Redis deployments this library claims to work on.

Cluster and Sentinel are alternative high-availability models -- Cluster does its
own failover and does not use Sentinel -- so there is no deployment running both.
What can be shared is the check: the same operations against each of them.

Every container publishes its ports one to one, so the address a node announces
is the address a client on the host can reach. Anything else and Sentinel hands
out an address that only exists inside Docker.
"""

from __future__ import annotations

import shutil
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis, RedisCluster
from redis.asyncio.sentinel import Sentinel
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

CLUSTER_PORT = 7300
MASTER_PORT = 7400
REPLICA_PORT = 7401
SENTINEL_PORT = 27400
MASTER_NAME = "mymaster"
DOCKER = shutil.which("docker") or "docker"

SENTINEL_BOOT = (
    f"redis-server --port {MASTER_PORT} --save '' --appendonly no --daemonize yes && "
    f"redis-server --port {REPLICA_PORT} --save '' --appendonly no "
    f"--replicaof 127.0.0.1 {MASTER_PORT} --daemonize yes && "
    f"printf 'port {SENTINEL_PORT}\\n"
    f"sentinel monitor {MASTER_NAME} 127.0.0.1 {MASTER_PORT} 1\\n"
    "sentinel announce-ip 127.0.0.1\\n"
    f"sentinel down-after-milliseconds {MASTER_NAME} 1000\\n"
    f"sentinel failover-timeout {MASTER_NAME} 5000\\n' > /tmp/s.conf && "
    "redis-sentinel /tmp/s.conf"
)


@dataclass(frozen=True, slots=True)
class Topology:
    """A way to reach Redis, and a name to say which one failed."""

    name: str
    make_client: Any

    def clients(self) -> tuple[Any, Any]:
        return self.make_client(socket_timeout=30.0), self.make_client()


def cluster_container() -> Iterator[Topology]:
    container = (
        DockerContainer("redis:7-alpine")
        .with_command(
            f"redis-server --port {CLUSTER_PORT} --cluster-enabled yes "
            "--cluster-announce-ip 127.0.0.1 --cluster-node-timeout 5000 "
            "--save '' --appendonly no"
        )
        .with_bind_ports(CLUSTER_PORT, CLUSTER_PORT)
    )
    with container:
        wait_for_logs(container, "Ready to accept connections", timeout=60)
        # One node owning every slot still enforces the cross-slot rule, which is
        # the rule the whole key schema exists to satisfy.
        subprocess.run(  # noqa: S603  # fixed argv, no shell
            [
                DOCKER,
                "exec",
                container.get_wrapped_container().id,
                "redis-cli",
                "-p",
                str(CLUSTER_PORT),
                "cluster",
                "addslotsrange",
                "0",
                "16383",
            ],
            check=True,
            capture_output=True,
        )
        wait_for_cluster_ready(container)

        def make(**kwargs: Any) -> Any:
            return RedisCluster.from_url(f"redis://127.0.0.1:{CLUSTER_PORT}", **kwargs)

        yield Topology(name="cluster", make_client=make)


def wait_for_cluster_ready(container: DockerContainer) -> None:
    for _ in range(60):
        state = subprocess.run(  # noqa: S603  # fixed argv, no shell
            [
                DOCKER,
                "exec",
                container.get_wrapped_container().id,
                "redis-cli",
                "-p",
                str(CLUSTER_PORT),
                "cluster",
                "info",
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        if "cluster_state:ok" in state:
            return
    raise RuntimeError("cluster never reached cluster_state:ok")


def new_sentinel() -> Sentinel:
    """A fresh Sentinel client.

    Never share one between tests: its pools bind to the event loop that first
    used them, and the next test runs on a different loop.
    """
    return Sentinel(  # type: ignore[no-untyped-call]  # redis-py leaves this untyped
        [("127.0.0.1", SENTINEL_PORT)], socket_timeout=5.0
    )


def sentinel_container() -> Iterator[Topology]:
    container = (
        DockerContainer("redis:7-alpine")
        .with_command(f'sh -c "{SENTINEL_BOOT}"')
        .with_bind_ports(MASTER_PORT, MASTER_PORT)
        .with_bind_ports(REPLICA_PORT, REPLICA_PORT)
        .with_bind_ports(SENTINEL_PORT, SENTINEL_PORT)
    )
    with container:
        wait_for_logs(container, "monitor master", timeout=60)

        def make(**kwargs: Any) -> Any:
            return new_sentinel().master_for(MASTER_NAME, **kwargs)

        yield Topology(name="sentinel", make_client=make)


def standalone(url: str) -> Topology:
    def make(**kwargs: Any) -> Any:
        return Redis.from_url(url, **kwargs)

    return Topology(name="standalone", make_client=make)
