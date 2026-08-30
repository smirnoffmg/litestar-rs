"""Redis deployments this library claims to work on.

Cluster and Sentinel are alternative high-availability models -- Cluster does its
own failover and does not use Sentinel -- so there is no deployment running both.
What can be shared is the check: the same operations against each of them.

Every container publishes its ports one to one, so the address a node announces
is the address a client on the host can reach. Anything else and Sentinel hands
out an address that only exists inside Docker.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis, RedisCluster
from redis.asyncio.sentinel import Sentinel
from testcontainers.core.container import DockerContainer

CLUSTER_PORTS = (7310, 7311, 7312)
MASTER_PORT = 7400
REPLICA_PORT = 7401
SENTINEL_PORT = 27400
MASTER_NAME = "mymaster"
IMAGE = os.environ.get("REDIS_IMAGE", "redis:7-alpine")
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
    container: DockerContainer | None = None
    """Set for topologies a test needs to reconfigure, such as resharding."""

    def clients(self) -> tuple[Any, Any]:
        return self.make_client(socket_timeout=30.0), self.make_client()


CLUSTER_BOOT = (
    " && ".join(
        f"redis-server --port {port} --cluster-enabled yes "
        f"--cluster-config-file /tmp/n{port}.conf --cluster-announce-ip 127.0.0.1 "
        "--cluster-node-timeout 5000 --save '' --appendonly no --daemonize yes"
        for port in CLUSTER_PORTS
    )
    + " && sleep infinity"
)


def cluster_container() -> Iterator[Topology]:
    """Three nodes, so the slots have different owners.

    A single node owning everything still enforces the cross-slot rule, but it
    never issues a MOVED. Three do, which is the difference between checking the
    key schema and checking that a client can follow the cluster around.
    """
    container = DockerContainer(IMAGE).with_command(f'sh -c "{CLUSTER_BOOT}"')
    for port in CLUSTER_PORTS:
        container = container.with_bind_ports(port, port)
    with container:
        # The servers are daemonized, so their readiness never reaches the
        # container's stdout; ask them instead.
        for port in CLUSTER_PORTS:
            wait_for_ping(container, port)
        nodes = [f"127.0.0.1:{port}" for port in CLUSTER_PORTS]
        run_in(container, ["redis-cli", "--cluster", "create", *nodes, "--cluster-yes"])
        wait_for_cluster_ready(container)

        def make(**kwargs: Any) -> Any:
            return RedisCluster.from_url(
                f"redis://127.0.0.1:{CLUSTER_PORTS[0]}", **kwargs
            )

        yield Topology(name="cluster", make_client=make, container=container)


def run_in(container: DockerContainer, command: list[str]) -> str:
    result = subprocess.run(  # noqa: S603  # fixed argv, no shell
        [DOCKER, "exec", container.get_wrapped_container().id, *command],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def wait_for_ping(container: DockerContainer, port: int) -> None:
    for _ in range(60):
        try:
            if "PONG" in run_in(container, ["redis-cli", "-p", str(port), "ping"]):
                return
        except subprocess.CalledProcessError:
            pass
        time.sleep(0.5)
    raise RuntimeError(f"redis on port {port} never answered")


def wait_for_sentinel_master(container: DockerContainer) -> None:
    for _ in range(60):
        masters = run_in(
            container, ["redis-cli", "-p", str(SENTINEL_PORT), "sentinel", "masters"]
        )
        if MASTER_NAME in masters:
            return
        time.sleep(0.5)
    raise RuntimeError(f"sentinel never picked up {MASTER_NAME}")


def wait_for_cluster_ready(container: DockerContainer) -> None:
    for _ in range(60):
        state = run_in(
            container, ["redis-cli", "-p", str(CLUSTER_PORTS[0]), "cluster", "info"]
        )
        if "cluster_state:ok" in state:
            return
        time.sleep(0.5)
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
        DockerContainer(IMAGE)
        .with_command(f'sh -c "{SENTINEL_BOOT}"')
        .with_bind_ports(MASTER_PORT, MASTER_PORT)
        .with_bind_ports(REPLICA_PORT, REPLICA_PORT)
        .with_bind_ports(SENTINEL_PORT, SENTINEL_PORT)
    )
    with container:
        # Asked rather than read off the logs: the servers are daemonized and a
        # log predicate is deprecated in testcontainers anyway.
        wait_for_ping(container, SENTINEL_PORT)
        wait_for_sentinel_master(container)

        def make(**kwargs: Any) -> Any:
            return new_sentinel().master_for(MASTER_NAME, **kwargs)

        yield Topology(name="sentinel", make_client=make)


def standalone(url: str) -> Topology:
    def make(**kwargs: Any) -> Any:
        return Redis.from_url(url, **kwargs)

    return Topology(name="standalone", make_client=make)


def node_id(container: DockerContainer, port: int) -> str:
    return run_in(container, ["redis-cli", "-p", str(port), "cluster", "myid"]).strip()


def slot_owner(container: DockerContainer, slot: int) -> int:
    """Which of our ports currently serves this slot."""
    nodes = run_in(
        container, ["redis-cli", "-p", str(CLUSTER_PORTS[0]), "cluster", "nodes"]
    )
    for line in nodes.splitlines():
        fields = line.split()
        port = int(fields[1].split("@")[0].rsplit(":", 1)[1])
        for held in fields[8:]:
            start, _, end = held.partition("-")
            if not start.isdigit():
                continue
            if int(start) <= slot <= int(end or start):
                return port
    raise RuntimeError(f"nobody owns slot {slot}")


def move_slot(container: DockerContainer, slot: int) -> tuple[int, int]:
    """Migrate one slot to another node, the way redis-cli reshard does.

    Returns the ports it moved between. Keys travel with the slot, and clients
    are redirected mid-flight -- which is the point of doing this under load.
    """
    source = slot_owner(container, slot)
    target = next(port for port in CLUSTER_PORTS if port != source)
    source_id, target_id = node_id(container, source), node_id(container, target)

    def cli(port: int, *args: str) -> str:
        return run_in(container, ["redis-cli", "-p", str(port), *args])

    cli(target, "cluster", "setslot", str(slot), "importing", source_id)
    cli(source, "cluster", "setslot", str(slot), "migrating", target_id)

    while True:
        keys = cli(source, "cluster", "getkeysinslot", str(slot), "100").split()
        if not keys:
            break
        cli(source, "migrate", "127.0.0.1", str(target), "", "0", "5000", "keys", *keys)

    # Every node must learn the new owner, or some of them keep redirecting back.
    for port in CLUSTER_PORTS:
        cli(port, "cluster", "setslot", str(slot), "node", target_id)
    return source, target
