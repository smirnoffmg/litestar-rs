"""Reading settings off a redis-py client, whatever shape it is.

A standalone client keeps them behind a connection pool and a cluster client
keeps them on itself. Both are accepted: the hash-tagged key schema exists for
cluster mode and would be pointless if a cluster client could not be handed in.
"""

from __future__ import annotations

from typing import Any


def connection_kwarg(client: Any, name: str) -> Any:
    pool = getattr(client, "connection_pool", None)
    if pool is not None:
        kwargs: dict[str, Any] = pool.connection_kwargs
        return kwargs.get(name)
    return getattr(client, "connection_kwargs", {}).get(name)
