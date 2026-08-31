# Tasks

A task is an ordinary function. Its parameters are split in two when the
application starts: anything the application provides as a dependency is
injected in the worker, and everything else travels in the payload.

The snippets on this page assume:

```python
from uuid import UUID

from smallage.litestar import TaskRegistry

tasks = TaskRegistry()


class AsyncSession:
    """Stands in for your database session, or whatever you inject."""


class Settings:
    """And for your configuration object."""
```

```python
@tasks.task
async def reindex(doc_id: UUID, session: AsyncSession, settings: Settings) -> None:
    ...
```

With `session` and `settings` registered as Litestar dependencies, only `doc_id`
is serialised. The call site says so:

<!-- docs-test: skip -- enqueue works once the plugin has bound the registry -->
```python
await reindex.enqueue(doc_id=doc_id)
```

There is no context dictionary, and no way to smuggle a dependency through one.

## Arguments

Payload parameters must be annotated — the annotations become a msgspec struct,
and that struct is built at `enqueue`. A wrong argument is therefore the
caller's error rather than a worker's:

<!-- docs-test: skip -- the same, and it is meant to raise -->
```python
await reindex.enqueue(doc_id="not-a-uuid")  # raises at the call site
```

Encoding and decoding go through the application's own serializer, including its
`type_encoders` and `type_decoders`, so a type your route handlers can return is
a type your tasks can take.

Never `pickle`. Payloads come back out of Redis, and unpickling one is remote
code execution the moment Redis is compromised — an import-linter contract keeps
`pickle` out of this package entirely. For the same reason a task name is looked
up in the registry and never imported: an unregistered name is deferred and
eventually buried, not resolved into something importable.

Adding a parameter with a default is safe across a rolling deploy; removing one
is not. Decoding ignores unknown fields on purpose, so a newer producer cannot
break an older worker.

## Dependencies

The application's `Provide` graph is resolved outside a request, which means
`use_cache`, `sync_to_thread` and generator dependencies with teardown all
behave as they do in a route handler. Generators are torn down in reverse, and
on the way out of a failed task as well as a successful one.

A provider that needs something only a request can supply is refused **when the
worker starts**, naming the provider and the parameter:

```
ConfigurationError: provider 'current_user', needed by task 'reindex', takes
'request', which only a request can supply; a task has no request
```

Unknown dependencies and cycles are caught in the same pass.

## Synchronous tasks

A `def` task runs in a thread pool sized by `QueueConfig.thread_limit`, off the
event loop. Blocking the loop does not merely cost throughput: it starves the
liveness refresh, and a worker that is alive but looks dead has its work
reclaimed and done twice.

```python
@tasks.task
def render(report_id: UUID) -> None:
    ...
```

## Timeouts

```python
@tasks.task(timeout_s=30)
async def rebuild(doc_id: UUID) -> None:
    ...
```

A timeout cancels at the next await. It cannot interrupt a CPU loop, a call
inside a C extension, or a thread — so a synchronous task that declares one is
refused at startup rather than quietly ignoring it.

## Queues

```python
@tasks.task(queue="high")
async def urgent_reindex(doc_id: UUID) -> None:
    ...
```

See [Priorities](priorities.md) for how a worker reads several queues.

## Large arguments

Above `QueueConfig.offload_over_bytes` the encoded payload goes to a
`PayloadStore` and the record carries a reference instead. One ships:

<!-- docs-test: skip -- creates the directory it is given -->
```python
from smallage import FilePayloadStore

payloads = FilePayloadStore("/mnt/queue-payloads")
```

Every worker must see the same directory — a network volume, or a single host.
A path local to one pod means the job runs wherever the file happens to be and
fails everywhere else; reading a reference from the wrong place raises
`PayloadMissing` saying so rather than failing obscurely.

Anything else is two methods:

```python
class S3Payloads:
    def __init__(self, client, bucket: str) -> None:
        self.client, self.bucket = client, bucket

    async def put(self, job_id: str, data: bytes) -> str:
        key = f"payloads/{job_id}"
        await self.client.put_object(Bucket=self.bucket, Key=key, Body=data)
        return f"s3://{self.bucket}/{key}"

    async def get(self, reference: str) -> bytes:
        _, _, rest = reference.partition("s3://")
        bucket, _, key = rest.partition("/")
        response = await self.client.get_object(Bucket=bucket, Key=key)
        return await response["Body"].read()
```

Without a store configured, the transport's own limit refuses an oversized
record rather than dropping it silently. Redis keeps the stream in memory; a
payload measured in megabytes is a direct route to an OOM kill.

## Enqueueing inside a transaction

A handler that writes a row and enqueues a job before `COMMIT` has a race it
cannot win: the worker is fast enough to read the row before it exists, and a
rollback leaves a job that has already run. `DeferredEnqueuer` buffers the jobs
and publishes them from a commit hook:

```python
from smallage import DeferredEnqueuer

deferred = DeferredEnqueuer(plugin)
# in the handler: enqueue through `deferred`
# in after_commit: await deferred.flush()
# in rollback:     deferred.discard()
```

This is not a transactional outbox: a crash between the commit and the flush
loses the job. What it removes is the ordering hazard. Work that must survive
that crash needs an outbox in the same database.
