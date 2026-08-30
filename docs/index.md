# litestar-rs

Распределённая очередь задач на Redis Streams с first-class интеграцией в Litestar.

Гарантия доставки — **at-least-once**. Хендлеры обязаны быть идемпотентными;
для этого есть dedup-ключ, проверяемый непосредственно перед вызовом.

## Установка

```bash
uv add "litestar-rs[litestar]"
```

## Задача

Параметры задачи делятся на две части при старте приложения: то, что приложение
объявило зависимостью, инжектится в воркере; остальное едет в payload. Никакого
словаря `ctx`.

```python
from uuid import UUID
from litestar import Litestar, post
from litestar.di import Provide
from litestar_rs.plugin import QueueConfig, QueuePlugin, TaskRegistry

tasks = TaskRegistry()


@tasks.task
async def reindex(doc_id: UUID, session: AsyncSession, settings: Settings) -> None:
    ...


@post("/documents")
async def create(doc_id: UUID) -> str:
    await reindex.enqueue(doc_id=doc_id)
    return "queued"


app = Litestar(
    route_handlers=[create],
    dependencies={"session": Provide(session), "settings": Provide(settings)},
    plugins=[QueuePlugin(QueueConfig(registry=tasks, redis_url="redis://localhost"))],
)
```

Воркер — та же команда, что и приложение:

```bash
litestar workers run --queue high --concurrency 20
```

## Что проверяется на старте, а не в рантайме

- задача зарегистрирована дважды;
- аргумент payload без аннотации — его нечем валидировать;
- зависимость, которой приложение не предоставляет;
- цикл в графе зависимостей;
- провайдер, которому нужен `request` или заголовки: у задачи запроса нет.

## Слои

- **ядро** (`litestar_rs.core`) — транспорт, воркер, планировщик, ретраи.
  Зависимости: `redis`, `msgspec`, `anyio`. Импорт Litestar запрещён и
  проверяется import-linter.
- **плагин** (`litestar_rs.plugin`) — DI, CLI, сериализация, health, трассировка.

Ядро пригодно к использованию без Litestar.

## Тестирование приложения

```python
from litestar_rs import CollectingEnqueuer, EagerEnqueuer, worker_running
```

- `CollectingEnqueuer` — ничего не исполняет, даёт `assert_enqueued`;
- `EagerEnqueuer` — исполняет задачу тут же, для юнит-тестов прикладного кода;
- `worker_running` — настоящий воркер на время блока, с дренажом на выходе.

## Документация

Публичный API — в [API Reference](api.md).
