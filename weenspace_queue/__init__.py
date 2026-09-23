from typing import Any, Callable, Type
from .base import (
    AsyncQueueEngine,
    Message,
    QueueEngine,
    QueueSpecification,
    TopicSpecification,
)
from .constants import (
    PROVIDER_AWS,
    PROVIDER_RABBITMQ,
    ExchangeKind,
    Provider,
    QueueKind,
)


def __getattr__(name: str) -> Any:
    """Lazily expose the low-level RabbitMQ compatibility API."""
    try:
        from rabbitmq_amqp_python_client import __dict__ as rabbitmq_namespace

        value = rabbitmq_namespace.get(name)
        if value is None and name == "DirectReplyToConsumerOptions":
            value = rabbitmq_namespace["ConsumerOptions"]
        if value is None:
            raise KeyError(name)
    except (ImportError, KeyError) as exc:
        raise AttributeError(name) from exc
    globals()[name] = value
    return value


def _engine_class(provider: str) -> Type[QueueEngine]:
    if provider == PROVIDER_AWS:
        from .providers.aws import AwsEngine

        return AwsEngine
    if provider == PROVIDER_RABBITMQ:
        from .providers.rabbitmq import RabbitMqEngine

        return RabbitMqEngine
    raise ValueError(f"Unsupported provider '{provider}'")


def _async_engine_class(provider: str) -> Type[AsyncQueueEngine]:
    if provider == PROVIDER_AWS:
        from .asyncio.aws_async import AwsAsyncEngine

        return AwsAsyncEngine
    if provider == PROVIDER_RABBITMQ:
        from .asyncio.rabbitmq_async import RabbitMqAsyncEngine

        return RabbitMqAsyncEngine
    raise ValueError(f"Unsupported provider '{provider}'")


class QueueClient:
    """Single client: pass provider name, then use the same publish/consume/topology methods."""

    def __init__(self, provider: str, **config: Any) -> None:
        prov_key = provider.strip().upper()
        engine_cls = _engine_class(prov_key)
        self.provider = prov_key
        self.engine: QueueEngine = engine_cls(**config)

    def declare_queue(self, spec: QueueSpecification) -> str:
        return self.engine.declare_queue(spec)

    def declare_topic(self, spec: TopicSpecification) -> str:
        return self.engine.declare_topic(spec)

    def bind_pattern(self, queue_id: str, topic_id: str, pattern: str) -> None:
        self.engine.bind_pattern(queue_id, topic_id, pattern)

    def publish(self, destination: str, message: Message) -> Any:
        return self.engine.publish(destination, message)

    def consume(
        self,
        queue_id: str,
        handler: Callable[[Message], None],
        *,
        prefetch: int | None = None,
    ) -> None:
        return self.engine.consume(queue_id, handler, prefetch=prefetch)

    def stop(self) -> None:
        self.engine.stop()

    def close(self) -> None:
        self.engine.close()

    def __enter__(self) -> "QueueClient":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()


class AsyncQueueClient:
    """Async twin of QueueClient. Same method names, same provider argument."""

    def __init__(self, provider: str, **config: Any) -> None:
        prov_key = provider.strip().upper()
        engine_cls = _async_engine_class(prov_key)
        self.provider = prov_key
        self.engine: AsyncQueueEngine = engine_cls(**config)

    async def declare_queue(self, spec: QueueSpecification) -> str:
        return await self.engine.declare_queue(spec)

    async def declare_topic(self, spec: TopicSpecification) -> str:
        return await self.engine.declare_topic(spec)

    async def bind_pattern(self, queue_id: str, topic_id: str, pattern: str) -> None:
        await self.engine.bind_pattern(queue_id, topic_id, pattern)

    async def publish(self, destination: str, message: Message) -> Any:
        return await self.engine.publish(destination, message)

    async def consume(
        self,
        queue_id: str,
        handler: Callable[[Message], None],
        *,
        prefetch: int | None = None,
    ) -> None:
        await self.engine.consume(queue_id, handler, prefetch=prefetch)

    async def stop(self) -> None:
        await self.engine.stop()

    async def close(self) -> None:
        await self.engine.close()

    async def __aenter__(self) -> "AsyncQueueClient":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()


__all__ = [
    "AsyncQueueClient",
    "AsyncQueueEngine",
    "ExchangeKind",
    "Message",
    "Provider",
    "QueueClient",
    "QueueEngine",
    "QueueKind",
    "QueueSpecification",
    "TopicSpecification",
]
