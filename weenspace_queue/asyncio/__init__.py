from typing import Any

__all__ = ["AwsAsyncEngine", "RabbitMqAsyncEngine"]


def __getattr__(name: str) -> Any:
	try:
		from rabbitmq_amqp_python_client.asyncio import __dict__ as rabbitmq_namespace

		value = rabbitmq_namespace[name]
		globals()[name] = value
		return value
	except (ImportError, KeyError):
		pass
	if name == "AwsAsyncEngine":
		from .aws_async import AwsAsyncEngine

		return AwsAsyncEngine
	if name == "RabbitMqAsyncEngine":
		from .rabbitmq_async import RabbitMqAsyncEngine

		return RabbitMqAsyncEngine
	raise AttributeError(name)
