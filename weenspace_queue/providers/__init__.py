from typing import Any

__all__ = ["AwsEngine", "RabbitMqEngine"]


def __getattr__(name: str) -> Any:
	if name == "AwsEngine":
		from .aws import AwsEngine

		return AwsEngine
	if name == "RabbitMqEngine":
		from .rabbitmq import RabbitMqEngine

		return RabbitMqEngine
	raise AttributeError(name)
