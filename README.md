# WeenSpace RabbitMQ Client

[![PyPI version](https://badge.fury.io/py/weenspace-queue.svg)](https://pypi.org/project/weenspace-queue/)
[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A powerful Python RabbitMQ client for AMQP 1.0 protocol, designed for event-driven microservices architecture.

## ✨ Features

- 🔐 **Authentication**: OAuth2, TLS/SSL, Basic Auth
- 📬 **Queue Types**: Classic, Quorum, Stream queues
- ☠️ **Dead Letter Queues**: Built-in DLQ support with configurable strategies
- ⚡ **Priority Queues**: Native priority queue support
- 🔄 **Auto Reconnection**: Configurable recovery with exponential backoff
- 🔀 **All Exchange Types**: Direct, Fanout, Topic, Headers
- 🚀 **Async Support**: Native asyncio integration
- 📊 **Streams**: RabbitMQ Streams with filtering support

## 📦 Installation

```bash
pip install weenspace-queue
```

## 🚀 Quick Start

### Unified Publisher/Consumer

```python
from weenspace_queue import Message, QueueClient, QueueSpecification

client = QueueClient("RABBITMQ", uri="amqp://guest:guest@localhost:5672/")
queue = client.declare_queue(QueueSpecification(name="my-queue"))
client.publish(queue, Message(body=b"Hello WeenSpace!"))

def on_message(message: Message) -> None:
    print(f"Received: {message.body}")
    message.accept()

client.consume(queue, handler=on_message, prefetch=10)
```

Install AWS support with `pip install "weenspace-queue[aws]"`.

### AWS SQS and SNS

See [examples/aws/sqs.py](https://github.com/WeenSpace/weenspace-queue/blob/main/examples/aws/sqs.py) for direct SQS publishing and [examples/aws/sns.py](https://github.com/WeenSpace/weenspace-queue/blob/main/examples/aws/sns.py) for an SNS-to-SQS workflow with routing-key filters.

### Async Support

```python
import asyncio
from weenspace_queue import AsyncQueueClient, Message, QueueSpecification

async def main():
    async with AsyncQueueClient("RABBITMQ", uri="amqp://localhost:5672/") as client:
        queue = await client.declare_queue(QueueSpecification(name="my-queue"))
        await client.publish(queue, Message(body=b"Async message!"))

asyncio.run(main())
```

### RabbitMQ authentication

```python
from weenspace_queue import QueueClient

client = QueueClient(
    "RABBITMQ",
    uri="amqp://localhost:5672/",
    oauth2_options=your_oauth_options
)
```

### TLS/SSL Connection

```python
from weenspace_queue import QueueClient

client = QueueClient(
    "RABBITMQ",
    uri="amqps://localhost:5671/",
    ssl_context=your_ssl_context
)
```

## 📚 Documentation

See the [examples folder](https://github.com/WeenSpace/weenspace-queue/tree/main/examples) for more detailed usage examples.

## 🔄 Migration from python-rabbitmq

```python
# Old (python-rabbitmq)
# from python_rabbitmq import RabbitMQ

# New (weenspace-queue)
from weenspace_queue import QueueClient
```

## 📋 Requirements

- Python 3.13+
- RabbitMQ 4.x with AMQP 1.0 plugin enabled

## 📄 License

MIT License - Based on the official [RabbitMQ AMQP Python Client](https://github.com/rabbitmq/rabbitmq-amqp-python-client)

## 🙏 Credits

This library is based on the official RabbitMQ AMQP 1.0 Python client by the RabbitMQ team.
