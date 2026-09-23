"""Direct AWS SQS example using QueueClient."""

from weenspace_queue import Message, QueueClient, QueueSpecification


def main(region_name: str = "us-east-1") -> None:
    with QueueClient("AWS", region_name=region_name) as client:
        queue_url = client.declare_queue(QueueSpecification(name="ween-sqs-demo"))
        result = client.publish(queue_url, Message(body=b"direct SQS message"))
        print(f"SQS published: {result.get('MessageId')}")

        def handle(message: Message) -> None:
            print(f"SQS received: {message.body.decode()}")
            if message.accept:
                message.accept()

        # Uncomment to receive messages:
        # client.consume(queue_url, handle, prefetch=10)


if __name__ == "__main__":
    main()