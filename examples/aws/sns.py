"""AWS SNS-to-SQS example using a routing-key filter."""

from weenspace_queue import Message, QueueClient, QueueSpecification, TopicSpecification


def main(region_name: str = "us-east-1") -> None:
    with QueueClient("AWS", region_name=region_name) as client:
        topic_arn = client.declare_topic(TopicSpecification(name="ween-sns-demo"))
        queue_url = client.declare_queue(
            QueueSpecification(name="ween-sns-sqs-demo")
        )

        # SNS publishes only matching routing keys to this SQS subscription.
        client.bind_pattern(queue_url, topic_arn, "orders.*")
        result = client.publish(
            topic_arn,
            Message(body=b"order created", routing_key="orders.created"),
        )
        print(f"SNS published: {result.get('MessageId')}")

        def handle(message: Message) -> None:
            print(f"SNS message received through SQS: {message.body.decode()}")
            if message.accept:
                message.accept()

        # Uncomment to receive the SNS notification from SQS:
        # client.consume(queue_url, handle, prefetch=10)


if __name__ == "__main__":
    main()