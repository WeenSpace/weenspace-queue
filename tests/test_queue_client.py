from types import SimpleNamespace

from weenspace_queue import Message, QueueClient
from weenspace_queue.providers.aws import AwsEngine
from weenspace_queue.utils import (
    inject_aws_routing_attrs,
    rabbitmq_exchange_address,
    rabbitmq_pattern_matches,
    rabbitmq_queue_address,
    rmq_pattern_to_aws_sns,
    sns_filter_matches,
    unwrap_sqs_body,
)


CASES = [
    ("order.placed", "order.placed", True),
    ("order.placed", "order.shipped", False),
    ("order.*.completed", "order.eu.completed", True),
    ("order.*.completed", "order.eu.uk.completed", False),
    ("global.orders.#", "global.orders", True),
    ("global.orders.#", "global.orders.eu.completed", True),
    ("global.orders.#", "global.other.eu", False),
    ("eu.*.truck.#", "eu.uk.truck.delayed", True),
    ("eu.*.truck.#", "eu.uk.car.delayed", False),
    ("#.delayed", "eu.uk.truck.delayed", True),
    ("#.delayed", "delayed", True),
    ("#.delayed", "eu.uk.truck.delivered", False),
    ("region.#.failed", "region.east.failed", True),
    ("region.#.failed", "region.failed", True),
    ("region.#.failed", "other.east.failed", False),
    ("#", "anything.at.all", True),
    ("user.click.*", "user.click.cart", True),
    ("user.click.*", "user.click.cart.extra", False),
]


def test_rabbitmq_and_aws_wildcard_cases_agree() -> None:
    for pattern, routing_key, expected in CASES:
        rmq = rabbitmq_pattern_matches(pattern, routing_key)
        aws = sns_filter_matches(rmq_pattern_to_aws_sns(pattern), routing_key)
        assert rmq is expected, f"rmq {pattern} vs {routing_key}"
        assert aws is expected, f"aws {pattern} vs {routing_key}"


def test_inject_routing_attributes() -> None:
    attrs = inject_aws_routing_attrs("eu.uk.truck.delayed")
    assert attrs["routing_key"]["StringValue"] == "eu.uk.truck.delayed"
    assert attrs["rk_len"]["StringValue"] == "4"
    assert attrs["rk_idx_0"]["StringValue"] == "eu"
    assert attrs["rk_ridx_0"]["StringValue"] == "delayed"


def test_unwrap_sns_envelope() -> None:
    raw = (
        '{"Type":"Notification","Message":"hello",'
        '"MessageAttributes":{"routing_key":{"Value":"order.placed"}}}'
    )
    body, routing_key, extra = unwrap_sqs_body(raw)
    assert body == b"hello"
    assert routing_key == "order.placed"
    assert extra["sns"]["Type"] == "Notification"


def test_rabbitmq_address_helpers() -> None:
    assert rabbitmq_queue_address("orders") == "/queues/orders"
    assert rabbitmq_queue_address("/queues/orders") == "/queues/orders"
    assert (
        rabbitmq_exchange_address("OrdersExchange", "order.placed")
        == "/exchanges/OrdersExchange/order.placed"
    )


def test_queue_client_rejects_unknown_provider() -> None:
    try:
        QueueClient(provider="kafka")
    except ValueError as exc:
        assert "Unsupported provider" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_aws_sns_publish_uses_topic_arn() -> None:
    engine = AwsEngine.__new__(AwsEngine)
    engine.sns = SimpleNamespace(publish=lambda **kwargs: kwargs)

    result = engine.publish(
        "arn:aws:sns:us-east-1:123456789012:click-events",
        Message(body=b"click", routing_key="user.click.cart"),
    )

    assert result["TopicArn"].endswith(":click-events")
    assert result["MessageAttributes"]["routing_key"]["StringValue"] == (
        "user.click.cart"
    )


def test_aws_sns_subscription_uses_topic_arn() -> None:
    calls = []
    engine = AwsEngine.__new__(AwsEngine)
    engine.sns = SimpleNamespace(
        subscribe=lambda **kwargs: calls.append(("subscribe", kwargs))
    )
    engine.sqs = SimpleNamespace(
        get_queue_attributes=lambda **kwargs: {
            "Attributes": {"QueueArn": "arn:aws:sqs:us-east-1:123:events"}
        },
        set_queue_attributes=lambda **kwargs: calls.append(("policy", kwargs)),
    )

    engine.bind_pattern(
        "https://sqs.us-east-1.amazonaws.com/123/events",
        "arn:aws:sns:us-east-1:123:click-events",
        "user.click.*",
    )

    assert calls[0][1]["TopicArn"].endswith(":click-events")
    assert calls[0][1]["Attributes"]["FilterPolicyScope"] == "MessageAttributes"
