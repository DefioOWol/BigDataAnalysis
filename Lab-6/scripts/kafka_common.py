"""Общие элементы Kafka."""

import json

from kafka import KafkaConsumer, KafkaProducer

from domain import BOOTSTRAP_SERVER, CONTROL_END


def is_control_end(payload: object) -> bool:
    """Проверить, является ли payload служебным сообщением конца батча."""
    if not isinstance(payload, dict):
        return False
    control_end_key = tuple(CONTROL_END.keys())[0]
    return payload.get(control_end_key) == CONTROL_END[control_end_key]


def json_producer() -> KafkaProducer:
    """Создать producer для JSON-сообщений."""
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVER,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def json_consumer(topic: str, group_id: str) -> KafkaConsumer:
    """Создать consumer для JSON-сообщений."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVER,
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
