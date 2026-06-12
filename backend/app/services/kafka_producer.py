import json
import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from app.core.config import get_settings

logger = structlog.get_logger(__name__)
settings = get_settings()

_producer: AIOKafkaProducer | None = None


async def get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",           # wait for full ISR acknowledgement
            compression_type="gzip",
            max_batch_size=32768,
            linger_ms=5,          # micro-batching for throughput
        )
        await _producer.start()
        logger.info("kafka_producer_started", servers=settings.kafka_bootstrap_servers)
    return _producer


async def stop_producer() -> None:
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None
        logger.info("kafka_producer_stopped")


async def produce_event(payload: dict, topic: str | None = None) -> int | None:
    """Produce a message; on serialization/schema failure route to DLQ."""
    target_topic = topic or settings.kafka_topic_user_events
    producer = await get_producer()
    try:
        record_meta = await producer.send_and_wait(target_topic, value=payload)
        logger.info(
            "kafka_event_produced",
            topic=target_topic,
            partition=record_meta.partition,
            offset=record_meta.offset,
        )
        return record_meta.offset
    except KafkaError as exc:
        logger.error("kafka_produce_failed", error=str(exc), payload=payload)
        # Route to Dead Letter Queue so the consumer never blocks
        try:
            await producer.send_and_wait(settings.kafka_topic_dlq, value={"error": str(exc), "original": payload})
        except KafkaError:
            pass
        return None
