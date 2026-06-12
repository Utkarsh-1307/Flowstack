from fastapi import APIRouter, HTTPException, status
from app.schemas.event import EventIngestSchema, EventIngestResponse
from app.services.kafka_producer import produce_event
from app.services.websocket_manager import manager
import structlog

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/events", tags=["events"])


@router.post("/ingest", response_model=EventIngestResponse, status_code=status.HTTP_202_ACCEPTED)
async def ingest_event(event: EventIngestSchema):
    """
    Accepts a validated event, produces it to Kafka, and broadcasts
    a lightweight notification over the WebSocket bus.
    Returns 202 Accepted — the event is durable once Kafka acks it.
    """
    payload = event.model_dump(by_alias=True, mode="json")
    offset = await produce_event(payload)

    # Push live notification to dashboard subscribers without blocking the response
    await manager.broadcast({"type": "new_event", "eventType": event.event_type, "offset": offset})

    if offset is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kafka unavailable — event routed to DLQ",
        )

    return EventIngestResponse(status="accepted", kafka_offset=offset)
