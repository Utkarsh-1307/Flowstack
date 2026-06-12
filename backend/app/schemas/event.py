from pydantic import BaseModel, Field
from datetime import datetime, timezone
from uuid import UUID
from typing import Optional


class EventIngestSchema(BaseModel):
    user_id: UUID = Field(..., serialization_alias="userId")
    event_type: str = Field(..., max_length=50, serialization_alias="eventType")
    product_id: Optional[int] = Field(None, serialization_alias="productId")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {
            "example": {
                "userId": "d3b07384-d113-49c6-a5e9-e0887e16ec9b",
                "eventType": "purchase",
                "productId": 550,
                "timestamp": "2026-06-12T10:00:00Z",
            }
        },
    }


class EventIngestResponse(BaseModel):
    status: str
    kafka_offset: Optional[int] = None
    message: str = "Event accepted"
