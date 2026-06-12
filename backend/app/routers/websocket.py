from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from app.services.websocket_manager import manager
import structlog

logger = structlog.get_logger(__name__)
router = APIRouter(tags=["websocket"])


@router.websocket("/ws/metrics")
async def metrics_websocket(websocket: WebSocket):
    """
    Persistent WebSocket endpoint. The dashboard connects here and receives
    real-time event notifications as they are produced to Kafka.
    """
    await manager.connect(websocket)
    try:
        while True:
            # Keep the connection alive; client can send pings
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
