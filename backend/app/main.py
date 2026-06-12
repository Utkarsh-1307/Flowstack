import structlog
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator

from app.core.config import get_settings
from app.core.database import engine, Base
from app.services.kafka_producer import get_producer, stop_producer
from app.routers import events, users, websocket

logger = structlog.get_logger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: create tables and warm up Kafka producer connection
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await get_producer()
    logger.info("application_startup_complete", env=settings.app_env)
    yield
    # Shutdown: drain in-flight Kafka messages before exiting
    await stop_producer()
    await engine.dispose()
    logger.info("application_shutdown_complete")


app = FastAPI(
    title="FlowStack API Gateway",
    description="High-throughput async ingestion gateway for the FlowStack ETL platform",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:80"],
    allow_methods=["*"],
    allow_headers=["*"],
)

Instrumentator().instrument(app).expose(app)

app.include_router(events.router, prefix="/api/v1")
app.include_router(users.router, prefix="/api/v1")
app.include_router(websocket.router)


@app.get("/health", tags=["health"])
async def health_check():
    return {"status": "ok", "env": settings.app_env}
