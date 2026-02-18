"""
AI Spark API - Main Application Entry Point (SQLite)
"""
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from app.api.routes import export_router, video_split_router, import_router
from app.core.config import settings
from app.db.session import init_db, close_db
from app.core.logging_config import logger
import uvicorn


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Lifespan context manager for startup and shutdown events."""

    # -------------------- Startup --------------------
    logger.info(f"Starting AI Spark API : {settings.APP_VERSION}")
    logger.info(f"SQLite DB path : {settings.SQLITE_DB_PATH}")

    try:
        await init_db()
        logger.info("SQLite database initialised and tables created")
        logger.info("")

    except Exception as e:
        logger.error(
            f"Failed to initialize SQLite database | error={str(e)} | "
            f"path={settings.SQLITE_DB_PATH}"
        )
        logger.error(
            "Please check: "
            "1. The SQLITE_DB_PATH directory is writable | "
            "2. SQLITE_DB_PATH in .env is correct"
        )
        raise

    yield

    # -------------------- Shutdown --------------------
    logger.info("Shutting down AI Spark API")
    await close_db()
    logger.info("SQLite engine disposed")


# -------------------- FastAPI app --------------------
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="AI Spark - Metadata packaging and handoff service for Fabric platform",
    docs_url="/docs" if settings.DEBUG else None,
    redoc_url="/redoc" if settings.DEBUG else None,
    lifespan=lifespan,
)

# -------------------- Middleware --------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=settings.CORS_ALLOW_CREDENTIALS,
    allow_methods=settings.CORS_ALLOW_METHODS,
    allow_headers=settings.CORS_ALLOW_HEADERS,
)

app.add_middleware(GZipMiddleware, minimum_size=1000)


# -------------------- Global exception handler --------------------
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(
        f"Unhandled exception | path={request.url.path} | "
        f"method={request.method} | error={str(exc)}"
    )
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": str(exc) if settings.DEBUG else "An unexpected error occurred",
        },
    )


# -------------------- Routers --------------------
app.include_router(export_router, prefix="/spark/export", tags=["Export"])
app.include_router(import_router, prefix="/spark/import", tags=["Import"])
app.include_router(video_split_router, prefix="/spark/video_split", tags=["Video Split"])


# -------------------- Root endpoint --------------------
@app.get("/")
async def root():
    return {
        "name": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "status": "operational",
        "environment": settings.ENVIRONMENT,
        "database": "SQLite",
    }


# -------------------- Run server --------------------
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.RELOAD,
        workers=settings.WORKERS if not settings.RELOAD else 1,
        log_level=settings.LOG_LEVEL.lower(),
    )
