"""
SQLite Database Configuration with SQLAlchemy async
"""
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from app.core.config import settings


class Base(DeclarativeBase):
    pass


# Create async engine
engine = create_async_engine(
    settings.database_url,
    echo=settings.DEBUG,
    connect_args={"check_same_thread": False},
)

# Session factory
AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_db() -> AsyncSession:
    """Dependency: yield an async DB session."""
    async with AsyncSessionLocal() as session:
        yield session


async def init_db():
    """Create all tables on startup."""
    from app.models.models import Base as ModelBase  # noqa: F401 - ensures models registered
    async with engine.begin() as conn:
        await conn.run_sync(ModelBase.metadata.create_all)


async def close_db():
    """Dispose engine on shutdown."""
    await engine.dispose()
