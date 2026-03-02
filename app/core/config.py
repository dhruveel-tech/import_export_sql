"""
Application Configuration
"""
from typing import List
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application Settings
    APP_NAME: str = "AI Spark API"
    APP_VERSION: str = "1.0.0"
    ENVIRONMENT: str = "development"
    DEBUG: bool = True
    LOG_LEVEL: str = "INFO"

    # Server Settings
    HOST: str = "192.168.0.146"
    PORT: int = 5000
    WORKERS: int = 4
    RELOAD: bool = False

    # MongoDB Settings
    MONGODB_URL: str = "mongodb://192.168.0.50:27017/"
    MONGODB_DB_NAME: str = "ApiDNA"
    MONGODB_MIN_POOL_SIZE: int = 10
    MONGODB_MAX_POOL_SIZE: int = 50
    MONGODB_COLLECTION_NAME_FOR_GET_DATA: str = "catalogAIEnrichedTimelineEvents"
    
    # SQLite Settings
    SQLITE_DB_PATH: str = "ai_spark.db"

    # Storage Settings
    EXPORT_BASE_PATH: str = "D:\\SDNA\\AI_Spark\\sdna_task\\exports"
    EXPORT_VIDEO_SPIT_PATH: str = "D:\\SDNA\\AI_Spark\\sdna_task\\videos"
    IMPORT_BASE_PATH: str = "D:\\SDNA\\AI_Spark\\sdna_task\\imports"
    MAX_UPLOAD_SIZE: int = 5_368_709_120  # 5GB
    ALLOWED_IMPORT_FORMATS: str = "json,csv"

    # EngineX / Export Server Settings
    ENGINEX_BASE_URL: str ="http://192.168.0.146:4000"
    EXPORT_URL_PREFIX : str ="https://enginex.example.com/exports"

    # Fabric Integration Settings
    FABRIC_API_URL: str = "http://192.168.0.50:4080"
    FABRIC_API_KEY: str = "$2b$10$JGCRHRqAqk6n"
    FABRIC_API_TIMEOUT: int = 30

    # CORS Settings
    CORS_ORIGINS: List[str] = ["http://localhost:3000", "http://localhost:8000"]
    CORS_ALLOW_CREDENTIALS: bool = True
    CORS_ALLOW_METHODS: List[str] = ["*"]
    CORS_ALLOW_HEADERS: List[str] = ["*"]

    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def parse_cors_origins(cls, v):
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",")]
        return v

    @property
    def allowed_import_formats_list(self) -> List[str]:
        """Get allowed import formats as a list."""
        return [fmt.strip() for fmt in self.ALLOWED_IMPORT_FORMATS.split(",")]

    @property
    def database_url(self) -> str:
        return f"sqlite+aiosqlite:///{self.SQLITE_DB_PATH}"


# Global settings instance
settings = Settings()
