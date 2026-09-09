import os
from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

_base_dir = Path(__file__).resolve().parent
_root_dir = _base_dir.parent

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(str(_root_dir / ".env"), str(_base_dir / ".env")),
        env_file_encoding="utf-8",
        extra="ignore"
    )

    PROJECT_NAME: str = "JudiQ Legal AI"
    VERSION: str = "12.5.0-ENTERPRISE"
    API_V1_STR: str = "/api/v1"
    # SECURITY: Never fall back to a weak default in production.
    SECRET_KEY: str = "changeme_secure_key_for_dev_only"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 8
    DATABASE_URL: str = ""
    BACKEND_CORS_ORIGINS: list = [
        "https://judiq-ai.netlify.app",
        "https://judiq.netlify.app",
        "https://cold-smoke-f63f.judiqai.workers.dev",
        "https://asg492607.github.io",
        "https://cheque-bounce-ragbased.onrender.com",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "http://localhost:8080",
        "http://127.0.0.1:8080",
        "http://localhost:5500",
        "http://127.0.0.1:5500",
        "http://localhost:5501",
        "http://127.0.0.1:5501",
    ]
    ENCRYPTION_KEY: str = "c2VjcmV0X2tleV90aGF0X2lzX2V4YWN0bHlfMzJfYnk="
    DEBUG: bool = False
    GROQ_API_KEY: str = ""

    # Administrator Authentication Configuration (from .env)
    ADMIN_EMAIL: str = "admin@judiq.ai"
    ADMIN_PASSWORD: str = ""
    ADMIN_PASSWORD_HASH: str = ""
    ADMIN_EMAILS: str = "admin@judiq.ai"


@lru_cache()
def get_settings():
    s = Settings()
    if len(s.ENCRYPTION_KEY) not in (43, 44) or " " in s.ENCRYPTION_KEY:
        if not s.DEBUG:
            raise ValueError("ENCRYPTION_KEY must be a valid 32-byte base64 string in production.")
        s.ENCRYPTION_KEY = "c2VjcmV0X2tleV90aGF0X2lzX2V4YWN0bHlfMzJfYnk="
    if not s.DEBUG and s.SECRET_KEY == "changeme_secure_key_for_dev_only":
        import logging
        logging.getLogger("config").warning(
            "CRITICAL SECURITY WARNING: SECRET_KEY is set to its insecure default value in production. "
            "Set a strong, random SECRET_KEY environment variable immediately."
        )
    if not s.DATABASE_URL:
        s.DATABASE_URL = "sqlite:///./analytics.db"
    extra_cors = os.environ.get("CORS_ORIGINS", "")
    if extra_cors:
        origins = [o.strip() for o in extra_cors.split(",") if o.strip()]
        for o in origins:
            if o not in s.BACKEND_CORS_ORIGINS:
                s.BACKEND_CORS_ORIGINS.append(o)
    return s


settings = get_settings()

