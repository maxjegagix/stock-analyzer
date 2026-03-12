import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return value


class Settings:
    def __init__(self):
        self.APP_NAME = _require_env("APP_NAME")
        self.APP_ENV = _require_env("APP_ENV")

        self.DB_HOST = _require_env("DB_HOST")
        self.DB_PORT = int(_require_env("DB_PORT"))
        self.DB_USER = _require_env("DB_USER")
        self.DB_PASSWORD = _require_env("DB_PASSWORD")
        self.DB_NAME = _require_env("DB_NAME")

        self.REDIS_HOST = _require_env("REDIS_HOST")
        self.REDIS_PORT = int(_require_env("REDIS_PORT"))

    @property
    def DATABASE_URL(self):
        return (
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )


settings = Settings()
