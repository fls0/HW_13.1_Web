
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):

    DB_URL: str = "postgresql+asyncpg://postgres:1231@localhost:5432/adress_book"

    SECRET_KEY_JWT: str = "secret_key_jwt"
    ALGORITHM_JWT: str = "HS256"

    MAIL_USERNAME: str = "example@meta.ua"
    MAIL_PASSWORD: str = "secretPassword"
    MAIL_FROM: str = "example@meta.ua"
    MAIL_PORT: int = 1231
    MAIL_SERVER: str = "smtp.meta.ua"

    REDIS_DOMAIN: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: str | None = None

    CLOUDINARY_NAME: str = 'dfxylfr01'
    CLOUDINARY_API_KEY: int = 323671115669631
    CLOUDINARY_API_SECRET: str = "gszLpE5Io8FYYikFtQonhSWvp0w"

    model_config = SettingsConfigDict(env_file='.env', extra='ignore', env_file_encoding='utf-8')



config = Settings()