from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_url: str = "redis://localhost:6379"
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_bucket: str = "satellite-files"
    s3_region: str = "us-east-1"
    stl_files_prefix: str = "stl-files"
    packed_files_prefix: str = "packed-files"
    backend_url: str = "http://localhost:3000"

    class Config:
        env_file = ".env"


settings = Settings()
