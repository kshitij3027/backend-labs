import os
from dataclasses import dataclass


@dataclass
class Config:
    chunk_size: int = 67_108_864  # 64 MB
    num_workers: int = os.cpu_count() or 4
    output_dir: str = "results"
    intermediate_dir: str = "intermediate"
    upload_dir: str = "uploads"
    host: str = "0.0.0.0"
    port: int = 8080
    log_level: str = os.environ.get("LOG_LEVEL", "INFO")

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
            port=int(os.environ.get("PORT", "8080")),
        )
