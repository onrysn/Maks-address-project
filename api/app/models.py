from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    pg_host: str = os.getenv("POSTGRES_HOST", "db")
    pg_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    pg_db: str = os.getenv("POSTGRES_DB", "maks")
    pg_user: str = os.getenv("POSTGRES_USER", "maks")
    pg_password: str = os.getenv("POSTGRES_PASSWORD", "maks")

    @property
    def dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} "
            f"dbname={self.pg_db} user={self.pg_user} password={self.pg_password}"
        )


settings = Settings()
