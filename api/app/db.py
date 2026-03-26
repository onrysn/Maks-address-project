from contextlib import contextmanager
from typing import Iterator
import psycopg

from app.models import settings


@contextmanager
def get_conn() -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(settings.dsn)
    try:
        yield conn
    finally:
        conn.close()
