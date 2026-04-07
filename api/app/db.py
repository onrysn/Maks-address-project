from contextlib import contextmanager
import threading
from typing import Iterator

import psycopg

from app.models import settings

_THREAD_LOCAL = threading.local()


def _get_or_create_thread_conn() -> psycopg.Connection:
    conn = getattr(_THREAD_LOCAL, "conn", None)
    if conn is None or conn.closed:
        conn = psycopg.connect(settings.dsn)
        _THREAD_LOCAL.conn = conn
    return conn


@contextmanager
def get_conn() -> Iterator[psycopg.Connection]:
    conn = _get_or_create_thread_conn()
    try:
        yield conn
    except Exception:
        # Keep the thread connection healthy after statement-level failures.
        if conn and not conn.closed:
            conn.rollback()
        raise
