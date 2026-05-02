"""
Database Connection Module
PostgreSQL/Supabase via psycopg2.

Exposes a DuckDB-compatible API so football.py needs minimal changes:
  - conn.execute(sql, params) returns an object with .fetchone()/.fetchall()
  - ? placeholders are auto-converted to %s
  - FOOTBALL. schema prefix is auto-stripped
"""
import os
import time as _time
import threading
import psycopg2

# Set SUPABASE_DB_URL (or DATABASE_URL) to the Supabase direct connection string:
# postgresql://postgres:[password]@db.<project>.supabase.co:5432/postgres
DATABASE_URL: str | None = os.getenv('SUPABASE_DB_URL') or os.getenv('DATABASE_URL')

_IDLE_CHECK_SECS = 45 * 60
_thread_local = threading.local()


class _Result:
    """Thin wrapper around a psycopg2 cursor matching DuckDB's result API."""
    __slots__ = ('_cur',)

    def __init__(self, cursor):
        self._cur = cursor

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    def __iter__(self):
        return iter(self._cur)


class _Conn:
    """Wraps a psycopg2 connection with a DuckDB-like execute() interface."""

    def __init__(self, pg_conn):
        self._conn = pg_conn

    def execute(self, sql: str, params=None):
        # Convert DuckDB-style ? placeholders to psycopg2 %s
        sql = sql.replace('?', '%s')
        # Strip the FOOTBALL. schema prefix used in the original DuckDB code
        sql = sql.replace('FOOTBALL.', '')
        cur = self._conn.cursor()
        cur.execute(sql, params)
        return _Result(cur)

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass


def _make_conn() -> _Conn:
    if not DATABASE_URL:
        raise ValueError(
            "SUPABASE_DB_URL or DATABASE_URL environment variable is required. "
            "Set it in your .env file or in Render's environment settings."
        )
    pg = psycopg2.connect(DATABASE_URL)
    pg.autocommit = True
    return _Conn(pg)


def get_db_connection(read_only: bool = False) -> _Conn:
    """Return a per-thread psycopg2 connection. Health-checks after 45 min idle."""
    now = _time.monotonic()
    conn: _Conn | None = getattr(_thread_local, 'conn', None)
    last_used: float = getattr(_thread_local, 'last_used', 0.0)

    if conn is not None:
        if now - last_used < _IDLE_CHECK_SECS:
            _thread_local.last_used = now
            return conn
        # After long idle, verify the connection is still alive
        try:
            conn._conn.cursor().execute('SELECT 1')
            _thread_local.last_used = now
            return conn
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            _thread_local.conn = None

    _thread_local.conn = _make_conn()
    _thread_local.last_used = now
    return _thread_local.conn


def close_all_connections():
    """Close this thread's DB connection (called on app shutdown)."""
    conn: _Conn | None = getattr(_thread_local, 'conn', None)
    if conn:
        conn.close()
        _thread_local.conn = None


def test_connection(timeout: int = 5) -> bool:
    try:
        get_db_connection().execute('SELECT 1').fetchone()
        return True
    except Exception:
        return False


def get_database_info() -> dict:
    url = DATABASE_URL or ''
    safe = url[:url.find('@') + 1] + '...' if '@' in url else url[:40] + '...'
    return {'type': 'Supabase (PostgreSQL)', 'url': safe}
