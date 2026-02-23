"""
Database Connection Module
Optimized for performance with connection pooling and caching

Supports both Local DuckDB and MotherDuck (cloud) via USE_LOCAL_DB environment variable.
"""
import os
import time as _time
import duckdb
from pathlib import Path
import asyncio
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from functools import lru_cache
import threading

# Configuration: Set USE_LOCAL_DB=true to use local DuckDB, otherwise uses MotherDuck
USE_LOCAL_DB = os.getenv('USE_LOCAL_DB', 'true').lower() in ('true', '1', 'yes')

# Local database path
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'ai_driven_data.duckdb')

# MotherDuck configuration (only needed if USE_LOCAL_DB is False)
MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN') or os.getenv('MOTHERDUCK_PAT')
MOTHERDUCK_DB = 'ai_driven_data'

# Validate configuration
if not USE_LOCAL_DB and not MOTHERDUCK_TOKEN:
    raise ValueError("MOTHERDUCK_TOKEN environment variable is required when USE_LOCAL_DB is not set. Set it in your .env file or set USE_LOCAL_DB=true for local database.")

# Global connection pool (thread-safe)
_connection_pool = {}
_pool_lock = threading.Lock()
_last_active: dict = {}          # thread_id -> monotonic timestamp of last use
_IDLE_CHECK_SECS = 45 * 60      # only health-ping after 45 min of idle


def get_db_connection(read_only=True):
    """
    Get database connection with connection pooling.
    Uses local DuckDB or MotherDuck based on USE_LOCAL_DB environment variable.
    Reuses connections to avoid overhead of creating new connections on every request.
    The SELECT 1 health-check only runs after 45 min of idle to avoid an extra
    MotherDuck round-trip on every single request.
    """
    thread_id = threading.get_ident()
    now = _time.monotonic()

    with _pool_lock:
        if thread_id in _connection_pool:
            conn = _connection_pool[thread_id]
            idle = now - _last_active.get(thread_id, 0)
            if idle < _IDLE_CHECK_SECS:
                # Connection recently used — skip health-check, return immediately
                _last_active[thread_id] = now
                return conn
            # Idle a long time — verify still alive before returning
            try:
                conn.execute("SELECT 1")
                _last_active[thread_id] = now
                return conn
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                del _connection_pool[thread_id]
                _last_active.pop(thread_id, None)

    # Create new connection
    try:
        if USE_LOCAL_DB:
            # Local DuckDB connection
            conn = duckdb.connect(DB_PATH, read_only=False)
            print(f"📁 Connected to local DuckDB: {DB_PATH}")
        else:
            # MotherDuck cloud connection
            conn = duckdb.connect(f'md:?motherduck_token={MOTHERDUCK_TOKEN}')
            conn.execute(f"CREATE DATABASE IF NOT EXISTS {MOTHERDUCK_DB}")
            conn.execute(f"USE {MOTHERDUCK_DB}")
            print(f"☁️ Connected to MotherDuck: {MOTHERDUCK_DB}")

        with _pool_lock:
            _connection_pool[thread_id] = conn
            _last_active[thread_id] = now

        return conn
    except Exception as e:
        db_type = "Local DuckDB" if USE_LOCAL_DB else "MotherDuck"
        raise Exception(f"{db_type} connection failed: {str(e)}")

def close_all_connections():
    """Close all connections in the pool (useful for cleanup)"""
    with _pool_lock:
        for conn in _connection_pool.values():
            try:
                conn.close()
            except:
                pass
        _connection_pool.clear()

def _test_connection_sync():
    """Synchronous database connection test"""
    try:
        conn = get_db_connection()
        # Test query
        result = conn.execute("SELECT 1 as test").fetchone()
        return result is not None
    except Exception as e:
        return False

def test_connection(timeout=5):
    """Test database connection with timeout"""
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_test_connection_sync)
            result = future.result(timeout=timeout)
            return result
    except FutureTimeoutError:
        return False
    except Exception as e:
        return False

def get_database_info():
    """Get information about the current database configuration"""
    return {
        'type': 'Local DuckDB' if USE_LOCAL_DB else 'MotherDuck (Cloud)',
        'path': DB_PATH if USE_LOCAL_DB else f'md:{MOTHERDUCK_DB}',
        'use_local': USE_LOCAL_DB
    }
