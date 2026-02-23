"""
Eko Football API - FastAPI backend for Eko React app only.
Run: uvicorn main_eko:app --host 0.0.0.0 --port 8000
"""
import os
from pathlib import Path
from datetime import date

from dotenv import load_dotenv
load_dotenv()

from core import database as _core_db
os.environ.setdefault("DUCKDB_PATH", str(Path(_core_db.DB_PATH).resolve()))
os.environ["USE_SHARED_DB_CONNECTION"] = "1"

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from datetime import datetime

from api.routes import football
from core.database import get_db_connection, close_all_connections, get_database_info, USE_LOCAL_DB


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: ensure FOOTBALL schema and tables exist. Shutdown: close DB."""
    print("Starting Eko Football API...")
    try:
        conn = get_db_connection()
        conn.execute("SELECT 1")
        conn.execute("CREATE SCHEMA IF NOT EXISTS FOOTBALL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS FOOTBALL.players (
                id INTEGER PRIMARY KEY,
                first_name VARCHAR NOT NULL,
                surname VARCHAR NOT NULL,
                baller_name VARCHAR NOT NULL UNIQUE,
                jersey_number INTEGER NOT NULL CHECK (jersey_number >= 1 AND jersey_number <= 100),
                email VARCHAR NOT NULL,
                whatsapp_phone VARCHAR NOT NULL,
                status VARCHAR NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
                password_hash VARCHAR,
                password_display VARCHAR,
                year_registered INTEGER,
                created_at TIMESTAMP DEFAULT current_timestamp,
                approved_at TIMESTAMP,
                suspended BOOLEAN DEFAULT false
            )
        """)
        for sql in [
            "ALTER TABLE FOOTBALL.players ADD COLUMN suspended BOOLEAN DEFAULT false",
            "ALTER TABLE FOOTBALL.players ADD COLUMN password_display VARCHAR",
        ]:
            try:
                conn.execute(sql)
            except Exception:
                pass
        conn.execute("""
            CREATE TABLE IF NOT EXISTS FOOTBALL.dues (
                id INTEGER PRIMARY KEY,
                player_id INTEGER NOT NULL,
                year INTEGER NOT NULL,
                quarter INTEGER NOT NULL CHECK (quarter >= 1 AND quarter <= 4),
                status VARCHAR NOT NULL DEFAULT 'owing',
                paid_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT current_timestamp,
                waiver_due_by DATE,
                UNIQUE(player_id, year, quarter)
            )
        """)
        try:
            conn.execute("ALTER TABLE FOOTBALL.dues ADD COLUMN waiver_due_by DATE")
        except Exception:
            pass
        conn.execute("""
            CREATE TABLE IF NOT EXISTS FOOTBALL.matchdays (
                id INTEGER PRIMARY KEY,
                sunday_date DATE NOT NULL,
                status VARCHAR NOT NULL DEFAULT 'voting_open',
                voting_opens_at TIMESTAMP,
                voting_closes_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT current_timestamp,
                reviewed_at TIMESTAMP,
                groups_published BOOLEAN DEFAULT false,
                fixtures_published BOOLEAN DEFAULT false,
                matchday_ended BOOLEAN DEFAULT false
            )
        """)
        for sql in [
            "ALTER TABLE FOOTBALL.matchdays ADD COLUMN fixtures_published BOOLEAN DEFAULT false",
            "ALTER TABLE FOOTBALL.matchdays ADD COLUMN matchday_ended BOOLEAN DEFAULT false",
        ]:
            try:
                conn.execute(sql)
            except Exception:
                pass
        try:
            conn.execute("CREATE SEQUENCE IF NOT EXISTS FOOTBALL.matchday_id_seq START 1")
            r = conn.execute("SELECT COALESCE(MAX(id), 0) FROM FOOTBALL.matchdays").fetchone()
            if r and r[0] and r[0] > 0:
                try:
                    conn.execute("SELECT setval('FOOTBALL.matchday_id_seq', ?)", [r[0]])
                except Exception:
                    pass
        except Exception:
            pass
        conn.execute("""
            CREATE TABLE IF NOT EXISTS FOOTBALL.matchday_fixtures (
                id INTEGER PRIMARY KEY,
                matchday_id INTEGER NOT NULL,
                group_a_id INTEGER NOT NULL,
                group_b_id INTEGER NOT NULL,
                status VARCHAR NOT NULL DEFAULT 'pending',
                home_goals INTEGER DEFAULT 0,
                away_goals INTEGER DEFAULT 0,
                started_at TIMESTAMP,
                ended_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT current_timestamp
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS FOOTBALL.fixture_goals (
                id INTEGER PRIMARY KEY,
                fixture_id INTEGER NOT NULL,
                scorer_player_id INTEGER NOT NULL,
                assister_player_id INTEGER,
                minute INTEGER,
                is_home_goal BOOLEAN NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS FOOTBALL.matchday_votes (
                id INTEGER PRIMARY KEY,
                matchday_id INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                voted_at TIMESTAMP DEFAULT current_timestamp,
                UNIQUE(matchday_id, player_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS FOOTBALL.matchday_groups (
                id INTEGER PRIMARY KEY,
                matchday_id INTEGER NOT NULL,
                group_index INTEGER NOT NULL,
                UNIQUE(matchday_id, group_index)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS FOOTBALL.matchday_group_members (
                id INTEGER PRIMARY KEY,
                matchday_id INTEGER NOT NULL,
                group_id INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                UNIQUE(matchday_id, player_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS FOOTBALL.matchday_attendance (
                matchday_id INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                present BOOLEAN NOT NULL DEFAULT true,
                PRIMARY KEY (matchday_id, player_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS FOOTBALL.fixture_ratings (
                fixture_id INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                rating REAL NOT NULL,
                PRIMARY KEY (fixture_id, player_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS FOOTBALL.matchday_cards (
                matchday_id INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                yellow_count INTEGER NOT NULL DEFAULT 0,
                red_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (matchday_id, player_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS FOOTBALL.fixture_cards (
                id INTEGER PRIMARY KEY,
                fixture_id INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                card_type VARCHAR NOT NULL CHECK (card_type IN ('yellow', 'red'))
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS FOOTBALL.payment_evidence (
                id INTEGER PRIMARY KEY,
                player_id INTEGER NOT NULL,
                year INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                file_path VARCHAR NOT NULL,
                file_name VARCHAR NOT NULL,
                status VARCHAR NOT NULL DEFAULT 'pending',
                submitted_at TIMESTAMP DEFAULT current_timestamp,
                reviewed_at TIMESTAMP,
                delete_after DATE
            )
        """)
        try:
            conn.execute("ALTER TABLE FOOTBALL.payment_evidence ADD COLUMN file_content BLOB")
        except Exception:
            pass
        # Seed fake players only in local dev (not in production/MotherDuck) so deletions persist
        if USE_LOCAL_DB:
            try:
                from api.routes.football import seed_fake_football_players
                seed_fake_football_players(conn)
            except Exception:
                pass
        print("FOOTBALL schema ready")
    except Exception as e:
        print(f"Startup warning: {e}")
    yield
    print("Shutting down...")
    close_all_connections()


app = FastAPI(
    title="Eko Football API",
    version="1.0.0",
    docs_url="/docs",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[],  # use regex to allow localhost (any port) and Render
    allow_origin_regex=r"https?://(localhost|127\.0\.0\.1)(:\d+)?$|https://.*\.onrender\.com",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)
app.include_router(football.router, prefix="/api/v1/football", tags=["Football"])


@app.get("/")
async def root():
    return {"name": "Eko Football API", "status": "ok", "docs": "/docs"}


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    import re
    origin = request.headers.get("origin") or ""
    allow = "*"
    if origin and re.match(r"https://.*\.onrender\.com", origin):
        allow = origin
    elif origin and ("localhost" in origin or "127.0.0.1" in origin):
        allow = origin
    return JSONResponse(
        status_code=500,
        content={"error": str(exc)},
        headers={
            "Access-Control-Allow-Origin": allow,
            "Access-Control-Allow-Credentials": "true",
        },
    )
