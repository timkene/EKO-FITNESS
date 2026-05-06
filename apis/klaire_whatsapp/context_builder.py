"""Live MotherDuck data pulls for Front Desk intents."""
import os
from typing import Optional, List

import duckdb


def _connect() -> duckdb.DuckDBPyConnection:
    if not os.environ.get("MOTHERDUCK_TOKEN"):
        raise RuntimeError("MOTHERDUCK_TOKEN environment variable is not set.")
    return duckdb.connect("md:ai_driven_data")


def get_mapped_hospital(memberid: str) -> Optional[dict]:
    """Return the hospital an enrollee is mapped to, or None."""
    con = _connect()
    try:
        row = con.execute(
            """
            SELECT p.providername, p.lganame, p.statename
            FROM "AI DRIVEN DATA".MEMBER_PROVIDER mp
            JOIN "AI DRIVEN DATA".PROVIDERS p
              ON TRY_CAST(mp.providerid AS BIGINT) = TRY_CAST(p.providerid AS BIGINT)
            WHERE CAST(mp.memberid AS VARCHAR) = ?
            LIMIT 1
            """,
            [memberid],
        ).fetchone()
        if not row:
            return None
        return {"providername": row[0], "lganame": row[1], "statename": row[2]}
    finally:
        con.close()


def get_plan_status(legacycode: str) -> Optional[dict]:
    """Return the enrollee's current active plan, or None."""
    con = _connect()
    try:
        row = con.execute(
            """
            SELECT mp.planid, mp.iscurrent,
                   CAST(mp.effectivedate AS VARCHAR),
                   CAST(mp.terminationdate AS VARCHAR)
            FROM "AI DRIVEN DATA".MEMBER_PLANS mp
            JOIN "AI DRIVEN DATA".MEMBER m
              ON CAST(mp.memberid AS VARCHAR) = CAST(m.memberid AS VARCHAR)
            WHERE m.legacycode = ?
              AND mp.iscurrent = 1
            LIMIT 1
            """,
            [legacycode],
        ).fetchone()
        if not row:
            return None
        return {
            "planid": str(row[0]),
            "iscurrent": bool(row[1]),
            "effectivedate": row[2],
            "terminationdate": row[3],
        }
    finally:
        con.close()


def get_limit_used(legacycode: str, start: str, end: str) -> float:
    """Sum of approved claims for the enrollee within a contract date range."""
    con = _connect()
    try:
        row = con.execute(
            """
            SELECT COALESCE(ROUND(SUM(c.approvedamount), 2), 0)
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            WHERE c.enrollee_id = ?
              AND TRY_CAST(c.encounterdatefrom AS DATE) BETWEEN ? AND ?
            """,
            [legacycode, start, end],
        ).fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0
    finally:
        con.close()


def get_pa_status(legacycode: str) -> List[dict]:
    """Return the last 5 PA requests for the enrollee."""
    con = _connect()
    try:
        rows = con.execute(
            """
            SELECT panumber, pastatus,
                   CAST(requestdate AS VARCHAR),
                   code, granted
            FROM "AI DRIVEN DATA"."PA DATA"
            WHERE IID = ?
            ORDER BY requestdate DESC
            LIMIT 5
            """,
            [legacycode],
        ).fetchall()
        return [
            {"panumber": r[0], "status": r[1], "date": r[2], "code": r[3], "granted": r[4]}
            for r in rows
        ]
    finally:
        con.close()


def get_benefits(planid: str) -> List[dict]:
    """Return benefit limits for the enrollee's plan (up to 20 rows)."""
    con = _connect()
    try:
        rows = con.execute(
            """
            SELECT pbl.benefitdesc, pbl.maxlimit, pbl.countperannum
            FROM "AI DRIVEN DATA".PLANBENEFITCODE_LIMIT pbl
            WHERE CAST(pbl.planid AS VARCHAR) = ?
            LIMIT 20
            """,
            [planid],
        ).fetchall()
        return [
            {"benefit": r[0], "max_limit": r[1], "count_per_year": r[2]}
            for r in rows
        ]
    finally:
        con.close()
