"""MotherDuck identity lookup for Klaire WhatsApp Agent."""
import os
from typing import Optional

import duckdb

from .models import EnrolleeIdentity

_SELECT = """
    SELECT CAST(memberid AS VARCHAR), legacycode, firstname, lastname,
           genderid, CAST(dob AS VARCHAR)
    FROM "AI DRIVEN DATA".MEMBER
"""


def _connect() -> duckdb.DuckDBPyConnection:
    if not os.environ.get("MOTHERDUCK_TOKEN"):
        raise RuntimeError("MOTHERDUCK_TOKEN environment variable is not set.")
    return duckdb.connect("md:ai_driven_data")


def _normalise_for_query(phone: str) -> str:
    """Convert canonical 234XXXXXXXXXX → 0XXXXXXXXXX for DB lookup."""
    if phone.startswith("234") and len(phone) == 13:
        return "0" + phone[3:]
    return phone


def _row_to_identity(row: tuple) -> EnrolleeIdentity:
    return EnrolleeIdentity(
        memberid=str(row[0]),
        legacycode=str(row[1]),
        firstname=row[2] or "",
        lastname=row[3] or "",
        genderid=int(row[4] or 0),
        dateofbirth=row[5],
    )


def lookup_by_legacycode(legacycode: str) -> Optional[EnrolleeIdentity]:
    """Primary lookup: find enrollee by their Enrollee ID (legacycode).

    Case-insensitive so enrollees can type CL/arik/698/2017 and still match.
    """
    con = _connect()
    try:
        row = con.execute(
            _SELECT + "WHERE UPPER(legacycode) = UPPER(?) LIMIT 1",
            [legacycode.strip()],
        ).fetchone()
        return _row_to_identity(row) if row else None
    finally:
        con.close()


def lookup_by_phone(phone: str) -> Optional[EnrolleeIdentity]:
    """Secondary lookup: used by aftercare nightly job to find phone from MEMBER.

    Tries both 234-prefix and 0-prefix formats across all three phone columns.
    """
    local = _normalise_for_query(phone)
    con = _connect()
    try:
        row = con.execute(
            _SELECT + """
            WHERE phone1 = ? OR phone2 = ? OR phone3 = ?
               OR phone1 = ? OR phone2 = ? OR phone3 = ?
            LIMIT 1
            """,
            [phone, phone, phone, local, local, local],
        ).fetchone()
        return _row_to_identity(row) if row else None
    finally:
        con.close()
