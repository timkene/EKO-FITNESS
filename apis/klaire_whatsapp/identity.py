"""MotherDuck identity lookup: resolve a phone number to an EnrolleeIdentity."""
from typing import Optional

import duckdb

from .models import EnrolleeIdentity


def _connect():
    return duckdb.connect("md:ai_driven_data")


def _normalise_for_query(phone: str) -> str:
    """Convert canonical 234XXXXXXXXXX → 0XXXXXXXXXX for DB lookup."""
    if phone.startswith("234") and len(phone) == 13:
        return "0" + phone[3:]
    return phone


def lookup_by_phone(phone: str) -> Optional[EnrolleeIdentity]:
    """Find an enrollee by phone number.

    Tries both the international (234-prefix) and local (0-prefix) formats
    across all three phone columns so that a match is found regardless of
    how the number was stored.
    """
    local = _normalise_for_query(phone)
    con = _connect()
    try:
        row = con.execute(
            """
            SELECT CAST(memberid AS VARCHAR), legacycode, firstname, lastname,
                   genderid, CAST(dateofbirth AS VARCHAR)
            FROM "AI DRIVEN DATA".MEMBER
            WHERE phone1 = ? OR phone2 = ? OR phone3 = ?
               OR phone1 = ? OR phone2 = ? OR phone3 = ?
            LIMIT 1
            """,
            [phone, phone, phone, local, local, local],
        ).fetchone()
        if not row:
            return None
        return EnrolleeIdentity(
            memberid=str(row[0]),
            legacycode=str(row[1]),
            firstname=row[2] or "",
            lastname=row[3] or "",
            genderid=int(row[4] or 0),
            dateofbirth=row[5],
        )
    finally:
        con.close()
