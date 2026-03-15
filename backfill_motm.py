"""
Backfill Man of the Match for all past ended matchdays.
Picks the highest-rated present player from each matchday.
Run once from project root: python backfill_motm.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from core.database import get_db_connection

def _player_matchday_rating(conn, matchday_id, player_id):
    """Average rating across all fixtures the player appeared in for this matchday."""
    rows = conn.execute("""
        SELECT fr.rating
        FROM FOOTBALL.fixture_ratings fr
        JOIN FOOTBALL.matchday_fixtures f ON f.id = fr.fixture_id
        WHERE f.matchday_id = ? AND fr.player_id = ?
    """, [matchday_id, player_id]).fetchall()
    if not rows:
        return 0.0
    return sum(r[0] for r in rows) / len(rows)

def compute_motm(conn, matchday_id):
    """Return (player_id, rating) of the highest-rated present player."""
    rows = conn.execute("""
        SELECT DISTINCT mgm.player_id
        FROM FOOTBALL.matchday_group_members mgm
        JOIN FOOTBALL.matchday_attendance a
          ON a.matchday_id = mgm.matchday_id AND a.player_id = mgm.player_id
        WHERE mgm.matchday_id = ? AND mgm.player_id > 0 AND a.present = true
    """, [matchday_id]).fetchall()
    if not rows:
        return None, 0.0
    best_pid, best_rating = None, -1.0
    for (pid,) in rows:
        r = _player_matchday_rating(conn, matchday_id, pid)
        if r > best_rating:
            best_rating = r
            best_pid = pid
    return best_pid, best_rating

def main():
    conn = get_db_connection(read_only=False)

    # Ensure table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS FOOTBALL.matchday_motm (
            matchday_id INTEGER NOT NULL,
            player_id   INTEGER NOT NULL,
            sunday_date DATE
        )
    """)

    ended = conn.execute(
        "SELECT id, sunday_date FROM FOOTBALL.matchdays WHERE matchday_ended = true ORDER BY sunday_date"
    ).fetchall()

    if not ended:
        print("No ended matchdays found.")
        return

    print(f"Found {len(ended)} ended matchday(s). Backfilling MOTM...\n")

    filled, skipped, no_data = 0, 0, 0
    for mid, sunday_date in ended:
        existing = conn.execute(
            "SELECT player_id FROM FOOTBALL.matchday_motm WHERE matchday_id = ?", [mid]
        ).fetchone()

        if existing:
            pid = existing[0]
            name = conn.execute(
                "SELECT baller_name FROM FOOTBALL.players WHERE id = ?", [pid]
            ).fetchone()
            print(f"  Matchday {sunday_date} (id={mid}) — already set: {name[0] if name else pid} [skipped]")
            skipped += 1
            continue

        motm_pid, motm_rating = compute_motm(conn, mid)
        if motm_pid:
            conn.execute(
                "INSERT INTO FOOTBALL.matchday_motm (matchday_id, player_id, sunday_date) VALUES (?, ?, ?)",
                [mid, motm_pid, sunday_date]
            )
            name = conn.execute(
                "SELECT baller_name FROM FOOTBALL.players WHERE id = ?", [motm_pid]
            ).fetchone()
            print(f"  Matchday {sunday_date} (id={mid}) — MOTM: {name[0] if name else motm_pid} (avg rating {motm_rating:.2f}) ✓")
            filled += 1
        else:
            print(f"  Matchday {sunday_date} (id={mid}) — no rated players found [skipped]")
            no_data += 1

    print(f"\nDone. Filled: {filled} | Already set: {skipped} | No data: {no_data}")
    conn.close()

if __name__ == "__main__":
    main()
