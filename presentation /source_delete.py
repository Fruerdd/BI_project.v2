#!/usr/bin/env python3
"""
delete_users.py

Bulk DELETE users from source.users table by user_id.
"""

import os
import urllib.parse
import psycopg2

# ─── CONFIG ───────────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"
)

def make_dsn(url: str) -> str:
    """
    Convert a SQLAlchemy‐style URL into a libpq DSN string.
    """
    u = urllib.parse.urlparse(url)
    return (
        f"host={u.hostname} "
        f"port={u.port} "
        f"dbname={u.path.lstrip('/')} "
        f"user={u.username} "
        f"password={u.password}"
    )

# ─── PAYLOAD ──────────────────────────────────────────────────────────────────
# List the user_ids you previously UPSERTed and now wish to remove:
USER_IDS_TO_DELETE = [100001, 100002]

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    dsn = make_dsn(DATABASE_URL)
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        sql = """
        DELETE FROM source.users
         WHERE user_id = ANY(%s);
        """
        cur.execute(sql, (USER_IDS_TO_DELETE,))
        conn.commit()
        print(f"Deleted {cur.rowcount} users (IDs: {USER_IDS_TO_DELETE}).")

if __name__ == "__main__":
    main()
