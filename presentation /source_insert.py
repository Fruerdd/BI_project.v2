#!/usr/bin/env python3
"""
insert_users.py

Bulk UPSERT into source.users:
  - Inserts new users
  - Updates existing users if user_id already exists
"""

import os
import urllib.parse
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"
)

def make_dsn(url: str) -> str:
    u = urllib.parse.urlparse(url)
    return (
        f"host={u.hostname} "
        f"port={u.port} "
        f"dbname={u.path.lstrip('/')} "
        f"user={u.username} "
        f"password={u.password}"
    )

# ─── PAYLOAD ──────────────────────────────────────────────────────────────────
# Each tuple must match the INSERT columns below:
USERS = [
    # (user_id, first_name, last_name, email, phone, country, registered_at)
    (100001, "Alice", "Johnson", "pavel@example.com", "555-0101", "USA", datetime.utcnow()),
    (100002, "Bob",   "Lee",     "kuznet@example.com",   "555-0102", "Canada", datetime.utcnow()),
    # …add more users here…
]

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    dsn = make_dsn(DATABASE_URL)
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        sql = """
        INSERT INTO source.users
          (user_id, first_name, last_name, email, phone, country, registered_at)
        VALUES %s
        ON CONFLICT (user_id) DO UPDATE
          SET first_name    = EXCLUDED.first_name,
              last_name     = EXCLUDED.last_name,
              email         = EXCLUDED.email,
              phone         = EXCLUDED.phone,
              country       = EXCLUDED.country,
              registered_at = EXCLUDED.registered_at;
        """
        execute_values(cur, sql, USERS)
        conn.commit()
        print(f"Upserted {len(USERS)} users.")

if __name__ == "__main__":
    main()
