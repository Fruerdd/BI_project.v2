#!/usr/bin/env python3
"""
update_traffic_sources.py

Bulk UPDATE channel and details in source.traffic_sources.
"""

import os
import urllib.parse
import psycopg2
from psycopg2.extras import execute_values

# ─── CONFIG ───────────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"
)

def make_dsn(url: str) -> str:
    """
    Convert a SQLAlchemy-style URL into a libpq DSN for psycopg2.
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
# List of tuples: (new_channel, new_details, source_id)
UPDATES = [
    # Example: change Telegram Channel to messaging, bump weight
    ("messaging", "Weight=12", 2),
    # Example: tweak Official Website weight
    ("web",       "Weight=25", 4),
    # Example: adjust Google Ads weight
    ("ads",       "Weight=12", 9),
    # …add more (channel, details, source_id) as needed…
]

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    dsn = make_dsn(DATABASE_URL)
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        sql = """
        UPDATE source.traffic_sources AS t
           SET channel = data.channel,
               details = data.details
          FROM (VALUES %s) AS data(channel, details, source_id)
         WHERE t.source_id = data.source_id;
        """
        # execute_values will expand '%s' into a VALUES list
        execute_values(cur, sql, UPDATES, template="(%s, %s, %s)")
        conn.commit()
        print(f"Updated {len(UPDATES)} traffic_sources rows.")

if __name__ == "__main__":
    main()
