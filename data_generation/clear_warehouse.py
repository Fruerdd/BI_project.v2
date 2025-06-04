#!/usr/bin/env python3
"""
clear_warehouse.py

Usage:
  # Run this script to delete all rows (and reset identities) in every warehouse table.
  python clear_warehouse.py
"""

from sqlalchemy import create_engine, text

# ───────────── Configuration ───────────────────────────────────────────────────
# Update this URL if your Postgres credentials/host/port/database differ.
DB_URL = "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"

# ───────────── Create Engine ────────────────────────────────────────────────────
engine = create_engine(DB_URL, echo=False)

# ───────────── Clear Warehouse Function ─────────────────────────────────────────
def clear_warehouse():
    """
    Truncate (i.e., delete all rows from) every table in the 'warehouse' schema.
    We use TRUNCATE ... RESTART IDENTITY CASCADE so that:
      • All rows are removed.
      • Any SERIAL / IDENTITY columns reset to 1.
      • Any dependent tables (FKs) are emptied automatically (CASCADE).
    """
    with engine.begin() as conn:
        conn.execute(text("""
            TRUNCATE TABLE
              warehouse.user_traffic,
              warehouse.sales,
              warehouse.enrollments,
              warehouse.traffic_sources,
              warehouse.courses,
              warehouse.sales_managers,
              warehouse.users
            RESTART IDENTITY CASCADE;
        """))
        print("✅ All warehouse tables truncated (identities reset).")

# ───────────── Main Entrypoint ───────────────────────────────────────────────────
if __name__ == "__main__":
    clear_warehouse()
