#!/usr/bin/env python3
# sample_warehouse_data.py

from sqlalchemy import create_engine
import pandas as pd

# ─── CONFIGURE CONNECTION ─────────────────────────────────────────────────────
engine = create_engine(
    "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"
)

# ─── FUNCTIONS ────────────────────────────────────────────────────────────────
def sample_table(schema: str, table: str, limit: int = 5):
    """
    Print `limit` rows from `schema.table`.
    """
    df = pd.read_sql_query(
        f"SELECT *\n"
        f"  FROM {schema}.{table}\n"
        f" LIMIT {limit};",
        engine
    )
    print(f"\n--- Sample rows from {schema}.{table} ---")
    print(df.to_string(index=False))


def top_users_by_id(schema: str, limit: int = 5):
    """
    Print the top `limit` users with the highest user_id
    from `schema.users`.
    """
    df = pd.read_sql_query(
        f"SELECT *\n"
        f"  FROM {schema}.users\n"
        f" ORDER BY user_id DESC\n"
        f" LIMIT {limit};",
        engine
    )
    print(f"\n--- Top {limit} users by user_id in {schema}.users ---")
    print(df.to_string(index=False))


# ─── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 1) Sample a few rows from warehouse.users, courses, sales
    for tbl in ("users", "courses", "sales"):
        sample_table("warehouse", tbl)

    # 2) Then show the users with the highest user_id
    top_users_by_id("warehouse", limit=5)
