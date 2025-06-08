#!/usr/bin/env python3

"""
update_source_sales.py

Double the cost_in_rubbles for all source.sales records
whose sale_date falls in the previous calendar month.
"""

import datetime
import calendar
from sqlalchemy import create_engine, text

# ───────────── Configuration ───────────────────────────────────────────────────
DB_URL = "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"

# ───────────── Helper to get last month range ──────────────────────────────────
def last_month_range(today=None):
    """
    Return (first_day, last_day) of the previous calendar month.
    """
    today = today or datetime.date.today()
    first_of_this_month = today.replace(day=1)
    last_day_prev = first_of_this_month - datetime.timedelta(days=1)
    first_day_prev = last_day_prev.replace(day=1)
    return first_day_prev, last_day_prev

# ───────────── Main update logic ────────────────────────────────────────────────
def main():
    engine = create_engine(DB_URL, echo=False)
    start_date, end_date = last_month_range()

    print(f"👉 Doubling cost_in_rubbles in source.sales from {start_date} to {end_date}…")

    sql = text("""
        UPDATE source.sales
           SET cost_in_rubbles = cost_in_rubbles * 2
         WHERE sale_date::DATE
               BETWEEN :start_date AND :end_date;
    """)

    with engine.begin() as conn:
        result = conn.execute(sql, {
            "start_date": start_date,
            "end_date":   end_date
        })
        print(f"✅ Updated {result.rowcount} row(s).")

if __name__ == "__main__":
    main()
