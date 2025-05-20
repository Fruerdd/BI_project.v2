#!/usr/bin/env python3
"""
etl.py

Usage:
  python etl.py full        # runs initial full load (batch_id=1)
  python etl.py inc <id>    # runs incremental load with given batch_id
"""

import sys
import pandas as pd
from sqlalchemy import create_engine, text
import datetime

# ───────────── Configuration ────────────────────────────────────────────────
SRC_DB    = "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"
WH_DB     = "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"
CSV_PATH  = "../data_sources/marketing_data_with_days.csv"
SRC_MAIN  = 1    # identifier for your OLTP database as a source
SRC_CSV   = 2    # identifier for your CSV as a source

# ───────────── Engines ───────────────────────────────────────────────────────
src_engine = create_engine(SRC_DB, echo=False)  # source (OLTP)
wh_engine  = create_engine(WH_DB,  echo=False)  # warehouse (staging + star)

# ───────────── Full Load ─────────────────────────────────────────────────────
def run_full_load():
    batch_id = 1
    print(f"[{datetime.datetime.now()}] ▶️  Starting FULL load (batch_id={batch_id})")

    # 1) Extract & Stage: pull *all* source tables into stage schema
    for tbl in ("users", "courses", "enrollments", "sales", "traffic_sources"):
        df = pd.read_sql(f"SELECT * FROM {tbl};", src_engine)
        df.to_sql(tbl, wh_engine, schema="stage", if_exists="replace", index=False)
        print(f"   • staged {tbl} ({len(df)} rows)")

    # Stage CSV
    cat_df = pd.read_csv(CSV_PATH)
    cat_df.to_sql("course_categories", wh_engine, schema="stage",
                  if_exists="replace", index=False)
    print(f"   • staged course_categories CSV ({len(cat_df)} rows)")

    # 2) Load all dimensions from staging -> warehouse with insert_id = batch_id
    with wh_engine.begin() as conn:
        # dim_user
        conn.execute(text("""
            INSERT INTO warehouse.dim_user
              (user_id, first_name, last_name, email, signup_date,
               insert_id, source_id)
            SELECT
              user_id, first_name, last_name, email,
              registered_at::date,
              :batch, :src
            FROM stage.users;
        """), {"batch": batch_id, "src": SRC_MAIN})
        print("   • loaded dim_user")

        # dim_course (merging CSV categories)
        conn.execute(text("""
            INSERT INTO warehouse.dim_course
              (course_id, title, subject, price_cents, category, sub_category,
               insert_id, source_id)
            SELECT
              c.course_id, c.title, c.subject, c.price_cents,
              cc.category, cc.sub_category,
              :batch, :src
            FROM stage.courses AS c
            LEFT JOIN stage.course_categories AS cc
              ON c.course_id = cc.course_id;
        """), {"batch": batch_id, "src": SRC_CSV})
        print("   • loaded dim_course")

        # dim_traffic_source
        conn.execute(text("""
            INSERT INTO warehouse.dim_traffic_source
              (traffic_source_id, name, channel, insert_id, source_id)
            SELECT
              source_id, name, channel,
              :batch, :src
            FROM stage.traffic_sources;
        """), {"batch": batch_id, "src": SRC_MAIN})
        print("   • loaded dim_traffic_source")

        # dim_date: build calendar between min/max sale_date
        result = conn.execute(text("""
            SELECT MIN(sale_date)::date AS min_d, MAX(sale_date)::date AS max_d
            FROM stage.sales
        """)).mappings().one()
        drange = pd.date_range(result["min_d"], result["max_d"], freq="D")
        date_df = pd.DataFrame({
            "date":      drange,
            "year":      drange.year,
            "quarter":   drange.quarter,
            "month":     drange.month,
            "day":       drange.day,
            "weekday":   drange.weekday,
            "insert_id": batch_id,
            "source_id": SRC_MAIN
        })
        date_df.to_sql("dim_date", wh_engine, schema="warehouse", if_exists="append", index=False)
        print(f"   • loaded dim_date ({len(date_df)} rows)")

    # 3) Load fact_sales by joining staging to dims
    with wh_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO warehouse.fact_sales
              (sale_id, user_key, course_key, traffic_source_key,
               date_key, amount_cents, enrollment_count)
            SELECT
              s.sale_id,
              du.user_key, dc.course_key, dt.traffic_source_key,
              dd.date_key,
              s.amount_cents,
              1
            FROM stage.sales AS s
            JOIN warehouse.dim_user           AS du ON s.user_id = du.user_id
            JOIN warehouse.dim_course         AS dc ON s.course_id = dc.course_id
            JOIN warehouse.dim_traffic_source AS dt ON s.source_id = dt.traffic_source_id
            JOIN warehouse.dim_date           AS dd ON s.sale_date::date = dd.date
        """))
        print("   • loaded fact_sales")

    print(f"[{datetime.datetime.now()}] ✅ FULL load complete.")

# ───────────── Helper: Upsert into staging ────────────────────────────────────
def upsert_stage(table: str, df: pd.DataFrame, pk_cols: list[str]):
    """
    Upsert all rows of df into stage.<table> using ON CONFLICT on pk_cols.
    """
    conn = wh_engine.raw_connection()
    cur = conn.cursor()
    cols = list(df.columns)
    col_list = ", ".join(cols)
    vals    = ", ".join(["%s"] * len(cols))
    updates = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c not in pk_cols)

    sql = f"""
      INSERT INTO stage.{table} ({col_list})
      VALUES ({vals})
      ON CONFLICT ({','.join(pk_cols)}) DO UPDATE
        SET {updates};
    """
    data = list(df.itertuples(index=False, name=None))
    cur.executemany(sql, data)
    conn.commit()
    cur.close()
    conn.close()
    print(f"   • upserted stage.{table} ({len(df)} rows)")

# ───────────── Incremental Load ───────────────────────────────────────────────
def run_incremental_load(batch_id: int):
    print(f"[{datetime.datetime.now()}] ▶️  Starting INCREMENTAL load (batch_id={batch_id})")

    # 1) Refresh staging via upsert (all tables + CSV)
    tables = [
        ("users", ["user_id"]),
        ("courses", ["course_id"]),
        ("enrollments", ["enrollment_id"]),
        ("sales", ["sale_id"]),
        ("traffic_sources", ["source_id"]),
        ("user_traffic", ["user_id","source_id","referred_at"])
    ]
    for tbl, pk in tables:
        df = pd.read_sql(f"SELECT * FROM {tbl};", src_engine)
        upsert_stage(tbl, df, pk)
    cat_df = pd.read_csv(CSV_PATH)
    upsert_stage("course_categories", cat_df, ["course_id"])

    # 2) Upsert new/changed dims (SCD Type-2 pattern)
    with wh_engine.begin() as conn:
        # New users
        conn.execute(text("""
            INSERT INTO warehouse.dim_user
              (user_id, first_name, last_name, email, signup_date,
               start_date, insert_id, source_id)
            SELECT su.user_id, su.first_name, su.last_name, su.email,
                   su.registered_at::date,
                   now(), :batch, :src
            FROM stage.users AS su
            LEFT JOIN warehouse.dim_user AS du
              ON su.user_id = du.user_id AND du.end_date IS NULL
            WHERE du.user_id IS NULL
        """), {"batch": batch_id, "src": SRC_MAIN})

        # Changed users: retire + insert
        conn.execute(text("""
            WITH changed AS (
              SELECT du.user_key, su.*
              FROM stage.users AS su
              JOIN warehouse.dim_user AS du
                ON su.user_id = du.user_id AND du.end_date IS NULL
              WHERE su.first_name <> du.first_name
                 OR su.last_name  <> du.last_name
                 OR su.email      <> du.email
                 OR su.registered_at::date <> du.signup_date
            )
            UPDATE warehouse.dim_user AS du
               SET end_date = now(), update_id = :batch
              FROM changed WHERE du.user_key = changed.user_key;

            INSERT INTO warehouse.dim_user
              (user_id, first_name, last_name, email, signup_date,
               start_date, insert_id, source_id)
            SELECT user_id, first_name, last_name, email,
                   registered_at::date,
                   now(), :batch, :src
            FROM changed
        """), {"batch": batch_id, "src": SRC_MAIN})

        # ── Repeat analogous logic for dim_course & dim_traffic_source ──

    # 3) Fact changes (new, changed, deleted)
    with wh_engine.begin() as conn:
        # New sales
        conn.execute(text("""
            INSERT INTO warehouse.fact_sales
              (sale_id, user_key, course_key, traffic_source_key,
               date_key, amount_cents, enrollment_count)
            SELECT
              s.sale_id,
              du.user_key, dc.course_key, dt.traffic_source_key,
              dd.date_key,
              s.amount_cents,
              1
            FROM stage.sales AS s
            JOIN warehouse.dim_user           AS du ON s.user_id = du.user_id AND du.end_date IS NULL
            JOIN warehouse.dim_course         AS dc ON s.course_id = dc.course_id AND dc.end_date IS NULL
            JOIN warehouse.dim_traffic_source AS dt ON s.source_id = dt.traffic_source_id AND dt.end_date IS NULL
            JOIN warehouse.dim_date           AS dd ON s.sale_date::date = dd.date
            LEFT JOIN warehouse.fact_sales AS f ON s.sale_id = f.sale_id
            WHERE f.sale_id IS NULL
        """))

        # Updated sales (amount only)
        conn.execute(text("""
            UPDATE warehouse.fact_sales AS f
               SET amount_cents = s.amount_cents
              FROM stage.sales AS s
             WHERE f.sale_id = s.sale_id
               AND f.amount_cents <> s.amount_cents
        """))

        # Deleted sales
        conn.execute(text("""
            DELETE FROM warehouse.fact_sales AS f
             WHERE NOT EXISTS (
               SELECT 1 FROM stage.sales AS s WHERE s.sale_id = f.sale_id
             );
        """))
        print("   • updated fact_sales")

    print(f"[{datetime.datetime.now()}] ✅ INCREMENTAL load complete.")

# ───────────── Main Entrypoint ───────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in ("full", "inc"):
        print(__doc__)
        sys.exit(1)

    if sys.argv[1] == "full":
        run_full_load()
    else:
        if len(sys.argv) != 3:
            print("Usage: python etl.py inc <batch_id>")
            sys.exit(1)
        run_incremental_load(int(sys.argv[2]))
