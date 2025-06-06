#!/usr/bin/env python3

"""
etl.py

Usage:
  # Full initial load (batch_id=1)
  python etl.py full

  # Incremental load (pass the next batch_id, e.g. 2, 3, …)
  python etl.py inc 2

This script performs a Slowly Changing Dimension (SCD2) load into a “warehouse” schema,
then builds a star‐schema (dim_user, dim_course, dim_traffic_source, dim_sales_manager, dim_date, fact_sales).

**Key change for incremental:**
During INCREMENTAL loads, we now rebuild the entire `fact_sales` from all **active** (`update_id IS NULL`, `end_date = '9999-12-31'`) rows in `warehouse.sales`.
This ensures that _every_ current version (where `update_id IS NULL`) ends up in the star schema, not just those inserted in the current batch.

**NEW**: We also want to load **`data_sources/user_traffic.csv`** into `warehouse.user_traffic`
(with `source_id_audit = 2`), using exactly the same SCD2 logic that we already use for `source.user_traffic` (“source” side is `source_id_audit = 1`).
That means:
  1. On a **full load**, insert all CSV rows (as if they were “brand‐new”) with `source_id_audit = 2`.
  2. On an **incremental load**, do two sub‐steps:
     - Insert any CSV row that did not exist at all in `warehouse.user_traffic` (matching on keys `(user_sk, traffic_source_sk, referred_at)`).
     - For those that _did_ exist but whose `campaign_code` changed, “close out” the old version (set its `end_date = NOW()` and `update_id = batch`) and insert a new version with `source_id_audit = 2`.

**Important**: _Don’t_ change **any** of the existing SCD2 logic against `source.user_traffic`; we merely tack on a second pass that picks up CSV‐side rows and treats them in parallel (just using `source_id_audit = 2`).
"""

import sys
import datetime
import pytz
import csv
from sqlalchemy import create_engine, text

# ───────────── Configuration ───────────────────────────────────────────────────
DB_URL           = "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"
SRC_MAIN         = 1   # arbitrary source_id for audit (the “source” schema)
SRC_CSV_AUDIT_ID = 2   # arbitrary source_id for CSV‐based audit

# Adjust this path if your CSV lives somewhere else:
CSV_PATH = "data_sources/user_traffic.csv"

# ───────────── Engines ───────────────────────────────────────────────────────────
source_engine    = create_engine(DB_URL, echo=False)
warehouse_engine = create_engine(DB_URL, echo=False)


def run_full_load():
    batch_id = 1
    now_ts = datetime.datetime.now(pytz.UTC)
    print(f"[{now_ts}] ▶️  Starting FULL load (batch_id={batch_id})\n")

    # ─── 1) TRUNCATE all warehouse + star_schema tables in one short transaction ───
    with warehouse_engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE warehouse.user_traffic        CASCADE"))
        conn.execute(text("TRUNCATE TABLE warehouse.traffic_sources    CASCADE"))
        conn.execute(text("TRUNCATE TABLE warehouse.sales             CASCADE"))
        conn.execute(text("TRUNCATE TABLE warehouse.enrollments       CASCADE"))
        conn.execute(text("TRUNCATE TABLE warehouse.courses           CASCADE"))
        conn.execute(text("TRUNCATE TABLE warehouse.sales_managers    CASCADE"))
        conn.execute(text("TRUNCATE TABLE warehouse.users             CASCADE"))

        conn.execute(text("TRUNCATE TABLE star_schema.fact_sales         CASCADE"))
        conn.execute(text("TRUNCATE TABLE star_schema.dim_date           CASCADE"))
        conn.execute(text("TRUNCATE TABLE star_schema.dim_traffic_source CASCADE"))
        conn.execute(text("TRUNCATE TABLE star_schema.dim_course         CASCADE"))
        conn.execute(text("TRUNCATE TABLE star_schema.dim_user           CASCADE"))
        conn.execute(text("TRUNCATE TABLE star_schema.dim_sales_manager  CASCADE"))
    print("   • truncated all warehouse + star_schema tables\n")

    # ───────────────────────────────────────────────────────────────────────────────
    # 2) Populate each warehouse table in its own transaction (to release locks quickly)
    # ───────────────────────────────────────────────────────────────────────────────
    #
    #  2.1) USERS → warehouse.users (SCD2 full)
    with warehouse_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO warehouse.users
              (user_id, first_name, last_name, email, phone, country, registered_at,
               start_date, end_date, source_id, insert_id, update_id)
            SELECT
              u.user_id,
              u.first_name,
              u.last_name,
              u.email,
              u.phone,
              u.country,
              u.registered_at,
              NOW()               AS start_date,
              '9999-12-31'::DATE  AS end_date,
              :src_main           AS source_id,
              :batch              AS insert_id,
              NULL                AS update_id
            FROM source.users AS u;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
    print("   • loaded warehouse.users")

    #  2.2) SALES_MANAGERS → warehouse.sales_managers (SCD2 full)
    with warehouse_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO warehouse.sales_managers
              (manager_id, first_name, last_name, email, hired_at,
               start_date, end_date, source_id, insert_id, update_id)
            SELECT
              m.manager_id,
              m.first_name,
              m.last_name,
              m.email,
              m.hired_at,
              NOW()               AS start_date,
              '9999-12-31'::DATE  AS end_date,
              :src_main           AS source_id,
              :batch              AS insert_id,
              NULL                AS update_id
            FROM source.sales_managers AS m;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
    print("   • loaded warehouse.sales_managers")

    #  2.3) COURSES → warehouse.courses (SCD2 full)
    with warehouse_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO warehouse.courses
              (course_id, title, subject, description, price_in_rubbles, created_at,
               category, sub_category,
               start_date, end_date, source_id, insert_id, update_id)
            SELECT
              c.course_id,
              c.title,
              c.subject,
              c.description,
              c.price_in_rubbles,
              c.created_at,
              cat.name                 AS category,
              sub.name                 AS sub_category,
              NOW()                    AS start_date,
              '9999-12-31'::DATE       AS end_date,
              :src_main                AS source_id,
              :batch                   AS insert_id,
              NULL                     AS update_id
            FROM source.courses AS c
            JOIN source.categories    AS cat ON c.category_id    = cat.category_id
            JOIN source.subcategories AS sub ON c.subcategory_id = sub.subcategory_id;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
    print("   • loaded warehouse.courses")

    #  2.4) ENROLLMENTS → warehouse.enrollments (SCD2 full)
    with warehouse_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO warehouse.enrollments
              (enrollment_id, user_sk, course_sk, enrolled_at, status,
               start_date, end_date, source_id, insert_id, update_id)
            SELECT
              e.enrollment_id,
              u.user_sk,
              c.course_sk,
              e.enrolled_at,
              e.status,
              NOW()               AS start_date,
              '9999-12-31'::DATE  AS end_date,
              :src_main           AS source_id,
              :batch              AS insert_id,
              NULL                AS update_id
            FROM source.enrollments AS e
            JOIN warehouse.users AS u
              ON e.user_id = u.user_id
             AND u.end_date = '9999-12-31'::DATE
            JOIN warehouse.courses AS c
              ON e.course_id = c.course_id
             AND c.end_date = '9999-12-31'::DATE;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
    print("   • loaded warehouse.enrollments")

    #  2.5) SALES → warehouse.sales (SCD2 full)
    with warehouse_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO warehouse.sales
              (sale_id, enrollment_sk, sales_manager_sk, sale_date, cost_in_rubbles,
               start_date, end_date, source_id, insert_id, update_id)
            SELECT
              s.sale_id,
              e.enrollment_sk,
              m.sales_manager_sk,
              s.sale_date,
              s.cost_in_rubbles,
              NOW()               AS start_date,
              '9999-12-31'::DATE  AS end_date,
              :src_main           AS source_id,
              :batch              AS insert_id,
              NULL                AS update_id
            FROM source.sales AS s
            JOIN warehouse.enrollments AS e
              ON s.enrollment_id = e.enrollment_id
             AND e.end_date = '9999-12-31'::DATE
            JOIN warehouse.sales_managers AS m
              ON s.manager_id = m.manager_id
             AND m.end_date = '9999-12-31'::DATE;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
    print("   • loaded warehouse.sales")

    #  2.6) TRAFFIC_SOURCES → warehouse.traffic_sources (SCD2 full)
    with warehouse_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO warehouse.traffic_sources
              (source_id, name, channel, details,
               start_date, end_date, source_id_audit, insert_id, update_id)
            SELECT
              t.source_id,
              t.name,
              t.channel,
              t.details,
              NOW()               AS start_date,
              '9999-12-31'::DATE  AS end_date,
              :src_main           AS source_id_audit,
              :batch              AS insert_id,
              NULL                AS update_id
            FROM source.traffic_sources AS t;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
    print("   • loaded warehouse.traffic_sources")

    # ───────────────────────────────────────────────────────────────────────────────
    #  2.7) USER_TRAFFIC → warehouse.user_traffic (SCD2 full from “source” side)
    #        (this is exactly as before, except we now tack on a second sub‐step for CSV)
    # ───────────────────────────────────────────────────────────────────────────────

    #  2.7.a) All “source.user_traffic” rows → warehouse.user_traffic (SCD2 full, source_id_audit = 1)
    with warehouse_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO warehouse.user_traffic
              (user_sk, traffic_source_sk, referred_at, campaign_code,
               start_date, end_date, source_id_audit, insert_id, update_id)
            SELECT
              u.user_sk,
              t.traffic_source_sk,
              ut.referred_at,
              ut.campaign_code,
              NOW()               AS start_date,
              '9999-12-31'::DATE  AS end_date,
              :src_main           AS source_id_audit,
              :batch              AS insert_id,
              NULL                AS update_id
            FROM source.user_traffic AS ut
            JOIN warehouse.users AS u
              ON ut.user_id = u.user_id
             AND u.end_date = '9999-12-31'::DATE
            JOIN warehouse.traffic_sources AS t
              ON ut.source_id = t.source_id
             AND t.end_date = '9999-12-31'::DATE;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
    print("   • loaded warehouse.user_traffic from source.user_traffic (source_id_audit=1)")

    # ───────────────────────────────────────────────────────────────────────────────
    #  2.7.b) Now load every row from CSV → warehouse.user_traffic (as if new, source_id_audit = 2)
    #        * Full‐load version simply “truncates” domain above, so we can insert _all_ CSV rows fresh
    # ───────────────────────────────────────────────────────────────────────────────
    #
    # We must look up u.user_sk  and t.traffic_source_sk by joining against warehouse.users/traffic_sources;
    # then we insert with start_date=NOW(), end_date='9999-12-31', source_id_audit = 2.
    #
    # NB: We assume the CSV columns are exactly: user_id, source_id, referred_at (ISO‐timestamp), campaign_code
    #
    with warehouse_engine.begin() as conn:
        reader = csv.DictReader(open(CSV_PATH, newline=""))
        rows_to_insert = []
        for row in reader:
            # Each row in the CSV has user_id, source_id, referred_at, campaign_code
            # We’ll pass these as params to the SELECT … INSERT; the SELECT will lookup user_sk & traffic_source_sk.
            rows_to_insert.append({
                "user_id"      : row["user_id"],
                "source_id"    : row["source_id"],
                "referred_at"  : row["referred_at"],
                "campaign_code": row["campaign_code"],
            })

        # If there are no rows in the CSV, skip the block:
        if rows_to_insert:
            conn.execute(text("""
                INSERT INTO warehouse.user_traffic
                  (user_sk, traffic_source_sk, referred_at, campaign_code,
                   start_date, end_date, source_id_audit, insert_id, update_id)
                SELECT
                  u.user_sk,
                  t.traffic_source_sk,
                  CAST(:referred_at AS TIMESTAMP),
                  :campaign_code,
                  NOW()               AS start_date,
                  '9999-12-31'::DATE  AS end_date,
                  :csv_audit          AS source_id_audit,
                  :batch              AS insert_id,
                  NULL                AS update_id
                FROM warehouse.users AS u
                JOIN warehouse.traffic_sources AS t
                  ON t.source_id = :source_id
                 AND t.end_date = '9999-12-31'::DATE
                WHERE u.user_id = :user_id
                  AND u.end_date = '9999-12-31'::DATE;
            """), [
                {
                    "user_id"      : r["user_id"],
                    "source_id"    : r["source_id"],
                    "referred_at"  : r["referred_at"],
                    "campaign_code": r["campaign_code"],
                    "csv_audit"    : SRC_CSV_AUDIT_ID,
                    "batch"        : batch_id
                }
                for r in rows_to_insert
            ])
    print("   • loaded warehouse.user_traffic from CSV (source_id_audit=2)\n")

    # ───────────────────────────────────────────────────────────────────────────────
    #  3) Build “star_schema” dims & fact, each in its own transaction (UNCHANGED)
    # ───────────────────────────────────────────────────────────────────────────────

    #  3.1) dim_user
    with warehouse_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO star_schema.dim_user
              (user_key, user_id, first_name, last_name, email, signup_date, country)
            SELECT
              u.user_sk    AS user_key,
              u.user_id,
              u.first_name,
              u.last_name,
              u.email,
              u.registered_at::DATE AS signup_date,
              u.country
            FROM warehouse.users AS u
            WHERE u.end_date = '9999-12-31'::DATE;
        """))
    print("   • loaded star_schema.dim_user")

    #  3.2) dim_course
    with warehouse_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO star_schema.dim_course
              (course_key, course_id, title, subject, price_in_rubbles, category, sub_category)
            SELECT
              c.course_sk    AS course_key,
              c.course_id,
              c.title,
              c.subject,
              c.price_in_rubbles,
              c.category,
              c.sub_category
            FROM warehouse.courses AS c
            WHERE c.end_date = '9999-12-31'::DATE;
        """))
    print("   • loaded star_schema.dim_course")

    #  3.3) dim_traffic_source
    with warehouse_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO star_schema.dim_traffic_source
              (traffic_source_key, traffic_source_id, name, channel)
            SELECT
              t.traffic_source_sk    AS traffic_source_key,
              t.source_id            AS traffic_source_id,
              t.name,
              t.channel
            FROM warehouse.traffic_sources AS t
            WHERE t.end_date = '9999-12-31'::DATE;
        """))
    print("   • loaded star_schema.dim_traffic_source")

    #  3.4) dim_sales_manager
    with warehouse_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO star_schema.dim_sales_manager
              (sales_manager_key, manager_id, first_name, last_name, email, hired_at)
            SELECT
              m.sales_manager_sk    AS sales_manager_key,
              m.manager_id,
              m.first_name,
              m.last_name,
              m.email,
              m.hired_at::DATE
            FROM warehouse.sales_managers AS m
            WHERE m.end_date = '9999-12-31'::DATE;
        """))
    print("   • loaded star_schema.dim_sales_manager")

    #  3.5) dim_date
    with warehouse_engine.begin() as conn:
        result = conn.execute(text("""
            SELECT
              (SELECT MIN(registered_at)::DATE     FROM warehouse.users      ) AS min_u,
              (SELECT MIN(created_at)::DATE        FROM warehouse.courses    ) AS min_c,
              (SELECT MIN(enrolled_at)::DATE       FROM warehouse.enrollments) AS min_e,
              (SELECT MIN(sale_date)::DATE         FROM warehouse.sales       ) AS min_s,
              (SELECT MIN(referred_at)::DATE       FROM warehouse.user_traffic) AS min_ut,

              (SELECT MAX(registered_at)::DATE     FROM warehouse.users      ) AS max_u,
              (SELECT MAX(created_at)::DATE        FROM warehouse.courses    ) AS max_c,
              (SELECT MAX(enrolled_at)::DATE       FROM warehouse.enrollments) AS max_e,
              (SELECT MAX(sale_date)::DATE         FROM warehouse.sales       ) AS max_s,
              (SELECT MAX(referred_at)::DATE       FROM warehouse.user_traffic) AS max_ut
        """)).mappings().one()

    min_candidates = [
        result["min_u"],
        result["min_c"],
        result["min_e"],
        result["min_s"],
        result["min_ut"]
    ]
    max_candidates = [
        result["max_u"],
        result["max_c"],
        result["max_e"],
        result["max_s"],
        result["max_ut"]
    ]

    min_date = min([d for d in min_candidates if d is not None], default=datetime.date.today())
    max_date = max([d for d in max_candidates if d is not None], default=datetime.date.today())

    with warehouse_engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO star_schema.dim_date
              (date_key, date, year, quarter, month, day, weekday)
            SELECT
              TO_CHAR(gs::DATE, 'YYYYMMDD')::INT AS date_key,
              gs::DATE                            AS date,
              EXTRACT(YEAR   FROM gs)::INT        AS year,
              EXTRACT(QUARTER FROM gs)::INT       AS quarter,
              EXTRACT(MONTH  FROM gs)::INT        AS month,
              EXTRACT(DAY    FROM gs)::INT        AS day,
              EXTRACT(DOW    FROM gs)::INT        AS weekday
            FROM generate_series(
              '{min_date}'::DATE,
              '{max_date}'::DATE,
              '1 day'::INTERVAL
            ) AS gs;
        """))
    total_days = (max_date - min_date).days + 1
    print(f"   • loaded star_schema.dim_date ({total_days} days from {min_date} to {max_date})")

    #  3.6) fact_sales
    with warehouse_engine.begin() as conn:
        conn.execute(text("""
                INSERT INTO star_schema.fact_sales
      (sale_id, user_key, course_key, sales_manager_key, traffic_source_key, date_key,
       total_in_rubbles, enrollment_count)
    SELECT
      s.sale_id,
      e.user_sk                   AS user_key,
      e.course_sk                 AS course_key,
      s.sales_manager_sk          AS sales_manager_key,
      COALESCE(ut.traffic_source_sk, -1) AS traffic_source_key,
      dd.date_key,
      s.cost_in_rubbles           AS total_in_rubbles,
      1                           AS enrollment_count
    FROM warehouse.sales AS s
    JOIN warehouse.enrollments AS e 
      ON s.enrollment_sk = e.enrollment_sk
    LEFT JOIN (
      SELECT DISTINCT ON (user_sk)
         user_sk,
         traffic_source_sk,
         referred_at,
         campaign_code
      FROM warehouse.user_traffic
      WHERE end_date = '9999-12-31'::DATE
      ORDER BY user_sk, referred_at DESC
      -- ^ “Distinct on (user_sk)” picks the single ACTIVE row with the latest referred_at per user
    ) AS ut
      ON e.user_sk = ut.user_sk
    JOIN star_schema.dim_date AS dd 
      ON s.sale_date::DATE = dd.date
    WHERE s.end_date = '9999-12-31'::DATE;

        """))
    print("   • loaded star_schema.fact_sales\n")

    print(f"[{datetime.datetime.now(pytz.UTC)}] ✅ FULL load complete.\n")



def run_incremental_load(batch_id: int):
    now_ts = datetime.datetime.now(pytz.UTC)
    print(f"[{now_ts}] ▶️  Starting INCREMENTAL load (batch_id={batch_id})\n")

    # ───────────────────────────────────────────────────────────────────────────────
    #  1) Incrementally update “warehouse” schema with SCD2 logic, one-step-per-txn
    # ───────────────────────────────────────────────────────────────────────────────

    #  1.1) USERS (unchanged)
    with warehouse_engine.begin() as conn:
        #  1.1.a) Insert brand-new users
        conn.execute(text("""
            INSERT INTO warehouse.users (
                user_id, first_name, last_name, email, phone, country, registered_at,
                start_date, end_date, source_id, insert_id, update_id
            )
            SELECT
                s.user_id,
                s.first_name,
                s.last_name,
                s.email,
                s.phone,
                s.country,
                s.registered_at,
                NOW()               AS start_date,
                '9999-12-31'::DATE  AS end_date,
                :src_main           AS source_id,
                :batch              AS insert_id,
                NULL                AS update_id
            FROM source.users AS s
            LEFT JOIN warehouse.users AS w
              ON s.user_id = w.user_id
                 AND w.end_date = '9999-12-31'::DATE
            WHERE w.user_id IS NULL;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
    print("   • inserted new warehouse.users")

    with warehouse_engine.begin() as conn:
        #  1.1.b) Insert new versions for changed users
        conn.execute(text("""
            INSERT INTO warehouse.users (
                user_id, first_name, last_name, email, phone, country, registered_at,
                start_date, end_date, source_id, insert_id, update_id
            )
            SELECT
                s.user_id,
                s.first_name,
                s.last_name,
                s.email,
                s.phone,
                s.country,
                s.registered_at,
                NOW()               AS start_date,
                '9999-12-31'::DATE  AS end_date,
                :src_main           AS source_id,
                :batch              AS insert_id,
                NULL                AS update_id
            FROM source.users AS s
            JOIN warehouse.users AS w
              ON s.user_id = w.user_id
             AND w.end_date = '9999-12-31'::DATE
            WHERE 
                  s.first_name    IS DISTINCT FROM w.first_name
              OR  s.last_name     IS DISTINCT FROM w.last_name
              OR  s.email         IS DISTINCT FROM w.email
              OR  s.phone         IS DISTINCT FROM w.phone
              OR  s.country       IS DISTINCT FROM w.country
              OR  s.registered_at IS DISTINCT FROM w.registered_at;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
    print("   • inserted new versions for changed warehouse.users")

    with warehouse_engine.begin() as conn:
        #  1.1.c) Close out old active user records
        conn.execute(text("""
            UPDATE warehouse.users AS w
               SET end_date  = NOW(),
                   update_id = :batch
            FROM source.users AS s
            WHERE w.user_id = s.user_id
              AND w.end_date = '9999-12-31'::DATE
              AND (
                    s.first_name    IS DISTINCT FROM w.first_name
                OR  s.last_name     IS DISTINCT FROM w.last_name
                OR  s.email         IS DISTINCT FROM w.email
                OR  s.phone         IS DISTINCT FROM w.phone
                OR  s.country       IS DISTINCT FROM w.country
                OR  s.registered_at IS DISTINCT FROM w.registered_at
              );
        """), {"batch": batch_id})
    print("   • closed out old versions of warehouse.users\n")

    #  1.2) SALES_MANAGERS (unchanged)
    with warehouse_engine.begin() as conn:
        #  1.2.a) Insert brand-new sales_managers
        conn.execute(text("""
            INSERT INTO warehouse.sales_managers (
                manager_id, first_name, last_name, email, hired_at,
                start_date, end_date, source_id, insert_id, update_id
            )
            SELECT
                s.manager_id,
                s.first_name,
                s.last_name,
                s.email,
                s.hired_at,
                NOW()               AS start_date,
                '9999-12-31'::DATE  AS end_date,
                :src_main           AS source_id,
                :batch              AS insert_id,
                NULL                AS update_id
            FROM source.sales_managers AS s
            LEFT JOIN warehouse.sales_managers AS w
              ON s.manager_id = w.manager_id
                 AND w.end_date = '9999-12-31'::DATE
            WHERE w.manager_id IS NULL;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
    print("   • inserted new warehouse.sales_managers")

    with warehouse_engine.begin() as conn:
        #  1.2.b) Insert new versions for changed sales_managers
        conn.execute(text("""
            INSERT INTO warehouse.sales_managers (
                manager_id, first_name, last_name, email, hired_at,
                start_date, end_date, source_id, insert_id, update_id
            )
            SELECT
                s.manager_id,
                s.first_name,
                s.last_name,
                s.email,
                s.hired_at,
                NOW()               AS start_date,
                '9999-12-31'::DATE  AS end_date,
                :src_main           AS source_id,
                :batch              AS insert_id,
                NULL                AS update_id
            FROM source.sales_managers AS s
            JOIN warehouse.sales_managers AS w
              ON s.manager_id = w.manager_id
             AND w.end_date = '9999-12-31'::DATE
            WHERE 
                  s.first_name IS DISTINCT FROM w.first_name
              OR  s.last_name  IS DISTINCT FROM w.last_name
              OR  s.email      IS DISTINCT FROM w.email
              OR  s.hired_at   IS DISTINCT FROM w.hired_at;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
    print("   • inserted new versions for changed warehouse.sales_managers")

    with warehouse_engine.begin() as conn:
        #  1.2.c) Close out old active sales_managers
        conn.execute(text("""
            UPDATE warehouse.sales_managers AS w
               SET end_date     = NOW(),
                   update_id    = :batch
            FROM source.sales_managers AS s
            WHERE w.manager_id = s.manager_id
              AND w.end_date = '9999-12-31'::DATE
              AND (
                    s.first_name IS DISTINCT FROM w.first_name
                OR  s.last_name  IS DISTINCT FROM w.last_name
                OR  s.email      IS DISTINCT FROM w.email
                OR  s.hired_at   IS DISTINCT FROM w.hired_at
              );
        """), {"batch": batch_id})
    print("   • closed out old versions of warehouse.sales_managers\n")

    #  1.3) COURSES (unchanged)
    with warehouse_engine.begin() as conn:
        #  1.3.a) Insert brand-new courses
        conn.execute(text("""
            INSERT INTO warehouse.courses (
                course_id, title, subject, description, price_in_rubbles, created_at,
                category, sub_category,
                start_date, end_date, source_id, insert_id, update_id
            )
            SELECT
                s.course_id,
                s.title,
                s.subject,
                s.description,
                s.price_in_rubbles,
                s.created_at,
                cat.name                 AS category,
                sub.name                 AS sub_category,
                NOW()                    AS start_date,
                '9999-12-31'::DATE       AS end_date,
                :src_main                AS source_id,
                :batch                   AS insert_id,
                NULL                     AS update_id
            FROM source.courses AS s
            JOIN source.categories    AS cat ON s.category_id    = cat.category_id
            JOIN source.subcategories AS sub ON s.subcategory_id = sub.subcategory_id
            LEFT JOIN warehouse.courses AS w
              ON s.course_id = w.course_id
                 AND w.end_date = '9999-12-31'::DATE
            WHERE w.course_id IS NULL;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
    print("   • inserted new warehouse.courses")

    with warehouse_engine.begin() as conn:
        #  1.3.b) Insert new versions for changed courses
        conn.execute(text("""
            INSERT INTO warehouse.courses (
                course_id, title, subject, description, price_in_rubbles, created_at,
                category, sub_category,
                start_date, end_date, source_id, insert_id, update_id
            )
            SELECT
                s.course_id,
                s.title,
                s.subject,
                s.description,
                s.price_in_rubbles,
                s.created_at,
                cat.name                 AS category,
                sub.name                 AS sub_category,
                NOW()                    AS start_date,
                '9999-12-31'::DATE       AS end_date,
                :src_main                AS source_id,
                :batch                   AS insert_id,
                NULL                     AS update_id
            FROM source.courses AS s
            JOIN warehouse.courses AS w
              ON s.course_id = w.course_id
             AND w.end_date = '9999-12-31'::DATE
            JOIN source.categories    AS cat ON s.category_id    = cat.category_id
            JOIN source.subcategories AS sub ON s.subcategory_id = sub.subcategory_id
            WHERE 
                  s.title             IS DISTINCT FROM w.title
              OR  s.subject           IS DISTINCT FROM w.subject
              OR  COALESCE(s.description, '') <> COALESCE(w.description, '')
              OR  s.price_in_rubbles  IS DISTINCT FROM w.price_in_rubbles
              OR  s.created_at        IS DISTINCT FROM w.created_at
              OR  cat.name            IS DISTINCT FROM w.category
              OR  sub.name            IS DISTINCT FROM w.sub_category;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
    print("   • inserted new versions for changed warehouse.courses")

    with warehouse_engine.begin() as conn:
        #  1.3.c) Close out old active courses
        conn.execute(text("""
            UPDATE warehouse.courses AS w
               SET end_date  = NOW(),
                   update_id = :batch
            FROM source.courses AS s
            JOIN source.categories    AS cat ON s.category_id    = cat.category_id
            JOIN source.subcategories AS sub ON s.subcategory_id = sub.subcategory_id
            WHERE w.course_id = s.course_id
              AND w.end_date = '9999-12-31'::DATE
              AND (
                    s.title             IS DISTINCT FROM w.title
                OR  s.subject           IS DISTINCT FROM w.subject
                OR  COALESCE(s.description, '') <> COALESCE(w.description, '')
                OR  s.price_in_rubbles  IS DISTINCT FROM w.price_in_rubbles
                OR  s.created_at        IS DISTINCT FROM w.created_at
                OR  cat.name            IS DISTINCT FROM w.category
                OR  sub.name            IS DISTINCT FROM w.sub_category
              );
        """), {"batch": batch_id})
    print("   • closed out old versions of warehouse.courses\n")

    #  1.4) ENROLLMENTS (unchanged)
    with warehouse_engine.begin() as conn:
        #  1.4.a) Insert brand-new enrollments
        conn.execute(text("""
            INSERT INTO warehouse.enrollments (
                enrollment_id, user_sk, course_sk, enrolled_at, status,
                start_date, end_date, source_id, insert_id, update_id
            )
            SELECT
                s.enrollment_id,
                u.user_sk,
                c.course_sk,
                s.enrolled_at,
                s.status,
                NOW()               AS start_date,
                '9999-12-31'::DATE  AS end_date,
                :src_main           AS source_id,
                :batch              AS insert_id,
                NULL                AS update_id
            FROM source.enrollments AS s
            JOIN warehouse.users AS u
              ON s.user_id = u.user_id
             AND u.end_date = '9999-12-31'::DATE
            JOIN warehouse.courses AS c
              ON s.course_id = c.course_id
             AND c.end_date = '9999-12-31'::DATE
            LEFT JOIN warehouse.enrollments AS w
              ON s.enrollment_id = w.enrollment_id
                 AND w.end_date = '9999-12-31'::DATE
            WHERE w.enrollment_id IS NULL;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
    print("   • inserted new warehouse.enrollments")

    with warehouse_engine.begin() as conn:
        #  1.4.b) Insert new versions for changed enrollments
        conn.execute(text("""
            INSERT INTO warehouse.enrollments (
                enrollment_id, user_sk, course_sk, enrolled_at, status,
                start_date, end_date, source_id, insert_id, update_id
            )
            SELECT
                s.enrollment_id,
                u.user_sk,
                c.course_sk,
                s.enrolled_at,
                s.status,
                NOW()               AS start_date,
                '9999-12-31'::DATE  AS end_date,
                :src_main           AS source_id,
                :batch              AS insert_id,
                NULL                AS update_id
            FROM source.enrollments AS s
            JOIN warehouse.enrollments AS w
              ON s.enrollment_id = w.enrollment_id
             AND w.end_date = '9999-12-31'::DATE
            JOIN warehouse.users AS u
              ON s.user_id = u.user_id
             AND u.end_date = '9999-12-31'::DATE
            JOIN warehouse.courses AS c
              ON s.course_id = c.course_id
             AND c.end_date = '9999-12-31'::DATE
            WHERE 
                  s.user_id     IS DISTINCT FROM (
                    SELECT x.user_id 
                    FROM warehouse.users x 
                    WHERE x.user_sk = w.user_sk
                  )
              OR  s.course_id   IS DISTINCT FROM (
                    SELECT y.course_id 
                    FROM warehouse.courses y 
                    WHERE y.course_sk = w.course_sk
                  )
              OR  s.enrolled_at IS DISTINCT FROM w.enrolled_at
              OR  s.status      IS DISTINCT FROM w.status;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
    print("   • inserted new versions for changed warehouse.enrollments")

    with warehouse_engine.begin() as conn:
        #  1.4.c) Close out old active enrollments
        conn.execute(text("""
            UPDATE warehouse.enrollments AS w
               SET end_date  = NOW(),
                   update_id = :batch
            FROM source.enrollments AS s
            JOIN warehouse.users AS u
              ON s.user_id = u.user_id
             AND u.end_date = '9999-12-31'::DATE
            JOIN warehouse.courses AS c
              ON s.course_id = c.course_id
             AND c.end_date = '9999-12-31'::DATE
            WHERE w.enrollment_id = s.enrollment_id
              AND w.end_date = '9999-12-31'::DATE
              AND (
                    s.user_id     IS DISTINCT FROM (
                      SELECT x.user_id 
                      FROM warehouse.users x 
                      WHERE x.user_sk = w.user_sk
                    )
                OR  s.course_id   IS DISTINCT FROM (
                      SELECT y.course_id 
                      FROM warehouse.courses y 
                      WHERE y.course_sk = w.course_sk
                    )
                OR  s.enrolled_at IS DISTINCT FROM w.enrolled_at
                OR  s.status      IS DISTINCT FROM w.status
              );
        """), {"batch": batch_id})
    print("   • closed out old versions of warehouse.enrollments\n")

    #  1.5) SALES (unchanged)
    with warehouse_engine.begin() as conn:
        #  1.5.a) Insert brand-new sales
        conn.execute(text("""
            INSERT INTO warehouse.sales (
                sale_id, enrollment_sk, sales_manager_sk, sale_date, cost_in_rubbles,
                start_date, end_date, source_id, insert_id, update_id
            )
            SELECT
                s.sale_id,
                e.enrollment_sk,
                m.sales_manager_sk,
                s.sale_date,
                s.cost_in_rubbles,
                NOW()               AS start_date,
                '9999-12-31'::DATE  AS end_date,
                :src_main           AS source_id,
                :batch              AS insert_id,
                NULL                AS update_id
            FROM source.sales AS s
            JOIN warehouse.enrollments AS e
              ON s.enrollment_id = e.enrollment_id
             AND e.end_date = '9999-12-31'::DATE
            JOIN warehouse.sales_managers AS m
              ON s.manager_id = m.manager_id
             AND m.end_date = '9999-12-31'::DATE
            LEFT JOIN warehouse.sales AS w
              ON s.sale_id = w.sale_id
                 AND w.end_date = '9999-12-31'::DATE
            WHERE w.sale_id IS NULL;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
    print("   • inserted new warehouse.sales")

    with warehouse_engine.begin() as conn:
        #  1.5.b) Insert new versions for changed sales
        conn.execute(text("""
            INSERT INTO warehouse.sales (
                sale_id, enrollment_sk, sales_manager_sk, sale_date, cost_in_rubbles,
                start_date, end_date, source_id, insert_id, update_id
            )
            SELECT
                s.sale_id,
                e.enrollment_sk,
                m.sales_manager_sk,
                s.sale_date,
                s.cost_in_rubbles,
                NOW()               AS start_date,
                '9999-12-31'::DATE  AS end_date,
                :src_main           AS source_id,
                :batch              AS insert_id,
                NULL                AS update_id
            FROM source.sales AS s
            JOIN warehouse.sales AS w
              ON s.sale_id = w.sale_id
             AND w.end_date = '9999-12-31'::DATE
            JOIN warehouse.enrollments AS e
              ON s.enrollment_id = e.enrollment_id
             AND e.end_date = '9999-12-31'::DATE
            JOIN warehouse.sales_managers AS m
              ON s.manager_id = m.manager_id
             AND m.end_date = '9999-12-31'::DATE
            WHERE 
                  s.enrollment_id   IS DISTINCT FROM (
                    SELECT x.enrollment_id 
                    FROM warehouse.enrollments x 
                    WHERE x.enrollment_sk = w.enrollment_sk
                  )
              OR  s.manager_id      IS DISTINCT FROM (
                    SELECT y.manager_id 
                    FROM warehouse.sales_managers y 
                    WHERE y.sales_manager_sk = w.sales_manager_sk
                  )
              OR  s.sale_date       IS DISTINCT FROM w.sale_date
              OR  s.cost_in_rubbles IS DISTINCT FROM w.cost_in_rubbles;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
    print("   • inserted new versions for changed warehouse.sales")

    with warehouse_engine.begin() as conn:
        #  1.5.c) Close out old active sales
        conn.execute(text("""
            UPDATE warehouse.sales AS w
               SET end_date  = NOW(),
                   update_id = :batch
            FROM source.sales AS s
            JOIN warehouse.enrollments AS e
              ON s.enrollment_id = e.enrollment_id
             AND e.end_date = '9999-12-31'::DATE
            JOIN warehouse.sales_managers AS m
              ON s.manager_id = m.manager_id
             AND m.end_date = '9999-12-31'::DATE
            WHERE w.sale_id = s.sale_id
              AND w.end_date = '9999-12-31'::DATE
              AND (
                    s.enrollment_id   IS DISTINCT FROM (
                      SELECT x.enrollment_id 
                      FROM warehouse.enrollments x 
                      WHERE x.enrollment_sk = w.enrollment_sk
                    )
                OR  s.manager_id      IS DISTINCT FROM (
                      SELECT y.manager_id 
                      FROM warehouse.sales_managers y 
                      WHERE y.sales_manager_sk = w.sales_manager_sk
                    )
                OR  s.sale_date       IS DISTINCT FROM w.sale_date
                OR  s.cost_in_rubbles IS DISTINCT FROM w.cost_in_rubbles
              );
        """), {"batch": batch_id})
    print("   • closed out old versions of warehouse.sales\n")

    #  1.6) TRAFFIC_SOURCES (unchanged)
    with warehouse_engine.begin() as conn:
        #  1.6.a) Insert brand-new traffic_sources
        conn.execute(text("""
            INSERT INTO warehouse.traffic_sources (
                source_id, name, channel, details,
                start_date, end_date, source_id_audit, insert_id, update_id
            )
            SELECT
                s.source_id,
                s.name,
                s.channel,
                s.details,
                NOW()               AS start_date,
                '9999-12-31'::DATE  AS end_date,
                :src_main           AS source_id_audit,
                :batch              AS insert_id,
                NULL                AS update_id
            FROM source.traffic_sources AS s
            LEFT JOIN warehouse.traffic_sources AS w
              ON s.source_id = w.source_id
                 AND w.end_date = '9999-12-31'::DATE
            WHERE w.source_id IS NULL;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
    print("   • inserted new warehouse.traffic_sources")

    with warehouse_engine.begin() as conn:
        #  1.6.b) Insert new versions for changed traffic_sources
        conn.execute(text("""
            INSERT INTO warehouse.traffic_sources (
                source_id, name, channel, details,
                start_date, end_date, source_id_audit, insert_id, update_id
            )
            SELECT
                s.source_id,
                s.name,
                s.channel,
                s.details,
                NOW()               AS start_date,
                '9999-12-31'::DATE  AS end_date,
                :src_main           AS source_id_audit,
                :batch              AS insert_id,
                NULL                AS update_id
            FROM source.traffic_sources AS s
            JOIN warehouse.traffic_sources AS w
              ON s.source_id = w.source_id
             AND w.end_date = '9999-12-31'::DATE
            WHERE 
                  s.name    IS DISTINCT FROM w.name
              OR  s.channel IS DISTINCT FROM w.channel
              OR  COALESCE(s.details, '') IS DISTINCT FROM COALESCE(w.details, '');
        """), {"src_main": SRC_MAIN, "batch": batch_id})
    print("   • inserted new versions for changed warehouse.traffic_sources")

    with warehouse_engine.begin() as conn:
        #  1.6.c) Close out old active traffic_sources
        conn.execute(text("""
            UPDATE warehouse.traffic_sources AS w
               SET end_date     = NOW(),
                   update_id    = :batch
            FROM source.traffic_sources AS s
            WHERE w.source_id = s.source_id
              AND w.end_date = '9999-12-31'::DATE
              AND (
                    s.name    IS DISTINCT FROM w.name
                OR  s.channel IS DISTINCT FROM w.channel
                OR  COALESCE(s.details, '') IS DISTINCT FROM COALESCE(w.details, '')
              );
        """), {"batch": batch_id})
    print("   • closed out old versions of warehouse.traffic_sources\n")

    # ───────────────────────────────────────────────────────────────────────────────
    #  1.7) USER_TRAFFIC
    # ───────────────────────────────────────────────────────────────────────────────

    #  1.7.a) Insert brand-new “source.user_traffic” rows → warehouse.user_traffic
    #          (SCD2, source_id_audit = 1)
    with warehouse_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO warehouse.user_traffic (
                user_sk, traffic_source_sk, referred_at, campaign_code,
                start_date, end_date, source_id_audit, insert_id, update_id
            )
            SELECT
                u.user_sk,
                t.traffic_source_sk,
                s.referred_at,
                s.campaign_code,
                NOW()               AS start_date,
                '9999-12-31'::DATE  AS end_date,
                :src_main           AS source_id_audit,
                :batch              AS insert_id,
                NULL                AS update_id
            FROM source.user_traffic AS s
            JOIN warehouse.users AS u
              ON s.user_id = u.user_id
             AND u.end_date = '9999-12-31'::DATE
            JOIN warehouse.traffic_sources AS t
              ON s.source_id = t.source_id
             AND t.end_date = '9999-12-31'::DATE
            LEFT JOIN warehouse.user_traffic AS w
              ON u.user_sk = w.user_sk
             AND t.traffic_source_sk = w.traffic_source_sk
             AND s.referred_at = w.referred_at
             AND w.end_date = '9999-12-31'::DATE
            WHERE w.user_traffic_sk IS NULL;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
    print("   • inserted new warehouse.user_traffic from source.user_traffic (source_id_audit=1)")

    with warehouse_engine.begin() as conn:
        #  1.7.b) Insert new versions for changed “source.user_traffic” (campaign_code changed)
        conn.execute(text("""
            INSERT INTO warehouse.user_traffic (
                user_sk, traffic_source_sk, referred_at, campaign_code,
                start_date, end_date, source_id_audit, insert_id, update_id
            )
            SELECT
                u.user_sk,
                t.traffic_source_sk,
                s.referred_at,
                s.campaign_code,
                NOW()               AS start_date,
                '9999-12-31'::DATE  AS end_date,
                :src_main           AS source_id_audit,
                :batch              AS insert_id,
                NULL                AS update_id
            FROM source.user_traffic AS s
            JOIN warehouse.user_traffic AS w
              ON s.referred_at = w.referred_at
             AND w.end_date = '9999-12-31'::DATE
            JOIN warehouse.users AS u
              ON s.user_id = u.user_id
             AND u.end_date = '9999-12-31'::DATE
            JOIN warehouse.traffic_sources AS t
              ON s.source_id = t.source_id
             AND t.end_date = '9999-12-31'::DATE
            WHERE COALESCE(s.campaign_code, '') <> COALESCE(w.campaign_code, '');
        """), {"src_main": SRC_MAIN, "batch": batch_id})
    print("   • inserted new versions for changed warehouse.user_traffic from source (source_id_audit=1)")

    with warehouse_engine.begin() as conn:
        #  1.7.c) Close out old active “source.user_traffic” rows (campaign_code changed)
        conn.execute(text("""
            UPDATE warehouse.user_traffic AS w
               SET end_date     = NOW(),
                   update_id    = :batch
            FROM source.user_traffic AS s
            JOIN warehouse.users AS u
              ON s.user_id = u.user_id
             AND u.end_date = '9999-12-31'::DATE
            JOIN warehouse.traffic_sources AS t
              ON s.source_id = t.source_id
             AND t.end_date = '9999-12-31'::DATE
            WHERE w.user_sk = u.user_sk
              AND w.traffic_source_sk = t.traffic_source_sk
              AND w.referred_at = s.referred_at
              AND w.end_date   = '9999-12-31'::DATE
              AND COALESCE(s.campaign_code, '') <> COALESCE(w.campaign_code, '');
        """), {"batch": batch_id})
    print("   • closed out old versions of warehouse.user_traffic from source (source_id_audit=1)\n")

    # ───────────────────────────────────────────────────────────────────────────────
    #  1.7.1) CSV “user_traffic.csv” → warehouse.user_traffic (source_id_audit = 2)
    #          (Incremental logic: brand‐new CSV rows + changed campaign_code)
    # ───────────────────────────────────────────────────────────────────────────────
    #
    # We must:
    #   (a) Insert brand‐new CSV rows that do not exist in warehouse.user_traffic
    #       (matching on user_sk, traffic_source_sk, referred_at).
    #   (b) For existing CSV rows whose campaign_code changed, “close” the old version,
    #       then insert a brand‐new version with source_id_audit = 2.
    #
    # We read the CSV fully and do two passes: new rows & changed rows.
    #
    csv_rows = list(csv.DictReader(open(CSV_PATH, newline="")))

    # (1.a) INSERT brand‐new CSV rows that don't exist yet:
    #       We look up for each CSV row: "warehouse.user_traffic" match on (user_sk, traffic_source_sk, referred_at, end_date='9999-12-31').
    #       If not found, we insert it as a new SCD2 row (start_date=NOW(), end_date='9999-12-31', source_id_audit=2).
    new_inserts = []
    for r in csv_rows:
        # Check existence in active warehouse.user_traffic
        with warehouse_engine.begin() as conn:
            found = conn.execute(text("""
                SELECT 1
                  FROM warehouse.user_traffic AS w
                  JOIN warehouse.users         AS u ON u.user_sk = w.user_sk
                  JOIN warehouse.traffic_sources AS t ON t.traffic_source_sk = w.traffic_source_sk
                 WHERE u.user_id = :user_id
                   AND t.source_id = :source_id
                   AND w.referred_at = CAST(:referred_at AS TIMESTAMP)
                   AND w.end_date = '9999-12-31'::DATE
               LIMIT 1;
            """), {
                "user_id"     : r["user_id"],
                "source_id"   : r["source_id"],
                "referred_at" : r["referred_at"],
            }).first()

        if not found:
            # This CSV row doesn’t exist in the active slice → we’ll insert it fresh.
            new_inserts.append(r)

    if new_inserts:
        with warehouse_engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO warehouse.user_traffic
                  (user_sk, traffic_source_sk, referred_at, campaign_code,
                   start_date, end_date, source_id_audit, insert_id, update_id)
                SELECT
                  u.user_sk,
                  t.traffic_source_sk,
                  CAST(:referred_at AS TIMESTAMP),
                  :campaign_code,
                  NOW()               AS start_date,
                  '9999-12-31'::DATE  AS end_date,
                  :csv_audit          AS source_id_audit,
                  :batch              AS insert_id,
                  NULL                AS update_id
                FROM warehouse.users AS u
                JOIN warehouse.traffic_sources AS t
                  ON t.source_id = :source_id
                 AND t.end_date = '9999-12-31'::DATE
                WHERE u.user_id = :user_id
                  AND u.end_date = '9999-12-31'::DATE;
            """), [
                {
                    "user_id"      : r["user_id"],
                    "source_id"    : r["source_id"],
                    "referred_at"  : r["referred_at"],
                    "campaign_code": r["campaign_code"],
                    "csv_audit"    : SRC_CSV_AUDIT_ID,
                    "batch"        : batch_id
                }
                for r in new_inserts
            ])
    print(f"   • inserted {len(new_inserts)} new CSV rows into warehouse.user_traffic (source_id_audit=2)")

    # (1.b) Find any CSV rows whose campaign_code changed _relative to_ the currently‐active row:
    #       If we see a row (user_id, source_id, referred_at) that _is_ in warehouse.user_traffic but its active version
    #       has a different campaign_code, we “close out” the old version and insert a new one.
    changed_inserts = []
    for r in csv_rows:
        # Look up the active warehouse.user_traffic for this combination:
        row = None
        with warehouse_engine.begin() as conn:
            row = conn.execute(text("""
                SELECT w.user_traffic_sk,
                       w.campaign_code
                  FROM warehouse.user_traffic AS w
                  JOIN warehouse.users AS u ON u.user_sk = w.user_sk
                  JOIN warehouse.traffic_sources AS t ON t.traffic_source_sk = w.traffic_source_sk
                 WHERE u.user_id = :user_id
                   AND t.source_id = :source_id
                   AND w.referred_at = CAST(:referred_at AS TIMESTAMP)
                   AND w.end_date = '9999-12-31'::DATE
                 LIMIT 1;
            """), {
                "user_id"     : r["user_id"],
                "source_id"   : r["source_id"],
                "referred_at" : r["referred_at"],
            }).mappings().first()

        if row:
            existing_campaign = row["campaign_code"] or ""
            new_campaign      = r["campaign_code"] or ""
            if new_campaign != existing_campaign:
                # The campaign_code changed → close out the old version and insert a new one
                changed_inserts.append(r)

                # (i) Close out the old version:
                with warehouse_engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE warehouse.user_traffic AS w
                           SET end_date   = NOW(),
                               update_id  = :batch
                        FROM warehouse.users AS u
                        JOIN warehouse.traffic_sources AS t ON t.traffic_source_sk = w.traffic_source_sk
                        WHERE u.user_id = :user_id
                          AND t.source_id = :source_id
                          AND w.referred_at = CAST(:referred_at AS TIMESTAMP)
                          AND w.end_date = '9999-12-31'::DATE;
                    """), {
                        "user_id"     : r["user_id"],
                        "source_id"   : r["source_id"],
                        "referred_at" : r["referred_at"],
                        "batch"       : batch_id
                    })

    # (ii) Insert a fresh version for each “changed” CSV row:
    if changed_inserts:
        with warehouse_engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO warehouse.user_traffic
                  (user_sk, traffic_source_sk, referred_at, campaign_code,
                   start_date, end_date, source_id_audit, insert_id, update_id)
                SELECT
                  u.user_sk,
                  t.traffic_source_sk,
                  CAST(:referred_at AS TIMESTAMP),
                  :campaign_code,
                  NOW()               AS start_date,
                  '9999-12-31'::DATE  AS end_date,
                  :csv_audit          AS source_id_audit,
                  :batch              AS insert_id,
                  NULL                AS update_id
                FROM warehouse.users AS u
                JOIN warehouse.traffic_sources AS t
                  ON t.source_id = :source_id
                 AND t.end_date = '9999-12-31'::DATE
                WHERE u.user_id = :user_id
                  AND u.end_date = '9999-12-31'::DATE;
            """), [
                {
                    "user_id"      : r["user_id"],
                    "source_id"    : r["source_id"],
                    "referred_at"  : r["referred_at"],
                    "campaign_code": r["campaign_code"],
                    "csv_audit"    : SRC_CSV_AUDIT_ID,
                    "batch"        : batch_id
                }
                for r in changed_inserts
            ])
    print(f"   • closed out and re‐inserted {len(changed_inserts)} changed CSV rows into warehouse.user_traffic (source_id_audit=2)\n")


    # ───────────────────────────────────────────────────────────────────────────────
    #  2) Refresh “star_schema” dims & fact
    # ───────────────────────────────────────────────────────────────────────────────

    #  2.0) DELETE existing fact_sales (FK constraints require this first)
    with warehouse_engine.begin() as conn:
        conn.execute(text("DELETE FROM star_schema.fact_sales;"))
    print("   • cleared star_schema.fact_sales")

    #  2.1) dim_user – delete + re‐insert changed keys (explicit SK)
    with warehouse_engine.begin() as conn:
        conn.execute(text("""
            DELETE FROM star_schema.dim_user 
            WHERE user_key IN (
                SELECT u.user_sk 
                FROM warehouse.users u
                WHERE u.insert_id = :batch OR u.update_id = :batch
            );
        """), {"batch": batch_id})
        conn.execute(text("""
            INSERT INTO star_schema.dim_user
              (user_key, user_id, first_name, last_name, email, signup_date, country)
            SELECT
              u.user_sk    AS user_key,
              u.user_id,
              u.first_name,
              u.last_name,
              u.email,
              u.registered_at::DATE AS signup_date,
              u.country
            FROM warehouse.users u
            WHERE u.end_date = '9999-12-31'::DATE
              AND (u.insert_id = :batch OR u.update_id = :batch);
        """), {"batch": batch_id})
    print("   • updated star_schema.dim_user")

    #  2.2) dim_course – delete + re‐insert changed keys (explicit SK)
    with warehouse_engine.begin() as conn:
        conn.execute(text("""
            DELETE FROM star_schema.dim_course
            WHERE course_key IN (
                SELECT c.course_sk 
                FROM warehouse.courses c
                WHERE c.insert_id = :batch OR c.update_id = :batch
            );
        """), {"batch": batch_id})
        conn.execute(text("""
            INSERT INTO star_schema.dim_course
              (course_key, course_id, title, subject, price_in_rubbles, category, sub_category)
            SELECT
              c.course_sk    AS course_key,
              c.course_id,
              c.title,
              c.subject,
              c.price_in_rubbles,
              c.category,
              c.sub_category
            FROM warehouse.courses c
            WHERE c.end_date = '9999-12-31'::DATE
              AND (c.insert_id = :batch OR c.update_id = :batch);
        """), {"batch": batch_id})
    print("   • updated star_schema.dim_course")

    #  2.3) dim_traffic_source – delete + re‐insert changed keys (explicit SK)
    with warehouse_engine.begin() as conn:
        conn.execute(text("""
            DELETE FROM star_schema.dim_traffic_source
            WHERE traffic_source_key IN (
                SELECT t.traffic_source_sk 
                FROM warehouse.traffic_sources t
                WHERE t.insert_id = :batch OR t.update_id = :batch
            );
        """), {"batch": batch_id})
        conn.execute(text("""
            INSERT INTO star_schema.dim_traffic_source
              (traffic_source_key, traffic_source_id, name, channel)
            SELECT
              t.traffic_source_sk    AS traffic_source_key,
              t.source_id            AS traffic_source_id,
              t.name,
              t.channel
            FROM warehouse.traffic_sources t
            WHERE t.end_date = '9999-12-31'::DATE
              AND (t.insert_id = :batch OR t.update_id = :batch);
        """), {"batch": batch_id})
    print("   • updated star_schema.dim_traffic_source")

    #  2.4) dim_sales_manager – delete + re‐insert changed keys (explicit SK)
    with warehouse_engine.begin() as conn:
        conn.execute(text("""
            DELETE FROM star_schema.dim_sales_manager
            WHERE sales_manager_key IN (
                SELECT m.sales_manager_sk 
                FROM warehouse.sales_managers m
                WHERE m.insert_id = :batch OR m.update_id = :batch
            );
        """), {"batch": batch_id})
        conn.execute(text("""
            INSERT INTO star_schema.dim_sales_manager
              (sales_manager_key, manager_id, first_name, last_name, email, hired_at)
            SELECT
              m.sales_manager_sk    AS sales_manager_key,
              m.manager_id,
              m.first_name,
              m.last_name,
              m.email,
              m.hired_at::DATE
            FROM warehouse.sales_managers m
            WHERE m.end_date = '9999-12-31'::DATE
              AND (m.insert_id = :batch OR m.update_id = :batch);
        """), {"batch": batch_id})
    print("   • updated star_schema.dim_sales_manager")

    #  2.5) dim_date – only if new sales dates fall outside existing range (UNCHANGED)
    with warehouse_engine.begin() as conn:
        sale_row = conn.execute(text("""
            SELECT 
                MIN(s.sale_date)::DATE AS min_sale_date,
                MAX(s.sale_date)::DATE AS max_sale_date
            FROM warehouse.sales s
            WHERE s.insert_id = :batch
              AND s.end_date = '9999-12-31'::DATE;
        """), {"batch": batch_id}).mappings().one()

    min_sale_date = sale_row["min_sale_date"]
    max_sale_date = sale_row["max_sale_date"]

    if not min_sale_date or not max_sale_date:
        print("   • no new sales dates found; skipped dim_date update")
    else:
        with warehouse_engine.begin() as conn:
            drange = conn.execute(text("""
                SELECT MIN(date) AS min_date, MAX(date) AS max_date
                FROM star_schema.dim_date;
            """)).mappings().one()

        current_min = drange["min_date"]
        current_max = drange["max_date"]

        needs_rebuild = False
        if (current_min is None) or (min_sale_date < current_min) or (max_sale_date > current_max):
            needs_rebuild = True

        if needs_rebuild:
            candidates_min = [d for d in [min_sale_date, current_min] if d is not None]
            candidates_max = [d for d in [max_sale_date, current_max] if d is not None]
            new_min = min(candidates_min) if candidates_min else datetime.date.today()
            new_max = max(candidates_max) if candidates_max else datetime.date.today()

            with warehouse_engine.begin() as conn:
                conn.execute(text("DELETE FROM star_schema.dim_date;"))
                conn.execute(text(f"""
                    INSERT INTO star_schema.dim_date
                      (date_key, date, year, quarter, month, day, weekday)
                    SELECT
                      TO_CHAR(gs::DATE, 'YYYYMMDD')::INT AS date_key,
                      gs::DATE                         AS date,
                      EXTRACT(YEAR   FROM gs)::INT     AS year,
                      EXTRACT(QUARTER FROM gs)::INT     AS quarter,
                      EXTRACT(MONTH  FROM gs)::INT     AS month,
                      EXTRACT(DAY    FROM gs)::INT     AS day,
                      EXTRACT(DOW    FROM gs)::INT     AS weekday
                    FROM generate_series(
                      '{new_min}'::DATE,
                      '{new_max}'::DATE,
                      '1 day'::INTERVAL
                    ) AS gs;
                """))
            day_count = (new_max - new_min).days + 1
            print(f"   • extended star_schema.dim_date ({day_count} days from {new_min} to {new_max})")
        else:
            print("   • no date range extension needed for star_schema.dim_date")

    #  2.6) fact_sales – Rebuild entire fact from all “active” warehouse.sales rows
    # ─────────────────────────────────────────────────────────────────────────────────────────
    #  2.6) fact_sales – Rebuild entire fact from all “active” warehouse.sales rows
    #      (use the same DISTINCT ON subquery for user_traffic as in full load)
    # ─────────────────────────────────────────────────────────────────────────────────────────

    with warehouse_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO star_schema.fact_sales
              (sale_id, user_key, course_key, sales_manager_key, traffic_source_key, date_key,
               total_in_rubbles, enrollment_count)
            SELECT
              s.sale_id,
              e.user_sk                   AS user_key,
              e.course_sk                 AS course_key,
              s.sales_manager_sk          AS sales_manager_key,
              COALESCE(ut.traffic_source_sk, -1) AS traffic_source_key,
              dd.date_key,
              s.cost_in_rubbles           AS total_in_rubbles,
              1                           AS enrollment_count
            FROM warehouse.sales AS s
            JOIN warehouse.enrollments AS e 
              ON s.enrollment_sk = e.enrollment_sk
             AND e.end_date = '9999-12-31'::DATE
            LEFT JOIN (
              -- pick exactly one “latest” user_traffic row per user_sk
              SELECT DISTINCT ON (user_sk)
                 user_sk,
                 traffic_source_sk,
                 referred_at,
                 campaign_code
              FROM warehouse.user_traffic
              WHERE end_date = '9999-12-31'::DATE
              ORDER BY user_sk, referred_at DESC
            ) AS ut
              ON e.user_sk = ut.user_sk
            JOIN star_schema.dim_date AS dd 
              ON s.sale_date::DATE = dd.date
            WHERE s.update_id IS NULL
              AND s.end_date = '9999-12-31'::DATE;
        """))
    print("   • rebuilt star_schema.fact_sales from all active warehouse.sales\n")

    print(f"[{datetime.datetime.now(pytz.UTC)}] ✅ INCREMENTAL load complete.\n")



if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in ("full", "inc"):
        print(__doc__)
        sys.exit(1)

    if sys.argv[1] == "full":
        run_full_load()
    else:
        if len(sys.argv) < 3:
            print("Error: Please provide batch_id for incremental load")
            print(__doc__)
            sys.exit(1)
        run_incremental_load(int(sys.argv[2]))
