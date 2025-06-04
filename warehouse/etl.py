#!/usr/bin/env python3

"""
etl.py

Usage:
  # Full initial load (batch_id=1)
  python etl.py full

  # Incremental load (pass the next batch_id, e.g. 2, 3, …)
  python etl.py inc 2
"""

import sys
import datetime
import time
from sqlalchemy import create_engine, text

# ───────────── Configuration ───────────────────────────────────────────────────
DB_URL   = "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"
SRC_MAIN = 1   # arbitrary source_id for audit (e.g. “the main source is ID=1”)

# ───────────── Engines ───────────────────────────────────────────────────────────
source_engine    = create_engine(DB_URL, echo=False)
warehouse_engine = create_engine(DB_URL, echo=False)


def run_full_load():
    """
    This does a “FULL” load:
      1) TRUNCATE everything in warehouse.* + star_schema.*
      2) Load all warehouse tables from source.*
      3) Build ALL star_schema dims: dim_user, dim_course, dim_traffic_source, dim_sales_manager, dim_date
      4) Populate star_schema.fact_sales via the correct FK joins
    """
    batch_id = 1
    # Always use a timezone-aware UTC timestamp in logs
    now_ts = datetime.datetime.now(datetime.timezone.utc)
    print(f"[{now_ts}] ▶️  Starting FULL load (batch_id={batch_id})\n")

    # ─── 1) TRUNCATE all warehouse.*/star_schema.* ───────────────────────────────────
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

    # ─── 2) Load ALL “warehouse” tables from “source” ─────────────────────────────────
    with warehouse_engine.begin() as conn:
        # 2.1) USERS → warehouse.users
        conn.execute(text("""
            INSERT INTO warehouse.users
              (user_id, first_name, last_name, email, phone, registered_at,
               start_date, end_date, source_id, insert_id, update_id)
            SELECT
              u.user_id,
              u.first_name,
              u.last_name,
              u.email,
              u.phone,
              u.registered_at,
              NOW()               AS start_date,
              '9999-12-31'::DATE  AS end_date,
              :src_main           AS source_id,
              :batch              AS insert_id,
              NULL                AS update_id
            FROM source.users AS u;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
        print("   • loaded warehouse.users")

        # 2.2) SALES_MANAGERS → warehouse.sales_managers
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

        # 2.3) COURSES → warehouse.courses
        conn.execute(text("""
            INSERT INTO warehouse.courses
              (course_id, title, subject, description, price_in_rubbles, created_at,
               start_date, end_date, source_id, insert_id, update_id)
            SELECT
              c.course_id,
              c.title,
              c.subject,
              c.description,
              c.price_in_rubbles,
              c.created_at,
              NOW()               AS start_date,
              '9999-12-31'::DATE  AS end_date,
              :src_main           AS source_id,
              :batch              AS insert_id,
              NULL                AS update_id
            FROM source.courses AS c;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
        print("   • loaded warehouse.courses")

        # 2.4) ENROLLMENTS → warehouse.enrollments
        #     We look up the current user_sk & course_sk from active rows (end_date = '9999-12-31')
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

        # 2.5) SALES → warehouse.sales
        #     We look up enrollment_sk & sales_manager_sk from active rows
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

        # 2.6) TRAFFIC_SOURCES → warehouse.traffic_sources
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

        # 2.7) USER_TRAFFIC → warehouse.user_traffic
        #     Join on user_sk & traffic_source_sk
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
        print("   • loaded warehouse.user_traffic\n")

    # ─── 3) Build “star_schema” dims ───────────────────────────────────────────────────
    with warehouse_engine.begin() as conn:
        # 3.1) dim_user
        conn.execute(text("""
            DELETE FROM star_schema.dim_user;
            INSERT INTO star_schema.dim_user
              (user_id, first_name, last_name, email, signup_date)
            SELECT
              w.user_id,
              w.first_name,
              w.last_name,
              w.email,
              w.registered_at::DATE AS signup_date
            FROM warehouse.users AS w
            WHERE w.end_date = '9999-12-31'::DATE;
        """))
        print("   • loaded star_schema.dim_user")

        # 3.2) dim_course
        conn.execute(text("DELETE FROM star_schema.dim_course;"))
        conn.execute(text("""
            INSERT INTO star_schema.dim_course
              (course_id, title, subject, price_in_rubbles)
            SELECT
              c.course_id,
              c.title,
              c.subject,
              c.price_in_rubbles
            FROM warehouse.courses AS c
            WHERE c.end_date = '9999-12-31'::DATE;
        """))
        rows_loaded = conn.execute(text("""
            SELECT COUNT(*) 
            FROM warehouse.courses
            WHERE end_date = '9999-12-31'::DATE;
        """)).scalar_one()
        print(f"   • loaded star_schema.dim_course ({rows_loaded} rows)")

        # 3.3) dim_traffic_source
        conn.execute(text("""
            DELETE FROM star_schema.dim_traffic_source;
            INSERT INTO star_schema.dim_traffic_source
              (traffic_source_id, name, channel)
            SELECT
              t.source_id      AS traffic_source_id,
              t.name,
              t.channel
            FROM warehouse.traffic_sources AS t
            WHERE t.end_date = '9999-12-31'::DATE;
        """))
        print("   • loaded star_schema.dim_traffic_source")

        # 3.4) dim_sales_manager
        conn.execute(text("""
            DELETE FROM star_schema.dim_sales_manager;
            INSERT INTO star_schema.dim_sales_manager
              (manager_id, first_name, last_name, email, hired_at)
            SELECT
              m.manager_id,
              m.first_name,
              m.last_name,
              m.email,
              m.hired_at
            FROM warehouse.sales_managers AS m
            WHERE m.end_date = '9999-12-31'::DATE;
        """))
        print("   • loaded star_schema.dim_sales_manager")

        # 3.5) dim_date
        #     If there are any sales at all, grab MIN/MAX dates from warehouse.sales
        conn.execute(text("DELETE FROM star_schema.dim_date;"))
        result = conn.execute(text("""
            SELECT MIN(sale_date)::DATE AS min_d, MAX(sale_date)::DATE AS max_d
            FROM warehouse.sales;
        """)).mappings().one()

        min_date = result["min_d"]
        max_date = result["max_d"]
        if min_date is not None and max_date is not None:
            conn.execute(text(f"""
                INSERT INTO star_schema.dim_date
                  (date, year, quarter, month, day, weekday)
                SELECT
                  gs::DATE            AS date,
                  EXTRACT(YEAR FROM gs)::INT   AS year,
                  EXTRACT(QUARTER FROM gs)::INT AS quarter,
                  EXTRACT(MONTH FROM gs)::INT   AS month,
                  EXTRACT(DAY FROM gs)::INT     AS day,
                  EXTRACT(DOW FROM gs)::INT     AS weekday
                FROM generate_series(
                  '{min_date}'::DATE,
                  '{max_date}'::DATE,
                  '1 day'::INTERVAL
                ) AS gs;
            """))
            total_days = (max_date - min_date).days + 1
            print(f"   • loaded star_schema.dim_date ({total_days} days)")
        else:
            print("   • no sales dates found; skipped dim_date load")

    # ─── 4) Populate star_schema.fact_sales ───────────────────────────────────────────
    with warehouse_engine.begin() as conn:
        # 4.1) Empty fact_sales
        conn.execute(text("DELETE FROM star_schema.fact_sales;"))
        print("   • cleared star_schema.fact_sales")

        # 4.2) INSERT into fact_sales, joining to the correct dim tables (user, course,
        #      sales_manager, traffic_source, date)
        conn.execute(text("""
            INSERT INTO star_schema.fact_sales
              (
                sale_id,
                user_key,
                course_key,
                sales_manager_key,
                traffic_source_key,
                date_key,
                total_in_rubbles,
                enrollment_count
              )
            SELECT
              s.sale_id,
              du.user_key,
              dc.course_key,
              dsm.sales_manager_key,
              dts.traffic_source_key,
              dd.date_key,
              s.cost_in_rubbles   AS total_in_rubbles,
              1                   AS enrollment_count
            FROM warehouse.sales AS s
            JOIN warehouse.enrollments          AS e   ON s.enrollment_sk     = e.enrollment_sk
            JOIN warehouse.user_traffic         AS ut  ON e.user_sk          = ut.user_sk
                                                  AND ut.end_date       = '9999-12-31'::DATE
            JOIN star_schema.dim_user           AS du  ON e.user_sk          = du.user_key
            JOIN star_schema.dim_course         AS dc  ON e.course_sk        = dc.course_key
            JOIN star_schema.dim_sales_manager  AS dsm ON s.sales_manager_sk = dsm.sales_manager_key
            JOIN star_schema.dim_traffic_source AS dts ON ut.traffic_source_sk = dts.traffic_source_key
            JOIN star_schema.dim_date           AS dd  ON s.sale_date::DATE  = dd.date;
        """))
        print("   • loaded star_schema.fact_sales\n")

    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] ✅ FULL load complete.\n")


def run_incremental_load(batch_id: int):
    """
    This does an “INCREMENTAL” load:
      1) Run your SCD2 logic on warehouse.* (insert new rows, version out old rows)
      2) TRUNCATE & rebuild star_schema dims
      3) Truncate & repopulate star_schema.fact_sales (using the correct dim joins)
    """
    now_ts = datetime.datetime.now(datetime.timezone.utc)
    print(f"[{now_ts}] ▶️  Starting INCREMENTAL load (batch_id={batch_id})\n")

    # ─── 1) SCD2 logic for warehouse.* ────────────────────────────────────────────────
    with warehouse_engine.begin() as conn:
        # 1.1) USERS ─────────────────────────────────────────────────────────────────────
        # 1.1.a) Insert brand‐new users
        conn.execute(text("""
            INSERT INTO warehouse.users (
                user_id, first_name, last_name, email, phone, registered_at,
                start_date, end_date, source_id, insert_id, update_id
            )
            SELECT
                s.user_id,
                s.first_name,
                s.last_name,
                s.email,
                s.phone,
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

        # 1.1.b) For changed users, insert a new “active” row (update_id = NULL)
        conn.execute(text("""
            INSERT INTO warehouse.users (
                user_id, first_name, last_name, email, phone, registered_at,
                start_date, end_date, source_id, insert_id, update_id
            )
            SELECT
                s.user_id,
                s.first_name,
                s.last_name,
                s.email,
                s.phone,
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
              OR  s.registered_at IS DISTINCT FROM w.registered_at;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
        print("   • inserted new versions for changed warehouse.users")

        # 1.1.c) Close out old active user rows
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
                OR  s.registered_at IS DISTINCT FROM w.registered_at
              );
        """), {"batch": batch_id})
        print("   • closed out old versions of warehouse.users\n")


        # 1.2) SALES_MANAGERS ─────────────────────────────────────────────────────────────
        # 1.2.a) Insert brand‐new sales_managers
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

        # 1.2.b) For changed sales_managers, insert new “active” row
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

        # 1.2.c) Close out old active sales_managers
        conn.execute(text("""
            UPDATE warehouse.sales_managers AS w
               SET end_date  = NOW(),
                   update_id = :batch
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


        # 1.3) COURSES ─────────────────────────────────────────────────────────────────────
        # 1.3.a) Insert brand‐new courses
        conn.execute(text("""
            INSERT INTO warehouse.courses (
                course_id, title, subject, description, price_in_rubbles, created_at,
                start_date, end_date, source_id, insert_id, update_id
            )
            SELECT
                s.course_id,
                s.title,
                s.subject,
                s.description,
                s.price_in_rubbles,
                s.created_at,
                NOW()               AS start_date,
                '9999-12-31'::DATE  AS end_date,
                :src_main           AS source_id,
                :batch              AS insert_id,
                NULL                AS update_id
            FROM source.courses AS s
            LEFT JOIN warehouse.courses AS w
              ON s.course_id = w.course_id
                 AND w.end_date = '9999-12-31'::DATE
            WHERE w.course_id IS NULL;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
        print("   • inserted new warehouse.courses")

        # 1.3.b) For changed courses, insert new “active” row
        conn.execute(text("""
            INSERT INTO warehouse.courses (
                course_id, title, subject, description, price_in_rubbles, created_at,
                start_date, end_date, source_id, insert_id, update_id
            )
            SELECT
                s.course_id,
                s.title,
                s.subject,
                s.description,
                s.price_in_rubbles,
                s.created_at,
                NOW()               AS start_date,
                '9999-12-31'::DATE  AS end_date,
                :src_main           AS source_id,
                :batch              AS insert_id,
                NULL                AS update_id
            FROM source.courses AS s
            JOIN warehouse.courses AS w
              ON s.course_id = w.course_id
             AND w.end_date = '9999-12-31'::DATE
            WHERE 
                  s.title             IS DISTINCT FROM w.title
              OR  s.subject           IS DISTINCT FROM w.subject
              OR  COALESCE(s.description, '') <> COALESCE(w.description, '')
              OR  s.price_in_rubbles  IS DISTINCT FROM w.price_in_rubbles
              OR  s.created_at        IS DISTINCT FROM w.created_at;
        """), {"src_main": SRC_MAIN, "batch": batch_id})
        print("   • inserted new versions for changed warehouse.courses")

        # 1.3.c) Close out old active courses
        conn.execute(text("""
            UPDATE warehouse.courses AS w
               SET end_date  = NOW(),
                   update_id = :batch
            FROM source.courses AS s
            WHERE w.course_id = s.course_id
              AND w.end_date = '9999-12-31'::DATE
              AND (
                    s.title             IS DISTINCT FROM w.title
                OR  s.subject           IS DISTINCT FROM w.subject
                OR  COALESCE(s.description, '') <> COALESCE(w.description, '')
                OR  s.price_in_rubbles  IS DISTINCT FROM w.price_in_rubbles
                OR  s.created_at        IS DISTINCT FROM w.created_at
              );
        """), {"batch": batch_id})
        print("   • closed out old versions of warehouse.courses\n")


        # 1.4) ENROLLMENTS ────────────────────────────────────────────────────────────────
        # 1.4.a) Insert brand‐new enrollments (use user_sk & course_sk)
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

        # 1.4.b) For changed enrollments, insert new “active” row
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

        # 1.4.c) Close out old active enrollments
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


        # 1.5) SALES ───────────────────────────────────────────────────────────────────────
        # 1.5.a) Insert brand‐new sales (use enrollment_sk & sales_manager_sk)
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

        # 1.5.b) For changed sales, insert new “active” row
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

        # 1.5.c) Close out old active sales
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


        # 1.6) TRAFFIC_SOURCES ─────────────────────────────────────────────────────────────
        # 1.6.a) Insert brand‐new traffic_sources
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

        # 1.6.b) For changed traffic_sources, insert new “active” row
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
              OR  COALESCE(s.details, '') <> COALESCE(w.details, '');
        """), {"src_main": SRC_MAIN, "batch": batch_id})
        print("   • inserted new versions for changed warehouse.traffic_sources")

        # 1.6.c) Close out old active traffic_sources
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
                OR  COALESCE(s.details, '') <> COALESCE(w.details, '')
              );
        """), {"batch": batch_id})
        print("   • closed out old versions of warehouse.traffic_sources\n")


        # 1.7) USER_TRAFFIC ───────────────────────────────────────────────────────────────
        # 1.7.a) Insert brand‐new user_traffic
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
        print("   • inserted new warehouse.user_traffic")

        # 1.7.b) For changed user_traffic, insert new “active” row
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
        print("   • inserted new versions for changed warehouse.user_traffic")

        # 1.7.c) Close out old active user_traffic
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
        print("   • closed out old versions of warehouse.user_traffic\n")

    # ─── 2) Rebuild ALL “star_schema” dims ─────────────────────────────────────────────
    with warehouse_engine.begin() as conn:
        # 2.1) dim_user
        conn.execute(text("""
            DELETE FROM star_schema.dim_user;
            INSERT INTO star_schema.dim_user
              (user_id, first_name, last_name, email, signup_date)
            SELECT
              w.user_id,
              w.first_name,
              w.last_name,
              w.email,
              w.registered_at::DATE AS signup_date
            FROM warehouse.users AS w
            WHERE w.end_date = '9999-12-31'::DATE;
        """))
        print("   • loaded star_schema.dim_user")

        # 2.2) dim_course
        conn.execute(text("DELETE FROM star_schema.dim_course;"))
        conn.execute(text("""
            INSERT INTO star_schema.dim_course
              (course_id, title, subject, price_in_rubbles)
            SELECT
              c.course_id,
              c.title,
              c.subject,
              c.price_in_rubbles
            FROM warehouse.courses AS c
            WHERE c.end_date = '9999-12-31'::DATE;
        """))
        rows_loaded = conn.execute(text("""
            SELECT COUNT(*)
            FROM warehouse.courses
            WHERE end_date = '9999-12-31'::DATE;
        """)).scalar_one()
        print(f"   • loaded star_schema.dim_course ({rows_loaded} rows)")

        # 2.3) dim_traffic_source
        conn.execute(text("""
            DELETE FROM star_schema.dim_traffic_source;
            INSERT INTO star_schema.dim_traffic_source
              (traffic_source_id, name, channel)
            SELECT
              t.source_id      AS traffic_source_id,
              t.name,
              t.channel
            FROM warehouse.traffic_sources AS t
            WHERE t.end_date = '9999-12-31'::DATE;
        """))
        print("   • loaded star_schema.dim_traffic_source")

        # 2.4) dim_sales_manager
        conn.execute(text("""
            DELETE FROM star_schema.dim_sales_manager;
            INSERT INTO star_schema.dim_sales_manager
              (manager_id, first_name, last_name, email, hired_at)
            SELECT
              m.manager_id,
              m.first_name,
              m.last_name,
              m.email,
              m.hired_at
            FROM warehouse.sales_managers AS m
            WHERE m.end_date = '9999-12-31'::DATE;
        """))
        print("   • loaded star_schema.dim_sales_manager")

        # 2.5) dim_date
        conn.execute(text("DELETE FROM star_schema.dim_date;"))
        result = conn.execute(text("""
            SELECT MIN(sale_date)::DATE AS min_d, MAX(sale_date)::DATE AS max_d
            FROM warehouse.sales;
        """)).mappings().one()
        min_date = result["min_d"]
        max_date = result["max_d"]
        if min_date is not None and max_date is not None:
            conn.execute(text(f"""
                INSERT INTO star_schema.dim_date
                  (date, year, quarter, month, day, weekday)
                SELECT
                  gs::DATE AS date,
                  EXTRACT(YEAR FROM gs)::INT   AS year,
                  EXTRACT(QUARTER FROM gs)::INT AS quarter,
                  EXTRACT(MONTH FROM gs)::INT   AS month,
                  EXTRACT(DAY FROM gs)::INT     AS day,
                  EXTRACT(DOW FROM gs)::INT     AS weekday
                FROM generate_series(
                  '{min_date}'::DATE,
                  '{max_date}'::DATE,
                  '1 day'::INTERVAL
                ) AS gs;
            """))
            total_days = (max_date - min_date).days + 1
            print(f"   • loaded star_schema.dim_date ({total_days} rows)")
        else:
            print("   • no sales dates found; skipped dim_date load")

    # ─── 3) Re‐populate star_schema.fact_sales ────────────────────────────────────────
    with warehouse_engine.begin() as conn:
        conn.execute(text("DELETE FROM star_schema.fact_sales;"))
        print("   • cleared star_schema.fact_sales")

        # 3.1) Use the correct join to dim_sales_manager + dim_traffic_source
        conn.execute(text("""
            INSERT INTO star_schema.fact_sales
              (
                sale_id,
                user_key,
                course_key,
                sales_manager_key,
                traffic_source_key,
                date_key,
                total_in_rubbles,
                enrollment_count
              )
            SELECT
              s.sale_id,
              du.user_key,
              dc.course_key,
              dsm.sales_manager_key,
              dts.traffic_source_key,
              dd.date_key,
              s.cost_in_rubbles   AS total_in_rubbles,
              1                   AS enrollment_count
            FROM warehouse.sales AS s
            JOIN warehouse.enrollments          AS e   ON s.enrollment_sk     = e.enrollment_sk
            JOIN warehouse.user_traffic         AS ut  ON e.user_sk          = ut.user_sk
                                                  AND ut.end_date       = '9999-12-31'::DATE
            JOIN star_schema.dim_user           AS du  ON e.user_sk          = du.user_key
            JOIN star_schema.dim_course         AS dc  ON e.course_sk        = dc.course_key
            JOIN star_schema.dim_sales_manager  AS dsm ON s.sales_manager_sk = dsm.sales_manager_key
            JOIN star_schema.dim_traffic_source AS dts ON ut.traffic_source_sk = dts.traffic_source_key
            JOIN star_schema.dim_date           AS dd  ON s.sale_date::DATE  = dd.date;
        """))
        print("   • reloaded star_schema.fact_sales\n")

    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] ✅ INCREMENTAL load complete.\n")


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in ("full", "inc"):
        print(__doc__)
        sys.exit(1)

    if sys.argv[1] == "full":
        run_full_load()
    else:
        run_incremental_load(int(sys.argv[2]))
