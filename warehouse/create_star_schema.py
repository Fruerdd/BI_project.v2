#!/usr/bin/env python3
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, Date, DateTime, ForeignKey, text
)

# 1) Update this URL with your Postgres credentials / host / port / database
DATABASE_URL = "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"

# 2) Create the engine & ensure 'warehouse' schema exists
engine = create_engine(DATABASE_URL, echo=True)
with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS warehouse"))

# 3) Bind MetaData to the 'warehouse' schema
metadata = MetaData(schema="warehouse")

# 4) Dimension: Users with SCD/audit columns
dim_user = Table(
    "dim_user", metadata,
    Column("user_key",    Integer, primary_key=True, autoincrement=True),
    Column("user_id",     Integer, nullable=False, unique=True),
    Column("first_name",  String(100)),
    Column("last_name",   String(100)),
    Column("email",       String(255)),
    Column("signup_date", Date),

    # ── SCD / Audit columns ──────────────────────────────────────────────────
    Column("start_date",  DateTime, nullable=False, server_default=text("now()")),
    Column("end_date",    DateTime, nullable=True),
    Column("insert_id",   Integer,  nullable=False),
    Column("update_id",   Integer,  nullable=True),
    Column("source_id",   Integer,  nullable=False),
    # ────────────────────────────────────────────────────────────────────────
)

# 5) Dimension: Courses with SCD/audit columns
dim_course = Table(
    "dim_course", metadata,
    Column("course_key",   Integer, primary_key=True, autoincrement=True),
    Column("course_id",    Integer, nullable=False, unique=True),
    Column("title",        String(255)),
    Column("subject",      String(100)),
    Column("price_in_rubbles",  Integer),
    Column("category",     String(100)),  # from your CSV
    Column("sub_category", String(100)),

    Column("start_date",  DateTime, nullable=False, server_default=text("now()")),
    Column("end_date",    DateTime, nullable=True),
    Column("insert_id",   Integer,  nullable=False),
    Column("update_id",   Integer,  nullable=True),
    Column("source_id",   Integer,  nullable=False),
)

# 6) Dimension: Traffic Sources with SCD/audit columns
#    Note: we rename the business PK to `traffic_source_id` to avoid conflict with the audit `source_id`
dim_traffic = Table(
    "dim_traffic_source", metadata,
    Column("traffic_source_key", Integer, primary_key=True, autoincrement=True),
    Column("traffic_source_id",  Integer, nullable=False, unique=True),
    Column("name",               String(100)),
    Column("channel",            String(100)),

    Column("start_date",  DateTime, nullable=False, server_default=text("now()")),
    Column("end_date",    DateTime, nullable=True),
    Column("insert_id",   Integer,  nullable=False),
    Column("update_id",   Integer,  nullable=True),
    Column("source_id",   Integer,  nullable=False),
)

# 7) Dimension: Date with SCD/audit columns
dim_date = Table(
    "dim_date", metadata,
    Column("date_key",  Integer, primary_key=True, autoincrement=True),
    Column("date",      Date,    nullable=False, unique=True),
    Column("year",      Integer),
    Column("quarter",   Integer),
    Column("month",     Integer),
    Column("day",       Integer),
    Column("weekday",   Integer),

    Column("start_date",  DateTime, nullable=False, server_default=text("now()")),
    Column("end_date",    DateTime, nullable=True),
    Column("insert_id",   Integer,  nullable=False),
    Column("update_id",   Integer,  nullable=True),
    Column("source_id",   Integer,  nullable=False),
)

# 8) Fact: Sales / Enrollments
#    (no SCD columns on facts; you can add `insert_id` here if you wish)
fact_sales = Table(
    "fact_sales", metadata,
    Column("sale_key",        Integer, primary_key=True, autoincrement=True),
    Column("sale_id",         Integer, nullable=False, unique=True),
    Column("user_key",        Integer, ForeignKey("warehouse.dim_user.user_key"),         nullable=False),
    Column("course_key",      Integer, ForeignKey("warehouse.dim_course.course_key"),       nullable=False),
    Column("traffic_source_key", Integer, ForeignKey("warehouse.dim_traffic_source.traffic_source_key"), nullable=False),
    Column("date_key",        Integer, ForeignKey("warehouse.dim_date.date_key"),           nullable=False),
    Column("total_in_rubbles",    Integer),
    Column("enrollment_count", Integer, nullable=False, default=1),
)

# 9) Execute CREATE TABLE for all DDL above
metadata.create_all(engine)
print("✅ Star schema (with SCD/audit) created in 'warehouse' schema.")
