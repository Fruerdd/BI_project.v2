#!/usr/bin/env python3
# create_source_tables.py

"""
This script mirrors the “warehouse” schema structure into a “source” schema.
All table/column names, data types, and constraints are kept identical,
but point to “source” instead of “warehouse” so we can ingest raw data
before any transformations.

References:
- SQLAlchemy Table and Column docs:
  https://docs.sqlalchemy.org/en/14/core/metadata.html#sqlalchemy.schema.Table
- SQLAlchemy ForeignKey docs:
  https://docs.sqlalchemy.org/en/14/core/constraints.html#sqlalchemy.schema.ForeignKey
"""

from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Text,
    DateTime,
    ForeignKey,
    CheckConstraint,
    PrimaryKeyConstraint,
    text
)
import datetime

# ───────────── 1) DATABASE URL ─────────────────────────────────────────────────
DATABASE_URL = "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"

# ───────────── 2) CREATE ENGINE & ENSURE “source” SCHEMA ───────────────────────
engine = create_engine(DATABASE_URL, echo=True)

with engine.begin() as conn:
    # If the “source” schema does not exist, create it
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS source"))

# ───────────── 3) ATTACH METADATA TO “source” SCHEMA ──────────────────────────
metadata = MetaData(schema="source")

# ───────────── 4) DEFINE SOURCE TABLES (mirror of warehouse) ──────────────────

# 4.1) users table in source
source_users = Table(
    "users", metadata,
    Column("user_id",       Integer, primary_key=True),
    Column("first_name",    String(100),  nullable=False),
    Column("last_name",     String(100),  nullable=False),
    Column("email",         String(255),  nullable=False, unique=True),
    Column("phone",         String(20)),
    Column("registered_at", DateTime,     nullable=False, default=datetime.datetime.utcnow),
)

# 4.2) sales_managers table in source
source_sales_managers = Table(
    "sales_managers", metadata,
    Column("manager_id", Integer, primary_key=True),
    Column("first_name", String(100), nullable=False),
    Column("last_name",  String(100), nullable=False),
    Column("email",      String(255), nullable=False, unique=True),
    Column("hired_at",   DateTime,    nullable=False),
)

# 4.3) courses table in source
source_courses = Table(
    "courses", metadata,
    Column("course_id",   Integer, primary_key=True),
    Column("title",       String(255), nullable=False),
    Column("subject",     String(100), nullable=False),
    Column("description", Text),
    Column("price_in_rubbles", Integer, nullable=False),
    Column("created_at",  DateTime, nullable=False, default=datetime.datetime.utcnow),
    CheckConstraint("price_in_rubbles >= 0", name="price_in_rubbles_non_negative"),
)

# 4.4) enrollments table in source
source_enrollments = Table(
    "enrollments", metadata,
    Column("enrollment_id", Integer, primary_key=True),
    # Note: ForeignKey points to source.users.user_id, not warehouse
    Column("user_id",       Integer, ForeignKey("source.users.user_id"),   nullable=False),
    Column("course_id",     Integer, ForeignKey("source.courses.course_id"), nullable=False),
    Column("enrolled_at",   DateTime, nullable=False, default=datetime.datetime.utcnow),
    Column("status", String(20), nullable=False),
    CheckConstraint(
        "status IN ('active','completed','cancelled')",
        name="valid_status"
    ),
)

# 4.5) sales table in source
source_sales = Table(
    "sales", metadata,
    Column("sale_id",       Integer, primary_key=True),
    Column("enrollment_id", Integer, ForeignKey("source.enrollments.enrollment_id"), nullable=False, unique=True),
    Column("manager_id",    Integer, nullable=False),
    Column("sale_date",     DateTime, nullable=False, default=datetime.datetime.utcnow),
    Column("cost_in_rubbles", Integer,  nullable=False),
    CheckConstraint("cost_in_rubbles >= 0", name="cost_in_rubbles_non_negative"),
)

# 4.6) traffic_sources table in source
source_traffic_sources = Table(
    "traffic_sources", metadata,
    Column("source_id", Integer, primary_key=True),
    Column("name",      String(100), nullable=False),
    Column("channel",   String(100), nullable=False),
    Column("details",   Text),
)

# 4.7) user_traffic table in source
source_user_traffic = Table(
    "user_traffic", metadata,
    Column("user_id",       Integer, ForeignKey("source.users.user_id"),   nullable=False),
    Column("source_id",     Integer, ForeignKey("source.traffic_sources.source_id"), nullable=False),
    Column("referred_at",   DateTime, nullable=False, default=datetime.datetime.utcnow),
    Column("campaign_code", String(50)),
    PrimaryKeyConstraint("user_id", "source_id", "referred_at", name="pk_user_traffic"),
)

# ───────────── 5) CREATE ALL SOURCE TABLES AT ONCE ─────────────────────────────
if __name__ == "__main__":
    metadata.create_all(engine)
    print("✅ All source tables created successfully.")
