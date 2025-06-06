#!/usr/bin/env python3
# create_star_schema.py
# (Add a new dimension for sales managers and update fact_sales accordingly.)
# ────────────────────────────────────────────────────────────────────────────────

from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, Date, ForeignKey, text
)

# 1) Update this URL with your Postgres credentials / host / port / database
DATABASE_URL = "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"

# 2) Create the engine & ensure 'star_schema' schema exists
engine = create_engine(DATABASE_URL, echo=True)
with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS star_schema"))

# 3) Bind MetaData to the 'star_schema' schema
metadata = MetaData(schema="star_schema")

# ────────────────────────────────────────────────────────────────────────────────

# 4) Dimension: Users
dim_user = Table(
    "dim_user", metadata,
    Column("user_key",    Integer, primary_key=True, autoincrement=True),
    Column("user_id",     Integer, nullable=False, unique=True),
    Column("first_name",  String(100)),
    Column("last_name",   String(100)),
    Column("email",       String(255)),
    Column("signup_date", Date),

    # ── ADDED: country column in dim_user ───────────────────────────────────────
    Column("country",     String(100))                                              # ← ADDED
)

# 5) Dimension: Courses
dim_course = Table(
    "dim_course", metadata,
    Column("course_key",       Integer, primary_key=True, autoincrement=True),
    Column("course_id",        Integer, nullable=False, unique=True),
    Column("title",            String(255)),
    Column("subject",          String(100)),
    Column("price_in_rubbles", Integer),

    # These two were already present:
    Column("category",         String(100)),
    Column("sub_category",     String(100)),
)

# 6) Dimension: Traffic Sources
dim_traffic_source = Table(
    "dim_traffic_source", metadata,
    Column("traffic_source_key", Integer, primary_key=True, autoincrement=True),
    Column("traffic_source_id",  Integer, nullable=False, unique=True),
    Column("name",               String(100)),
    Column("channel",            String(100)),
)

# 7) Dimension: Sales Managers  ← NEW
dim_sales_manager = Table(
    "dim_sales_manager", metadata,
    Column("sales_manager_key", Integer, primary_key=True, autoincrement=True),
    Column("manager_id",        Integer, nullable=False, unique=True),
    Column("first_name",        String(100)),
    Column("last_name",         String(100)),
    Column("email",             String(255)),
    Column("hired_at",          Date),
)

# 8) Dimension: Date
dim_date = Table(
    "dim_date", metadata,
    Column("date_key", Integer, primary_key=True, autoincrement=True),
    Column("date",     Date,    nullable=False, unique=True),
    Column("year",     Integer),
    Column("quarter",  Integer),
    Column("month",    Integer),
    Column("day",      Integer),
    Column("weekday",  Integer),
)

# 9) Fact: Sales
fact_sales = Table(
    "fact_sales", metadata,
    Column("sale_key",           Integer, primary_key=True, autoincrement=True),
    Column("sale_id",            Integer, nullable=False, unique=True),

    # FK → dim_user
    Column(
        "user_key",
        Integer,
        ForeignKey("star_schema.dim_user.user_key", ondelete="RESTRICT"),
        nullable=False
    ),

    # FK → dim_course
    Column(
        "course_key",
        Integer,
        ForeignKey("star_schema.dim_course.course_key", ondelete="RESTRICT"),
        nullable=False
    ),

    # FK → dim_sales_manager   (NEW)
    Column(
        "sales_manager_key",
        Integer,
        ForeignKey("star_schema.dim_sales_manager.sales_manager_key", ondelete="RESTRICT"),
        nullable=False
    ),

    # FK → dim_traffic_source
    Column(
        "traffic_source_key",
        Integer,
        ForeignKey("star_schema.dim_traffic_source.traffic_source_key", ondelete="RESTRICT"),
        nullable=False
    ),

    # FK → dim_date
    Column(
        "date_key",
        Integer,
        ForeignKey("star_schema.dim_date.date_key", ondelete="RESTRICT"),
        nullable=False
    ),

    Column("total_in_rubbles", Integer),
    Column("enrollment_count",  Integer, nullable=False, server_default=text("1")),
)

# 10) Execute CREATE TABLE for all DDL above
if __name__ == "__main__":
    metadata.create_all(engine)
    print("✅ Star schema tables created (including dim_sales_manager).")
