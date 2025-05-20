from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, Text, DateTime,
    ForeignKey, CheckConstraint, PrimaryKeyConstraint
)
from sqlalchemy.sql import text
import datetime

# 1) Configure your connection URL
DATABASE_URL = "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"

# 2) Create engine and attach metadata with schema='stage'
engine = create_engine(DATABASE_URL, echo=True)
metadata = MetaData(schema="stage")

# 3) Ensure the 'stage' schema exists
with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS stage"))

# 4) Define staging tables

# stage.users
users = Table(
    "users", metadata,
    Column("user_id",       Integer, primary_key=True),
    Column("first_name",    String(100),  nullable=False),
    Column("last_name",     String(100),  nullable=False),
    Column("email",         String(255),  nullable=False, unique=True),
    Column("phone",         String(20)),
    Column("registered_at", DateTime,     nullable=False,
           default=datetime.datetime.utcnow),
)

# stage.courses
courses = Table(
    "courses", metadata,
    Column("course_id",   Integer,      primary_key=True),
    Column("title",       String(255),  nullable=False),
    Column("subject",     String(100),  nullable=False),
    Column("description", Text),
    Column("price_in_rubbles", Integer,      nullable=False),
    Column("created_at",  DateTime,     nullable=False,
           default=datetime.datetime.utcnow),
    CheckConstraint("price_cents >= 0", name="price_non_negative"),
)

# stage.enrollments
enrollments = Table(
    "enrollments", metadata,
    Column("enrollment_id", Integer,     primary_key=True),
    Column("user_id",       Integer,     ForeignKey("stage.users.user_id"),   nullable=False),
    Column("course_id",     Integer,     ForeignKey("stage.courses.course_id"), nullable=False),
    Column("enrolled_at",   DateTime,    nullable=False, default=datetime.datetime.utcnow),
    Column("status",        String(20),  nullable=False),
    CheckConstraint(
        "status IN ('active','completed','cancelled')",
        name="valid_status"
    ),
)

# stage.sales
sales = Table(
    "sales", metadata,
    Column("sale_id",       Integer,     primary_key=True),
    Column("enrollment_id", Integer,     ForeignKey("stage.enrollments.enrollment_id"), nullable=False, unique=True),
    Column("manager_id",    Integer,     nullable=False),
    Column("sale_date",     DateTime,    nullable=False, default=datetime.datetime.utcnow),
    Column("cost_in_rubbles",  Integer,     nullable=False),
    CheckConstraint("cost_in_rubbles >= 0", name="amount_non_negative"),
    # you can add: ForeignKey("stage.sales_managers.manager_id") if you stage managers
)

# stage.traffic_sources
traffic_sources = Table(
    "traffic_sources", metadata,
    Column("source_id", Integer, primary_key=True),
    Column("name",      String(100), nullable=False),
    Column("channel",   String(100), nullable=False),
    Column("details",   Text),
)

# stage.user_traffic (composite PK)
user_traffic = Table(
    "user_traffic", metadata,
    Column("user_id",     Integer, ForeignKey("stage.users.user_id"),   nullable=False),
    Column("source_id",   Integer, ForeignKey("stage.traffic_sources.source_id"), nullable=False),
    Column("referred_at", DateTime,                nullable=False, default=datetime.datetime.utcnow),
    Column("campaign_code", String(50)),
    PrimaryKeyConstraint("user_id", "source_id", "referred_at", name="pk_user_traffic")
)

# optional: stage.course_categories (your CSV)
course_categories = Table(
    "course_categories", metadata,
    Column("course_id",    Integer, ForeignKey("stage.courses.course_id"), primary_key=True),
    Column("category",     String(100),  nullable=False),
    Column("sub_category", String(100)),
)

# 5) Issue CREATE TABLE for all staging tables at once
metadata.create_all(engine)

print("✅ All staging tables created under schema 'stage'.")
