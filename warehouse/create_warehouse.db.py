#!/usr/bin/env python3
# create_warehouse_tables.py

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
    text
)
import datetime

# ───────────── 1) DATABASE URL ─────────────────────────────────────────────────
DATABASE_URL = "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"

# ───────────── 2) CREATE ENGINE & ENSURE SCHEMA ─────────────────────────────────
engine = create_engine(DATABASE_URL, echo=True)
with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS warehouse"))

# ───────────── 3) ATTACH METADATA TO `warehouse` SCHEMA ──────────────────────────
metadata = MetaData(schema="warehouse")

# ───────────── 4) DEFINE WAREHOUSE TABLES (ADD surrogate PK + corrected FKs) ─────

# 4.1) users table in warehouse
warehouse_users = Table(
    "users", metadata,
    Column("user_sk",       Integer, primary_key=True, autoincrement=True),  # surrogate PK
    Column("user_id",       Integer, nullable=False),                        # business key
    Column("first_name",    String(100),  nullable=False),
    Column("last_name",     String(100),  nullable=False),
    Column("email",         String(255),  nullable=False),
    Column("phone",         String(20)),
    Column("registered_at", DateTime,     nullable=False, default=datetime.datetime.utcnow),

    # ── Audit / SCD columns ────────────────────────────────────────────────────
    Column("start_date",   DateTime, nullable=False, server_default=text("NOW()")),
    Column("end_date",     DateTime, nullable=True),
    Column("source_id",    Integer,  nullable=False),
    Column("insert_id",    Integer,  nullable=False),
    Column("update_id",    Integer,  nullable=True),
)

# 4.2) sales_managers table in warehouse
warehouse_sales_managers = Table(
    "sales_managers", metadata,
    Column("sales_manager_sk", Integer, primary_key=True, autoincrement=True),  # surrogate PK
    Column("manager_id",      Integer, nullable=False),                         # business key
    Column("first_name",      String(100), nullable=False),
    Column("last_name",       String(100), nullable=False),
    Column("email",           String(255), nullable=False),
    Column("hired_at",        DateTime,    nullable=False),

    # ── Audit / SCD columns ────────────────────────────────────────────────────
    Column("start_date",   DateTime, nullable=False, server_default=text("NOW()")),
    Column("end_date",     DateTime, nullable=True),
    Column("source_id",    Integer,  nullable=False),
    Column("insert_id",    Integer,  nullable=False),
    Column("update_id",    Integer,  nullable=True),
)

# 4.3) courses table in warehouse
warehouse_courses = Table(
    "courses", metadata,
    Column("course_sk",       Integer, primary_key=True, autoincrement=True),  # surrogate PK
    Column("course_id",       Integer, nullable=False),                        # business key
    Column("title",           String(255), nullable=False),
    Column("subject",         String(100), nullable=False),
    Column("description",     Text),
    Column("price_in_rubbles", Integer, nullable=False),
    Column("created_at",      DateTime, nullable=False, default=datetime.datetime.utcnow),

    CheckConstraint("price_in_rubbles >= 0", name="price_in_rubbles_non_negative"),

    # ── Audit / SCD columns ────────────────────────────────────────────────────
    Column("start_date",   DateTime, nullable=False, server_default=text("NOW()")),
    Column("end_date",     DateTime, nullable=True),
    Column("source_id",    Integer,  nullable=False),
    Column("insert_id",    Integer,  nullable=False),
    Column("update_id",    Integer,  nullable=True),
)

# 4.4) enrollments table in warehouse
warehouse_enrollments = Table(
    "enrollments", metadata,
    Column("enrollment_sk",   Integer, primary_key=True, autoincrement=True),  # surrogate PK
    Column("enrollment_id",   Integer, nullable=False),                        # business key
    Column("user_sk",         Integer, ForeignKey("warehouse.users.user_sk"),   nullable=False),
    Column("course_sk",       Integer, ForeignKey("warehouse.courses.course_sk"), nullable=False),
    Column("enrolled_at",     DateTime, nullable=False, default=datetime.datetime.utcnow),

    Column("status", String(20), nullable=False),
    CheckConstraint(
        "status IN ('active','completed','cancelled')",
        name="valid_status"
    ),

    # ── Audit / SCD columns ────────────────────────────────────────────────────
    Column("start_date",   DateTime, nullable=False, server_default=text("NOW()")),
    Column("end_date",     DateTime, nullable=True),
    Column("source_id",    Integer,  nullable=False),
    Column("insert_id",    Integer,  nullable=False),
    Column("update_id",    Integer,  nullable=True),
)

# 4.5) sales table in warehouse
warehouse_sales = Table(
    "sales", metadata,
    Column("sale_sk",         Integer, primary_key=True, autoincrement=True),  # surrogate PK
    Column("sale_id",         Integer, nullable=False),                        # business key
    Column("enrollment_sk",   Integer, ForeignKey("warehouse.enrollments.enrollment_sk"), nullable=False),
    Column("sales_manager_sk",Integer, ForeignKey("warehouse.sales_managers.sales_manager_sk"), nullable=False),
    Column("sale_date",       DateTime, nullable=False, default=datetime.datetime.utcnow),
    Column("cost_in_rubbles", Integer,  nullable=False),

    CheckConstraint("cost_in_rubbles >= 0", name="cost_in_rubbles_non_negative"),

    # ── Audit / SCD columns ────────────────────────────────────────────────────
    Column("start_date",   DateTime, nullable=False, server_default=text("NOW()")),
    Column("end_date",     DateTime, nullable=True),
    Column("source_id",    Integer,  nullable=False),
    Column("insert_id",    Integer,  nullable=False),
    Column("update_id",    Integer,  nullable=True),
)

# 4.6) traffic_sources table in warehouse
warehouse_traffic_sources = Table(
    "traffic_sources", metadata,
    Column("traffic_source_sk", Integer, primary_key=True, autoincrement=True),  # surrogate PK
    Column("source_id",       Integer, nullable=False),                          # business key
    Column("name",            String(100), nullable=False),
    Column("channel",         String(100), nullable=False),
    Column("details",         Text),

    # ── Audit / SCD columns ────────────────────────────────────────────────────
    Column("start_date",       DateTime, nullable=False, server_default=text("NOW()")),
    Column("end_date",         DateTime, nullable=True),
    Column("source_id_audit",  Integer, nullable=False),
    Column("insert_id",        Integer, nullable=False),
    Column("update_id",        Integer, nullable=True),
)

# 4.7) user_traffic table in warehouse
warehouse_user_traffic = Table(
    "user_traffic", metadata,
    Column("user_traffic_sk", Integer, primary_key=True, autoincrement=True),  # surrogate PK
    Column("user_sk",         Integer, ForeignKey("warehouse.users.user_sk"),   nullable=False),
    Column("traffic_source_sk",Integer, ForeignKey("warehouse.traffic_sources.traffic_source_sk"), nullable=False),
    Column("referred_at",     DateTime, nullable=False, default=datetime.datetime.utcnow),
    Column("campaign_code",   String(50)),

    # ── Audit / SCD columns ────────────────────────────────────────────────────
    Column("start_date",       DateTime, nullable=False, server_default=text("NOW()")),
    Column("end_date",         DateTime, nullable=True),
    Column("source_id_audit",  Integer, nullable=False),
    Column("insert_id",        Integer, nullable=False),
    Column("update_id",        Integer, nullable=True),
)

# ───────────── 5) CREATE ALL TABLES AT ONCE ─────────────────────────────────────
if __name__ == "__main__":
    metadata.create_all(engine)
    print("✅ All warehouse tables created (including sales_managers, surrogate PKs, and corrected FKs).")
#!/usr/bin/env python3
# create_warehouse_tables.py

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
    text
)
import datetime

# ───────────── 1) DATABASE URL ─────────────────────────────────────────────────
DATABASE_URL = "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"

# ───────────── 2) CREATE ENGINE & ENSURE SCHEMA ─────────────────────────────────
engine = create_engine(DATABASE_URL, echo=True)
with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS warehouse"))

# ───────────── 3) ATTACH METADATA TO `warehouse` SCHEMA ──────────────────────────
metadata = MetaData(schema="warehouse")

# ───────────── 4) DEFINE WAREHOUSE TABLES (ADD surrogate PK + corrected FKs) ─────

# 4.1) users table in warehouse
warehouse_users = Table(
    "users", metadata,
    Column("user_sk",       Integer, primary_key=True, autoincrement=True),  # surrogate PK
    Column("user_id",       Integer, nullable=False),                        # business key
    Column("first_name",    String(100),  nullable=False),
    Column("last_name",     String(100),  nullable=False),
    Column("email",         String(255),  nullable=False),
    Column("phone",         String(20)),
    Column("registered_at", DateTime,     nullable=False, default=datetime.datetime.utcnow),

    # ── Audit / SCD columns ────────────────────────────────────────────────────
    Column("start_date",   DateTime, nullable=False, server_default=text("NOW()")),
    Column("end_date",     DateTime, nullable=True),
    Column("source_id",    Integer,  nullable=False),
    Column("insert_id",    Integer,  nullable=False),
    Column("update_id",    Integer,  nullable=True),
)

# 4.2) sales_managers table in warehouse
warehouse_sales_managers = Table(
    "sales_managers", metadata,
    Column("sales_manager_sk", Integer, primary_key=True, autoincrement=True),  # surrogate PK
    Column("manager_id",      Integer, nullable=False),                         # business key
    Column("first_name",      String(100), nullable=False),
    Column("last_name",       String(100), nullable=False),
    Column("email",           String(255), nullable=False),
    Column("hired_at",        DateTime,    nullable=False),

    # ── Audit / SCD columns ────────────────────────────────────────────────────
    Column("start_date",   DateTime, nullable=False, server_default=text("NOW()")),
    Column("end_date",     DateTime, nullable=True),
    Column("source_id",    Integer,  nullable=False),
    Column("insert_id",    Integer,  nullable=False),
    Column("update_id",    Integer,  nullable=True),
)

# 4.3) courses table in warehouse
warehouse_courses = Table(
    "courses", metadata,
    Column("course_sk",       Integer, primary_key=True, autoincrement=True),  # surrogate PK
    Column("course_id",       Integer, nullable=False),                        # business key
    Column("title",           String(255), nullable=False),
    Column("subject",         String(100), nullable=False),
    Column("description",     Text),
    Column("price_in_rubbles", Integer, nullable=False),
    Column("created_at",      DateTime, nullable=False, default=datetime.datetime.utcnow),

    CheckConstraint("price_in_rubbles >= 0", name="price_in_rubbles_non_negative"),

    # ── Audit / SCD columns ────────────────────────────────────────────────────
    Column("start_date",   DateTime, nullable=False, server_default=text("NOW()")),
    Column("end_date",     DateTime, nullable=True),
    Column("source_id",    Integer,  nullable=False),
    Column("insert_id",    Integer,  nullable=False),
    Column("update_id",    Integer,  nullable=True),
)

# 4.4) enrollments table in warehouse
warehouse_enrollments = Table(
    "enrollments", metadata,
    Column("enrollment_sk",   Integer, primary_key=True, autoincrement=True),  # surrogate PK
    Column("enrollment_id",   Integer, nullable=False),                        # business key
    Column("user_sk",         Integer, ForeignKey("warehouse.users.user_sk"),   nullable=False),
    Column("course_sk",       Integer, ForeignKey("warehouse.courses.course_sk"), nullable=False),
    Column("enrolled_at",     DateTime, nullable=False, default=datetime.datetime.utcnow),

    Column("status", String(20), nullable=False),
    CheckConstraint(
        "status IN ('active','completed','cancelled')",
        name="valid_status"
    ),

    # ── Audit / SCD columns ────────────────────────────────────────────────────
    Column("start_date",   DateTime, nullable=False, server_default=text("NOW()")),
    Column("end_date",     DateTime, nullable=True),
    Column("source_id",    Integer,  nullable=False),
    Column("insert_id",    Integer,  nullable=False),
    Column("update_id",    Integer,  nullable=True),
)

# 4.5) sales table in warehouse
warehouse_sales = Table(
    "sales", metadata,
    Column("sale_sk",         Integer, primary_key=True, autoincrement=True),  # surrogate PK
    Column("sale_id",         Integer, nullable=False),                        # business key
    Column("enrollment_sk",   Integer, ForeignKey("warehouse.enrollments.enrollment_sk"), nullable=False),
    Column("sales_manager_sk",Integer, ForeignKey("warehouse.sales_managers.sales_manager_sk"), nullable=False),
    Column("sale_date",       DateTime, nullable=False, default=datetime.datetime.utcnow),
    Column("cost_in_rubbles", Integer,  nullable=False),

    CheckConstraint("cost_in_rubbles >= 0", name="cost_in_rubbles_non_negative"),

    # ── Audit / SCD columns ────────────────────────────────────────────────────
    Column("start_date",   DateTime, nullable=False, server_default=text("NOW()")),
    Column("end_date",     DateTime, nullable=True),
    Column("source_id",    Integer,  nullable=False),
    Column("insert_id",    Integer,  nullable=False),
    Column("update_id",    Integer,  nullable=True),
)

# 4.6) traffic_sources table in warehouse
warehouse_traffic_sources = Table(
    "traffic_sources", metadata,
    Column("traffic_source_sk", Integer, primary_key=True, autoincrement=True),  # surrogate PK
    Column("source_id",       Integer, nullable=False),                          # business key
    Column("name",            String(100), nullable=False),
    Column("channel",         String(100), nullable=False),
    Column("details",         Text),

    # ── Audit / SCD columns ────────────────────────────────────────────────────
    Column("start_date",       DateTime, nullable=False, server_default=text("NOW()")),
    Column("end_date",         DateTime, nullable=True),
    Column("source_id_audit",  Integer, nullable=False),
    Column("insert_id",        Integer, nullable=False),
    Column("update_id",        Integer, nullable=True),
)

# 4.7) user_traffic table in warehouse
warehouse_user_traffic = Table(
    "user_traffic", metadata,
    Column("user_traffic_sk", Integer, primary_key=True, autoincrement=True),  # surrogate PK
    Column("user_sk",         Integer, ForeignKey("warehouse.users.user_sk"),   nullable=False),
    Column("traffic_source_sk",Integer, ForeignKey("warehouse.traffic_sources.traffic_source_sk"), nullable=False),
    Column("referred_at",     DateTime, nullable=False, default=datetime.datetime.utcnow),
    Column("campaign_code",   String(50)),

    # ── Audit / SCD columns ────────────────────────────────────────────────────
    Column("start_date",       DateTime, nullable=False, server_default=text("NOW()")),
    Column("end_date",         DateTime, nullable=True),
    Column("source_id_audit",  Integer, nullable=False),
    Column("insert_id",        Integer, nullable=False),
    Column("update_id",        Integer, nullable=True),
)

# ───────────── 5) CREATE ALL TABLES AT ONCE ─────────────────────────────────────
if __name__ == "__main__":
    metadata.create_all(engine)
    print("✅ All warehouse tables created (including sales_managers, surrogate PKs, and corrected FKs).")
