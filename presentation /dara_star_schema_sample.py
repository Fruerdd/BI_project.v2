#!/usr/bin/env python3
# sample_star_schema_data.py

from sqlalchemy import create_engine
import pandas as pd

# 1) Configure your Postgres connection
DATABASE_URL = "postgresql+psycopg2://postgres:admin@localhost:5432/bi_project"
engine = create_engine(DATABASE_URL)

def sample_table(schema: str, table: str, limit: int = 5) -> pd.DataFrame:
    """
    Fetch up to `limit` rows from `schema.table` into a DataFrame.
    """
    sql = f"SELECT * FROM {schema}.{table} LIMIT {limit};"
    return pd.read_sql_query(sql, engine)  # https://pandas.pydata.org/docs/reference/api/pandas.read_sql_query.html

def print_markdown(df: pd.DataFrame, title: str) -> None:
    """
    Print a DataFrame as a Markdown-style table.
    """
    print(f"\n## {title}\n")
    print(df.to_markdown(index=False))    # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_markdown.html

if __name__ == "__main__":
    schema = "star_schema"

    # 2a) Sample from a dimension table (e.g. dim_course)
    dim_course_df = sample_table(schema, "dim_course")
    print_markdown(dim_course_df, "Sample Rows from star_schema.dim_course (Dimension)")

    # 2b) Sample from a fact table (fact_sales)
    fact_sales_df = sample_table(schema, "fact_sales")
    print_markdown(fact_sales_df, "Sample Rows from star_schema.fact_sales (Fact)")
