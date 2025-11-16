import os
import sys
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Ensure local imports work when run as a module/script
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.db import get_engine


def main():
    """
    Verifies RDS connectivity and minimal permissions:
      1) Connect and run SELECT 1
      2) Create/write/read a tiny test table via pandas
    Configure with:
      - DATABASE_URL: required for DB check
      - DB_CHECK_TABLE: optional table name (default: __db_check)
    """
    table_name = os.environ.get('DB_CHECK_TABLE', '__db_check')

    engine = get_engine()
    if engine is None:
        print("DATABASE_URL not set. Set it and re-run to test DB connectivity.")
        sys.exit(1)

    print("Connecting to database...")
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            val = result.scalar()
            if val != 1:
                print(f"Unexpected result from SELECT 1: {val}")
                sys.exit(2)
        print("Connectivity OK (SELECT 1).")
    except SQLAlchemyError as e:
        print(f"Connection test failed: {e}")
        sys.exit(2)

    # Prepare tiny dataframe
    df = pd.DataFrame(
        [
            {"k": "ping", "v": "ok"},
        ]
    )

    print(f"Attempting write/read on table '{table_name}' ...")
    try:
        # Write (create if not exists, then append)
        df.to_sql(table_name, con=engine, if_exists='append', index=False, method='multi', chunksize=1000)
        print("Write OK.")
    except SQLAlchemyError as e:
        print(f"Write failed (permissions or schema issue): {e}")
        sys.exit(3)

    try:
        with engine.connect() as conn:
            # Read back count
            result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
            count_val = result.scalar()
            print(f"Read OK. Rows in '{table_name}': {count_val}")
    except SQLAlchemyError as e:
        # Some engines (e.g., Postgres) don't use backticks; try unquoted identifier
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {table_name}'))
                count_val = result.scalar()
                print(f"Read OK. Rows in '{table_name}': {count_val}")
        except SQLAlchemyError as e2:
            print(f"Read failed: {e2}")
            sys.exit(4)

    print("DB check completed successfully.")


if __name__ == "__main__":
    main()


