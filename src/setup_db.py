import os
import sys
import csv
import pymysql
import argparse


def env(name: str, default: str = "") -> str:
    v = os.environ.get(name, default)
    if v is None or v == "":
        return default
    return v


def create_database_and_user(args):
    admin_host = args.admin_host or env("ADMIN_HOST")
    admin_user = args.admin_user or env("ADMIN_USER")
    admin_password = args.admin_password or env("ADMIN_PASSWORD")
    admin_port = int(args.admin_port or env("ADMIN_PORT", "3306"))

    db_name = args.db_name or env("DB_NAME")
    app_user = args.app_user or env("APP_USER", "app_user")
    app_password = args.app_password or env("APP_PASSWORD", "StrongPassword!")

    table_name = args.table or env("OPTION_CHAIN_TABLE", "option_chain")
    csv_schema_path = args.csv_schema or env("CSV_SCHEMA_PATH", "")

    missing = [k for k, v in [
        ("ADMIN_HOST", admin_host),
        ("ADMIN_USER", admin_user),
        ("ADMIN_PASSWORD", admin_password),
        ("DB_NAME", db_name),
    ] if not v]
    if missing:
        print(f"Missing required env vars: {', '.join(missing)}")
        sys.exit(1)

    print(f"Connecting to {admin_host}:{admin_port} as {admin_user} ...")

    # Use RDS CA bundle for SSL
    conn = pymysql.connect(
        host=admin_host,
        user=admin_user,
        password=admin_password,
        port=admin_port,
        autocommit=True,
        ssl={"ca": "/usr/local/share/ca-certificates/rds-ca.pem"},
        cursorclass=pymysql.cursors.Cursor,
    )

    try:
        with conn.cursor() as cur:
            # Create database
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}` DEFAULT CHARACTER SET utf8mb4;")
            print(f"Database ensured: {db_name}")

            # Create user (parameterize user and host to avoid % interpolation issues)
            cur.execute(
                "CREATE USER IF NOT EXISTS %s@%s IDENTIFIED BY %s;",
                (app_user, "%", app_password)
            )
            print(f"User ensured: {app_user}")

            # Grants
            cur.execute(
                f"GRANT INSERT, SELECT, CREATE, ALTER ON `{db_name}`.* TO %s@%s;",
                (app_user, "%")
            )
            cur.execute("FLUSH PRIVILEGES;")
            print(f"Granted privileges on {db_name} to {app_user}")

            # Optional table creation from CSV header
            if csv_schema_path and os.path.exists(csv_schema_path):
                with open(csv_schema_path, newline="", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    headers = next(reader, [])
                if not headers:
                    print(f"No headers found in CSV {csv_schema_path}; skipping table creation.")
                else:
                    col_defs = ", ".join([f"`{h}` VARCHAR(255)" for h in headers])
                    cur.execute(f"CREATE TABLE IF NOT EXISTS `{db_name}`.`{table_name}` ({col_defs});")
                    print(f"Table ensured: {db_name}.{table_name}")
            else:
                print("No CSV schema; skipping table creation.")

    finally:
        conn.close()
        print("Done.")


def test_app_connection(host: str, port: int, db_name: str, app_user: str, app_password: str) -> bool:
    print(f"Testing app user connection to {host}:{port}/{db_name} as {app_user} ...")
    try:
        # Use RDS CA bundle for SSL
        conn = pymysql.connect(
            host=host,
            user=app_user,
            password=app_password,
            database=db_name,
            port=port,
            autocommit=True,
            ssl={"ca": "/usr/local/share/ca-certificates/rds-ca.pem"},
            cursorclass=pymysql.cursors.Cursor,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                row = cur.fetchone()
                if row and row[0] == 1:
                    print("App user connectivity OK (SELECT 1).")
                    return True
                else:
                    print(f"Unexpected SELECT 1 result: {row}")
                    return False
        finally:
            conn.close()
    except pymysql.MySQLError as e:
        print(f"App user connection failed: {e}")
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Setup MariaDB/Aurora MySQL.")
    parser.add_argument("--admin-host")
    parser.add_argument("--admin-user")
    parser.add_argument("--admin-password")
    parser.add_argument("--admin-port")

    parser.add_argument("--db-name")
    parser.add_argument("--app-user")
    parser.add_argument("--app-password")

    parser.add_argument("--table")
    parser.add_argument("--csv-schema")

    parser.add_argument("--print-url", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    create_database_and_user(args)

    host = args.admin_host or env("ADMIN_HOST")
    port = int(args.admin_port or env("ADMIN_PORT", "3306"))
    db = args.db_name or env("DB_NAME")
    app_user = args.app_user or env("APP_USER", "app_user")
    app_password = args.app_password or env("APP_PASSWORD", "StrongPassword!")

    if test_app_connection(host, port, db, app_user, app_password):
        url = f"mysql+pymysql://{app_user}:{app_password}@{host}:{port}/{db}"
        print("Setup complete.")
        if args.print_url:
            print("DATABASE_URL:")
            print(url)
    else:
        print("Setup completed, but app user connectivity failed.")
