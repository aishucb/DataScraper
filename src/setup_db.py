import os
import sys
import csv
import pymysql


def env(name: str, default: str = "") -> str:
    v = os.environ.get(name, default)
    if v is None or v == "":
        return default
    return v


def create_database_and_user():
    """
    Creates the database, app user, and grants in MariaDB/Aurora MySQL.

    Required environment variables:
      - ADMIN_HOST            e.g. database-1.xxxx.ap-south-1.rds.amazonaws.com
      - ADMIN_USER            e.g. admin
      - ADMIN_PASSWORD
      - DB_NAME               e.g. scrapeddataNSE

    Optional:
      - ADMIN_PORT            default 3306
      - APP_USER              default app_user
      - APP_PASSWORD          default StrongPassword!
      - OPTION_CHAIN_TABLE    default option_chain
      - CSV_SCHEMA_PATH       if set, use CSV header to create table columns (VARCHAR(255))
    """
    admin_host = env("ADMIN_HOST")
    admin_user = env("ADMIN_USER")
    admin_password = env("ADMIN_PASSWORD")
    admin_port = int(env("ADMIN_PORT", "3306"))

    db_name = env("DB_NAME")
    app_user = env("APP_USER", "app_user")
    app_password = env("APP_PASSWORD", "StrongPassword!")

    table_name = env("OPTION_CHAIN_TABLE", "option_chain")
    csv_schema_path = env("CSV_SCHEMA_PATH", "")

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
    conn = pymysql.connect(
        host=admin_host,
        user=admin_user,
        password=admin_password,
        port=admin_port,
        autocommit=True,
        cursorclass=pymysql.cursors.Cursor,
    )

    try:
        with conn.cursor() as cur:
            # Create database
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}` DEFAULT CHARACTER SET utf8mb4;")
            print(f"Database ensured: {db_name}")

            # Create user
            cur.execute(f"CREATE USER IF NOT EXISTS '{app_user}'@'%' IDENTIFIED BY %s;", (app_password,))
            print(f"User ensured: {app_user}")

            # Grants
            cur.execute(f"GRANT INSERT, SELECT, CREATE, ALTER ON `{db_name}`.* TO '{app_user}'@'%';")
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
                    # Build columns as VARCHAR(255)
                    col_defs = ", ".join([f"`{h}` VARCHAR(255)" for h in headers])
                    cur.execute(f"CREATE TABLE IF NOT EXISTS `{db_name}`.`{table_name}` ({col_defs});")
                    print(f"Table ensured: {db_name}.{table_name} (from CSV headers)")
            else:
                print("No CSV_SCHEMA_PATH provided; skipping table creation (pandas will auto-create on first write).")

    finally:
        conn.close()
        print("Done.")


if __name__ == "__main__":
    create_database_and_user()


