import os
from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_database_url() -> Optional[str]:
    """
    Read the database URL from environment variable DATABASE_URL.
    Expected formats:
      - Postgres: postgresql+psycopg2://user:pass@host:5432/dbname
      - MySQL:    mysql+pymysql://user:pass@host:3306/dbname
	Example (AWS RDS):
	  - export DATABASE_URL="postgresql+psycopg2://USER:PASS@your-rds-endpoint.rds.amazonaws.com:5432/DBNAME"
	  - export DATABASE_URL="mysql+pymysql://USER:PASS@your-rds-endpoint.rds.amazonaws.com:3306/DBNAME"
    """
    return os.environ.get('DATABASE_URL')


def get_engine(echo: bool = False, pool_size: int = 5, max_overflow: int = 10) -> Optional[Engine]:
    """
    Return a SQLAlchemy engine if DATABASE_URL is set, else None.
    """
    db_url = get_database_url()
    if not db_url:
        return None
    return create_engine(db_url, echo=echo, pool_size=pool_size, max_overflow=max_overflow, pool_pre_ping=True)


