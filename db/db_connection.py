import os
import psycopg


def connect_rw():
    """RW connection, used for trigger install and guard table inserts."""
    dsn = os.getenv("POSTGRES_DSN_RW")
    if not dsn:
        user = os.getenv("POSTGRES_USER", "uos_planner")
        pwd = os.getenv("POSTGRES_PASSWORD", "uos_pw")
        host = os.getenv("POSTGRES_HOST", "postgres")
        port = os.getenv("POSTGRES_PORT", "5432")
        db = os.getenv("POSTGRES_DB", "urbanos")
        dsn = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
    return psycopg.connect(dsn, autocommit=True)


def connect_ro():
    """RO connection, safe for KPI view queries."""
    dsn = os.getenv("POSTGRES_DSN_RO")
    if dsn:
        return psycopg.connect(dsn, autocommit=True)
    return connect_rw()
