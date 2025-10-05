from sqlalchemy import create_engine, text
import os

# engine = create_engine("sqlite:///mobility.db")
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "sqlite:///mobility.db"  # fallback para desarrollo
)
engine = create_engine(DATABASE_URL, pool_pre_ping=True)


def run_sql_query(sql: str):
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        rows = result.fetchall()
        return [dict(row._mapping) for row in rows]
