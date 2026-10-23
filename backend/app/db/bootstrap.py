from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine

from app.core.settings import settings
from app.db.models import Base


MIGRATIONS_DIR = Path("/app/migrations")


def bootstrap_schema() -> int:
    engine = create_engine(settings.database_url, pool_pre_ping=True, hide_parameters=True)
    Base.metadata.create_all(bind=engine)
    migrations = sorted(MIGRATIONS_DIR.glob("*.sql"))
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        for migration in migrations:
            connection.exec_driver_sql(migration.read_text(encoding="utf-8"))
    engine.dispose()
    return len(migrations)


def main() -> None:
    count = bootstrap_schema()
    print(f"Database schema bootstrap completed: {count} migrations applied.")


if __name__ == "__main__":
    main()
