from infrastructure_runtime.security import hardened
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.config.settings import StorageSinkSettings
from app.db.models import Base


def create_session_factory(settings: StorageSinkSettings) -> sessionmaker:
    engine = create_engine(settings.database_url, pool_pre_ping=True, hide_parameters=True, pool_size=5, max_overflow=10,
        connect_args={"connect_timeout": 5, "options": "-c statement_timeout=15000 -c lock_timeout=5000"})
    with engine.connect() as connection:
        connection.execute(text("SELECT 1 FROM pipeline_receipts LIMIT 0"))
        connection.execute(text("SELECT 1 FROM pipeline_outbox LIMIT 0"))
    if not hardened():
        Base.metadata.create_all(bind=engine)
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)

