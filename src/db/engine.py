from sqlalchemy import create_engine
import os

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(
            os.getenv("DB_URL"),
            pool_pre_ping=True
        )
    return _engine

def shutdown():
    global _engine
    if _engine:
        _engine.dispose()
