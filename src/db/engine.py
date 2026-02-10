from sqlalchemy import create_engine
import logging
from dotenv import load_dotenv
import os

load_dotenv()

logger = logging.getLogger(__name__)
_engine= None


def get_engine():
    global _engine
    if _engine is None:
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_name = os.getenv("DB_NAME")

        if not all([db_user, db_password, db_host, db_name]):
            raise ValueError("Database credentials are not fully set in environment variables.")

        _engine = create_engine(
            f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}",
            echo=True,           # optional: logs SQL statements
            pool_pre_ping=True
        )
        print([db_user,db_host,db_name])
        logger.info("Database engine created successfully.")
    return _engine

def shutdown():
    global _engine
    if _engine:
        _engine.dispose()
        logger.info("Database engine disposed.")
        _engine = None

get_engine()