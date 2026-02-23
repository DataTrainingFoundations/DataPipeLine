"""Module that handles database connection engine"""
import os
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv


load_dotenv()

logger = logging.getLogger(__name__)
ENGINE = None

def get_engine():
    """Creates connection engine"""
    global ENGINE
    if ENGINE is None:
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_name = os.getenv("DB_NAME")

        if not all([db_user, db_password, db_host, db_name]):
            raise ValueError("Database credentials are not fully set in environment variables.")

        # Assign to the global ENGINE
        ENGINE = create_engine(
            f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}",
            echo=True,           # optional: logs SQL statements
            pool_pre_ping=True
        )
        print([db_user, db_host, db_name])
        logger.info("Database engine created successfully.")
    return ENGINE

def shutdown():
    """Shuts down created connection engine"""
    global ENGINE
    if ENGINE:
        ENGINE.dispose()
        logger.info("Database engine disposed.")
        ENGINE = None

get_engine()
