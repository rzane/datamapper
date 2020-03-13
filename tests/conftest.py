import os
import pytest
from databases import Database
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, database_exists, drop_database
from tests.support import metadata

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres@localhost/datamapper")


@pytest.fixture(scope="session", autouse=True)
def setup_database():
    """Create database tables before running the test suite"""

    engine = create_engine(DATABASE_URL)
    if database_exists(engine.url):
        drop_database(engine.url)
    create_database(engine.url)
    metadata.create_all(engine)


@pytest.fixture(scope="function")
async def database():
    """Surround each test in a transaction"""
    async with Database(DATABASE_URL, force_rollback=True) as database:
        yield database
