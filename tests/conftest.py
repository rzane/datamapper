import pytest
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, drop_database, database_exists
from tests.support import DATABASE_URL, metadata, database


@pytest.fixture(scope="session", autouse=True)
def setup_database():
    """Create database tables before running the test suite"""

    engine = create_engine(DATABASE_URL)
    if database_exists(engine.url):
        drop_database(engine.url)
    create_database(engine.url)
    metadata.create_all(engine)


@pytest.fixture(autouse=True)
async def rollback_transaction():
    """Surround each test in a transaction"""
    async with database:
        yield
