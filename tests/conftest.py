import os
import pytest
from databases import Database
from tests.support import provision_database

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres@localhost/datamapper")


@pytest.fixture(scope="session", autouse=True)
def setup_database():
    """Create database tables before running the test suite"""
    provision_database(DATABASE_URL)


@pytest.fixture(scope="function")
async def database():
    """Surround each test in a transaction"""
    async with Database(DATABASE_URL, force_rollback=True) as database:
        yield database
