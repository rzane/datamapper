import os
import pytest
from databases import Database
from tests.support import provision_database

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///test.db")
DATABASE_URLS = os.getenv("DATABASE_URLS", DATABASE_URL).split(",")


@pytest.fixture(scope="session", autouse=True, params=DATABASE_URLS)
def setup_database(request):
    """Create database tables before running the test suite"""
    provision_database(request.param)


@pytest.fixture(scope="function", params=DATABASE_URLS)
async def database(request):
    """Surround each test in a transaction"""
    async with Database(request.param, force_rollback=True) as database:
        yield database
