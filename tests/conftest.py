import pytest
from databases import Database
from datamapper import Repo
from tests.support import provision_database, DATABASE_URLS


@pytest.fixture(scope="session", autouse=True)
def setup_database(request):
    """Create database tables before running the test suite"""
    for url in DATABASE_URLS:
        provision_database(url)


@pytest.fixture(scope="function", params=DATABASE_URLS)
async def repo(request):
    async with Database(request.param, force_rollback=True) as database:
        yield Repo(database)
