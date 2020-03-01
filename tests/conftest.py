import pytest
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, database_exists
from tests.models import DATABASE_URL, metadata


@pytest.fixture(scope="session", autouse=True)
def setup_database():
    engine = create_engine(DATABASE_URL)

    if not database_exists(engine.url):
        create_database(engine.url)

    metadata.create_all(engine)
