import os
import pytest
from databases import Database
from sqlalchemy import MetaData, create_engine
from sqlalchemy_utils import create_database, database_exists

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost/datamapper")

metadata = MetaData()
database = Database(DATABASE_URL, force_rollback=True)


@pytest.fixture(scope="session", autouse=True)
def setup_database():
    engine = create_engine(DATABASE_URL)

    if not database_exists(engine.url):
        create_database(engine.url)

    metadata.create_all(engine)
