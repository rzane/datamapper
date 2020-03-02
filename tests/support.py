import os
import functools
import asyncio
from datamapper import Model
from databases import Database
from sqlalchemy import MetaData, Column, BigInteger, String
from sqlalchemy.dialects import postgresql
from sqlalchemy_utils import create_database, database_exists


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost/datamapper")


metadata = MetaData()
database = Database(DATABASE_URL, force_rollback=True)


class User(Model):
    __tablename__ = "users"
    __metadata__ = metadata

    id = Column(BigInteger, primary_key=True)
    name = Column(String)


def to_sql(statement):
    """Convert a SQLAlchemy statement to raw SQL"""

    return str(
        statement.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    )
