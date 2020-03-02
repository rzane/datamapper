import os
import functools
import asyncio
from datamapper import Model, BelongsTo, HasOne
from databases import Database
from sqlalchemy import MetaData, Column, BigInteger, String, ForeignKey
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
    profile = HasOne("tests.support.Profile", "user_id")


class Profile(Model):
    __tablename__ = "profiles"
    __metadata__ = metadata

    id = Column(BigInteger, primary_key=True)
    user_id = Column(BigInteger, ForeignKey("users.id"), nullable=False)
    user = BelongsTo("tests.support.User", "user_id")


def to_sql(statement):
    """Convert a SQLAlchemy statement to raw SQL"""

    return str(
        statement.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    )
