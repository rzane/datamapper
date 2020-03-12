import os

from databases import Database
from sqlalchemy import BigInteger, Column, ForeignKey, MetaData, String, Table
from sqlalchemy.dialects import postgresql

from datamapper import Associations, BelongsTo, HasMany, HasOne, Model

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres@localhost/datamapper")


metadata = MetaData()
database = Database(DATABASE_URL, force_rollback=True)


class User(Model):
    __table__ = Table(
        "users",
        metadata,
        Column("id", BigInteger, primary_key=True),
        Column("name", String),
    )

    __associations__ = Associations(
        HasOne("home", "tests.support.Home", "owner_id"),
        HasMany("pets", "tests.support.Pet", "owner_id"),
    )


class Home(Model):
    __table__ = Table(
        "homes",
        metadata,
        Column("id", BigInteger, primary_key=True),
        Column("name", String),
        Column("owner_id", BigInteger, ForeignKey("users.id")),
    )

    __associations__ = Associations(BelongsTo("owner", User, "owner_id"))


class Pet(Model):
    __table__ = Table(
        "pets",
        metadata,
        Column("id", BigInteger, primary_key=True),
        Column("name", String),
        Column("owner_id", BigInteger, ForeignKey("users.id")),
    )

    __associations__ = Associations(BelongsTo("owner", User, "owner_id"))


def to_sql(statement):
    """Convert a SQLAlchemy statement to raw SQL"""

    return str(
        statement.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    )
