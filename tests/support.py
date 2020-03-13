from sqlalchemy import Integer, Column, ForeignKey, MetaData, String, Table
from sqlalchemy.dialects import postgresql

from datamapper import Associations, BelongsTo, HasMany, HasOne, Model

metadata = MetaData()


class User(Model):
    __table__ = Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
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
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("owner_id", Integer, ForeignKey("users.id")),
    )

    __associations__ = Associations(BelongsTo("owner", User, "owner_id"))


class Pet(Model):
    __table__ = Table(
        "pets",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("owner_id", Integer, ForeignKey("users.id")),
    )

    __associations__ = Associations(BelongsTo("owner", User, "owner_id"))


def to_sql(statement):
    """Convert a SQLAlchemy statement to raw SQL"""

    return str(
        statement.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    )
