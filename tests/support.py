import sqlalchemy as sa
from sqlalchemy_utils import create_database, database_exists, drop_database

from datamapper import Associations, BelongsTo, HasMany, HasOne, Model

metadata = sa.MetaData()


class User(Model):
    __table__ = sa.Table(
        "users",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String),
    )

    __associations__ = Associations(
        HasOne("home", "tests.support.Home", "owner_id"),
        HasMany("pets", "tests.support.Pet", "owner_id"),
    )


class Home(Model):
    __table__ = sa.Table(
        "homes",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String),
        sa.Column("owner_id", sa.Integer, sa.ForeignKey("users.id")),
    )

    __associations__ = Associations(BelongsTo("owner", User, "owner_id"))


class Pet(Model):
    __table__ = sa.Table(
        "pets",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String),
        sa.Column("owner_id", sa.Integer, sa.ForeignKey("users.id")),
    )

    __associations__ = Associations(BelongsTo("owner", User, "owner_id"))


def provision_database(url: str):
    engine = sa.create_engine(url)
    if database_exists(engine.url):
        drop_database(engine.url)
    create_database(engine.url)
    metadata.create_all(engine)


def to_sql(statement):
    """Convert a SQLAlchemy statement to raw SQL"""
    dialect = sa.dialects.postgresql.dialect()
    options = {"literal_binds": True}
    return str(statement.compile(dialect=dialect, compile_kwargs=options))
