import os
from datamapper import Model, BelongsTo, HasOne, HasMany
from databases import Database
from sqlalchemy import MetaData, Column, BigInteger, String, ForeignKey
from sqlalchemy.dialects import postgresql


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost/datamapper")


metadata = MetaData()
database = Database(DATABASE_URL, force_rollback=True)


class User(Model):
    __tablename__ = "users"
    __metadata__ = metadata

    id = Column(BigInteger, primary_key=True)
    name = Column(String)
    home = HasOne("tests.support.Home", "owner_id")
    pets = HasMany("tests.support.Pet", "owner_id")


class Home(Model):
    __tablename__ = "homes"
    __metadata__ = metadata

    id = Column(BigInteger, primary_key=True)
    name = Column(String)
    owner_id = Column(BigInteger, ForeignKey("users.id"))
    owner = BelongsTo("tests.support.User", "owner_id")


class Pet(Model):
    __tablename__ = "pets"
    __metadata__ = metadata

    id = Column(BigInteger, primary_key=True)
    name = Column(String)
    owner_id = Column(BigInteger, ForeignKey("users.id"))
    owner = BelongsTo("tests.support.User", "owner_id")


def to_sql(statement):
    """Convert a SQLAlchemy statement to raw SQL"""

    return str(
        statement.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    )
