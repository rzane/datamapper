import pytest
from sqlalchemy import Column, Table, BigInteger, String
from datamapper import Model
from tests.conftest import metadata


class User(Model):
    __tablename__ = "users"
    __metadata__ = metadata

    id = Column(BigInteger, primary_key=True)
    name = Column(String)


def test_getttr():
    assert User(name="Ray").name == "Ray"


def test_getattr_invalid():
    user = User(invalid_attribute="Foo")

    with pytest.raises(AttributeError):
        user.invalid_attribute

    with pytest.raises(AttributeError):
        user.missing_attribute


def test_tablename():
    assert User.__tablename__ == "users"


def test_table():
    assert isinstance(User.__table__, Table)
    assert isinstance(User.__table__.c.id, Column)

    assert User.__table__.name == "users"
    assert User.__table__.metadata == metadata
