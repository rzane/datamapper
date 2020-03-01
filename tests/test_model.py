import pytest
from sqlalchemy import Column, BigInteger, Table
from datamapper import Model
from tests.conftest import metadata


class User(Model):
    __tablename__ = "users"
    __metadata__ = metadata

    id = Column(BigInteger, primary_key=True)


def test_read_attribute():
    assert User(name="Ray").name == "Ray"


def test_read_invalid_attribute():
    with pytest.raises(AttributeError):
        User().invalid_attribute


def test_tablename():
    assert User.__tablename__ == "users"


def test_table():
    assert isinstance(User.__table__, Table)
    assert isinstance(User.__table__.c.id, Column)

    assert User.__table__.name == "users"
    assert User.__table__.metadata == metadata
