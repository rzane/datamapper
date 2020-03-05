import pytest
from sqlalchemy import Column, Table
from tests.support import metadata, User, Home
from datamapper.model import BelongsTo, HasOne, HasMany, Cardinality
from datamapper.errors import NotLoadedError


def test_init_with_invalid_attribute():
    with pytest.raises(AttributeError, match=r"'User'.*'invalid_attribute'"):
        User(invalid_attribute="Foo")


def test_getttr():
    assert User().name is None
    assert User(name="Ray").name == "Ray"
    with pytest.raises(AttributeError, match=r"'User'.*'missing_attribute'"):
        User().missing_attribute


def test_getattr_association():
    user = User()
    home = Home()
    user.home = home
    assert user.home == home


def test_setattr():
    user = User()
    user.name = "Ray"
    assert user.name == "Ray"


def test_setattr_belongs_to():
    user = User(id=99)
    home = Home()
    home.owner = user
    assert home.owner == user
    assert home.owner_id == user.id


def test_getattr_association_not_loaded():
    with pytest.raises(NotLoadedError):
        User().home


def test_tablename():
    assert User.__tablename__ == "users"


def test_table():
    assert isinstance(User.__table__, Table)
    assert isinstance(User.__table__.c.id, Column)

    assert User.__table__.name == "users"
    assert User.__table__.metadata == metadata


def test_belongs_to():
    assoc = BelongsTo(User, "user_id")
    assert assoc.related == User
    assert assoc.owner_key == "user_id"
    assert assoc.related_key == "id"
    assert assoc.cardinality == Cardinality.ONE
    assert assoc.values(User(id=9)) == {"user_id": 9}


def test_has_one():
    assoc = HasOne(User, "user_id")
    assert assoc.related == User
    assert assoc.owner_key == "id"
    assert assoc.related_key == "user_id"
    assert assoc.cardinality == Cardinality.ONE
    assert assoc.values(User(id=9)) == {}


def test_has_many():
    assoc = HasMany(User, "user_id")
    assert assoc.related == User
    assert assoc.owner_key == "id"
    assert assoc.related_key == "user_id"
    assert assoc.cardinality == Cardinality.MANY
    assert assoc.values(User(id=9)) == {}


def test_assoc_related_to_string():
    assoc = BelongsTo("tests.support.User", "user_id")
    assert assoc.related == User
