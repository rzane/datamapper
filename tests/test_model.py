import pytest
from sqlalchemy import Column, Table
from tests.support import metadata, User, Home
from datamapper.model import Association
from datamapper.errors import NotLoadedError


def test_getttr():
    assert User(name="Ray").name == "Ray"


def test_getattr_invalid():
    user = User(invalid_attribute="Foo")

    with pytest.raises(AttributeError):
        user.invalid_attribute

    with pytest.raises(AttributeError):
        user.missing_attribute


def test_getattr_association():
    user = User()
    home = Home()
    user._associations["home"] = home
    assert user.home == home


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


def test_association():
    assoc = Association(User, "user_id")
    assert assoc.model == User
    assert assoc.foreign_key == "user_id"
    assert assoc.primary_key == "id"


def test_model_as_string():
    assoc = Association("tests.support.User", "user_id")
    assert assoc.model == User
    assert assoc.primary_key == "id"
