import pytest
from sqlalchemy import BigInteger, Column, String

from datamapper.changeset import Changeset
from datamapper.model import Associations, HasMany, HasOne, Model, Table
from tests.support import Home, User, metadata


class Person(Model):
    __table__ = Table(
        "people",
        metadata,
        Column("id", BigInteger, primary_key=True),
        Column("name", String),
        Column("age", BigInteger),
    )

    __associations__ = Associations(
        HasOne("home", "tests.support.Home", "owner_id"),
        HasMany("pets", "tests.support.Pet", "owner_id"),
    )


def is_30(age):
    if age != 30:
        return "not 30"


def test_cast_empty():
    changeset = Changeset(User()).cast({}, [])
    assert changeset.is_valid
    assert changeset.changes == {}


def test_cast():
    changeset = Changeset(User()).cast({"name": "Richard", "foo": "bar"}, ["name"])
    assert changeset.is_valid
    assert changeset.changes == {"name": "Richard"}


def test_cast_type_check():
    changeset = Changeset(User()).cast({"name": 1}, ["name"])
    assert changeset.errors == {"name": ["Not a valid string."]}


def test_put_assoc():
    user = User(id=1, name="Bear")
    changeset = (
        Changeset(Home())
        .cast({"name": "Big Blue House"}, ["name"])
        .put_assoc("owner", user)
    )
    assert changeset.changes == {"owner_id": 1, "name": "Big Blue House"}


def test_put_assoc_with_dict_data_is_invalid():
    user = User(id=1, name="Bear")
    with pytest.raises(ValueError):
        (
            Changeset({})
            .cast({"name": "Big Blue House"}, ["name"])
            .put_assoc("owner", user)
        )


def test_validate_required():
    changeset = Changeset(User()).cast({}, ["foo"]).validate_required(["foo", "bar"])
    assert changeset.errors == {
        "foo": ["Missing data for required field."],
        "bar": ["Missing data for required field."],
    }


def test_validate_change():
    changeset = (
        Changeset(Person())
        .cast({"name": "Richard", "age": 31}, ["name", "age"])
        .validate_change("age", is_30)
    )
    assert changeset.errors == {
        "age": ["not 30"],
    }


def test_validate_change_only_validates_if_field_is_changed():
    changeset = (
        Changeset(Person())
        .cast({"name": "Richard"}, ["name"])
        .validate_change("age", is_30)
    )
    assert changeset.is_valid


def test_change_allows_one_to_add_invalid_changes():
    changeset = (
        Changeset(Person())
        .cast({"name": "Richard"}, ["name"])
        .validate_change("age", is_30)
        .change({"id": "foo"})
    )

    assert changeset.changes["id"] == "foo"
