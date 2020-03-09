from tests.support import metadata, User
from datamapper.model import Model, HasOne, HasMany
from datamapper.changeset import Changeset
from sqlalchemy import Column, BigInteger, String


class Person(Model):
    __tablename__ = "people"
    __metadata__ = metadata

    id = Column(BigInteger, primary_key=True)
    name = Column(String)
    age = Column(BigInteger)
    home = HasOne("tests.support.Home", "owner_id")
    pets = HasMany("tests.support.Pet", "owner_id")


def test_cast_empty():
    changeset = Changeset(User).cast({}, [])
    assert changeset.is_valid
    assert changeset.changes == {}


def test_cast():
    changeset = Changeset(User).cast({"name": "Richard", "foo": "bar"}, ["name"])
    assert changeset.is_valid
    assert changeset.changes == {"name": "Richard"}


def test_cast_type_check():
    changeset = Changeset(User).cast({"name": 1}, ["name"])
    assert changeset.errors == {"name": ["Not a valid string."]}


def test_validate_required():
    changeset = (
        Changeset(User).cast({}, ["foo", "bar"]).validate_required(["foo", "bar"])
    )
    assert changeset.errors == {
        "foo": ["Missing data for required field."],
        "bar": ["Missing data for required field."],
    }
