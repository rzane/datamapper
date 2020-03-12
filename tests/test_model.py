import pytest

from datamapper import Associations, BelongsTo, HasMany, HasOne, Model
from datamapper.errors import NotLoadedError, UnknownAssociationError
from datamapper.model import Cardinality
from tests.support import Home, User


def test_model_associations():
    assert isinstance(Model.__associations__, Associations)


def test_model_init():
    user = User(id=1, name="Foo")
    assert user.id == 1
    assert user.name == "Foo"


def test_model_init_invalid():
    with pytest.raises(AttributeError, match=r"'User'.*'invalid_attribute'"):
        User(invalid_attribute="Foo")


def test_model_association():
    assert User.association("pets") == User.__associations__["pets"]


def test_model_association_invalid():
    message = "association 'crap' does not exist for model 'User'"

    with pytest.raises(UnknownAssociationError, match=message):
        User.association("crap")


def test_model_getttr_attribute():
    assert User().name is None
    assert User(name="Ray").name == "Ray"
    with pytest.raises(AttributeError, match=r"'User'.*'missing_attribute'"):
        User().missing_attribute


def test_model_getattr_association():
    user = User()
    home = Home()
    user.home = home
    assert user.home == home


def test_model_getattr_association_not_loaded():
    user = User()
    with pytest.raises(NotLoadedError):
        user.home


def test_model_setattr_attribute():
    user = User()
    user.name = "Ray"
    assert user.name == "Ray"


def test_model_setattr_belongs_to():
    user = User(id=99)
    home = Home()
    home.owner = user
    assert home.owner == user
    assert home.owner_id == user.id


def test_belongs_to():
    assoc = BelongsTo("user", User, "user_id")
    assert assoc.name == "user"
    assert assoc.related == User
    assert assoc.owner_key == "user_id"
    assert assoc.related_key == "id"
    assert assoc.cardinality == Cardinality.ONE
    assert assoc.values(User(id=9)) == {"user_id": 9}


def test_belongs_to_string():
    assoc = BelongsTo("user", "tests.support.User", "user_id")
    assert assoc.related == User


def test_has_one():
    assoc = HasOne("user", User, "user_id")
    assert assoc.name == "user"
    assert assoc.related == User
    assert assoc.owner_key == "id"
    assert assoc.related_key == "user_id"
    assert assoc.cardinality == Cardinality.ONE
    assert assoc.values(User(id=9)) == {}


def test_has_one_string():
    assoc = HasOne("user", "tests.support.User", "user_id")
    assert assoc.related == User


def test_has_many():
    assoc = HasMany("user", User, "user_id")
    assert assoc.name == "user"
    assert assoc.related == User
    assert assoc.owner_key == "id"
    assert assoc.related_key == "user_id"
    assert assoc.cardinality == Cardinality.MANY
    assert assoc.values(User(id=9)) == {}


def test_has_many_string():
    assoc = HasMany("user", "tests.support.User", "user_id")
    assert assoc.related == User


def test_associations():
    user = BelongsTo("user", User, "user_id")
    associations = Associations(user)
    assert associations["user"] == user
    assert len(associations) == 1
    assert list(associations) == ["user"]
