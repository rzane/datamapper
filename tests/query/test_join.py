import pytest
from datamapper.errors import MissingJoinError
from datamapper.query.join import Join, to_join_tree
from tests.support import User, Pet


def test_join() -> None:
    join = Join(User, ["pets"])
    assert join.base == User
    assert join.path == ["pets"]


def test_join_name() -> None:
    join = Join(User, ["pets"])
    assert join.name == "pets"


def test_join_name_nested() -> None:
    join = Join(User, ["pets", "owner"])
    assert join.name == "pets.owner"


def test_join_find_association() -> None:
    join = Join(User, ["pets"])
    assoc = join.find_association()
    assert assoc == User.association("pets")


def test_join_association_nested() -> None:
    join = Join(User, ["pets", "owner"])
    assoc = join.find_association()
    assert assoc == Pet.association("owner")


def test_join_equality() -> None:
    join1 = Join(User, ["pets"])
    join2 = Join(User, ["pets"])

    assert join1 == join2
    assert join2 == join1


def test_join_inequality() -> None:
    pets = Join(User, ["pets"])
    home = Join(User, ["home"])

    assert pets != home
    assert home != pets


def test_join_inquality_other() -> None:
    join = Join(User, ["pets"])
    assert join != 100


def test_join_hash() -> None:
    pets = Join(User, ["pets"])
    owner = Join(User, ["pets", "owner"])
    joins: dict = {pets: {owner: {}}}

    assert pets in joins
    assert owner in joins[pets]
    assert joins[pets][owner] == {}


def test_join_repr() -> None:
    join = Join(User, ["pets", "owner"])
    assert repr(join) == '<Join base=User name="pets.owner">'


def test_to_join_tree() -> None:
    pets = Join(User, ["pets"])
    owner = Join(User, ["pets", "owner"])
    tree = to_join_tree([pets, owner])
    assert tree[pets][owner] == {}


def test_to_join_tree_missing() -> None:
    owner = Join(User, ["pets", "owner"])
    message = "Can't join 'pets.owner' without joining 'pets'"

    with pytest.raises(MissingJoinError, match=message):
        to_join_tree([owner])
