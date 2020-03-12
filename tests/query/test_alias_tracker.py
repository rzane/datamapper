import pytest
from sqlalchemy.sql.expression import Alias

from datamapper.errors import ConflictingAliasError, UnknownAliasError
from datamapper.query.alias_tracker import AliasTracker
from tests.support import User


def test_put() -> None:
    tracker = AliasTracker()

    table = User.__table__
    assert table.name == "users"

    alias = tracker.put(table)
    assert isinstance(alias, Alias)
    assert alias.name == "u0"

    alias = tracker.put(table)
    assert isinstance(alias, Alias)
    assert alias.name == "u1"


def test_put_named() -> None:
    tracker = AliasTracker()
    alias = tracker.put(User.__table__, alias_name="u")
    assert isinstance(alias, Alias)
    assert alias.name == "u"


def test_put_conflict() -> None:
    tracker = AliasTracker()
    tracker.put(User.__table__)

    with pytest.raises(ConflictingAliasError):
        tracker.put(User.__table__, "u0")


def test_fetch_generated() -> None:
    tracker = AliasTracker()
    tracker.put(User.__table__)

    alias = tracker.fetch("u0")
    assert isinstance(alias, Alias)


def test_fetch_named() -> None:
    tracker = AliasTracker()
    tracker.put(User.__table__, "u")

    alias = tracker.fetch("u")
    assert isinstance(alias, Alias)


def test_fetch_unknown() -> None:
    tracker = AliasTracker()
    message = "alias 'trash' does not exist"

    with pytest.raises(UnknownAliasError, match=message):
        tracker.fetch("trash")
