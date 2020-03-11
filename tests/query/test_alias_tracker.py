from datamapper.query.alias_tracker import AliasTracker
from tests.support import User


def test_alias_tracker() -> None:
    tracker = AliasTracker()

    table = User.__table__
    assert table.name == "users"

    alias = tracker.alias(table)
    assert alias.name == "u0"

    alias = tracker.alias(table)
    assert alias.name == "u1"
