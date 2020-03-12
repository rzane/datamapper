import pytest
from sqlalchemy import Column

from datamapper._utils import assert_one, get_column, to_list, to_tree
from datamapper.errors import MultipleResultsError, NoResultsError, UnknownColumnError
from tests.support import User


def test_assert_one():
    assert_one([1])
    with pytest.raises(NoResultsError):
        assert_one([])
    with pytest.raises(MultipleResultsError):
        assert_one([1, 2])


def test_to_list():
    assert to_list(1) == [1]
    assert to_list("foo") == ["foo"]
    assert to_list(["foo"]) == ["foo"]
    assert to_list(None) == []


def test_to_tree():
    assert to_tree(["a"]) == {"a": {}}
    assert to_tree(["a", "b"]) == {"a": {}, "b": {}}
    assert to_tree(["a.b"]) == {"a": {"b": {}}}
    assert to_tree(["a.b", "a.c"]) == {"a": {"b": {}, "c": {}}}


def test_get_column():
    table = User.__table__
    column = get_column(table, "id")
    assert isinstance(column, Column)
    assert column.name == "id"


def test_get_column_aliased():
    table = User.__table__.alias("u")
    column = get_column(table, "id")
    assert isinstance(column, Column)
    assert column.name == "id"


def test_get_column_invalid():
    table = User.__table__
    message = "column 'trash' does not exist for table 'users'"

    with pytest.raises(UnknownColumnError, match=message):
        get_column(table, "trash")


def test_get_column_invalid_aliased():
    table = User.__table__.alias("u")
    message = "column 'trash' does not exist for table 'users'"

    with pytest.raises(UnknownColumnError, match=message):
        get_column(table, "trash")
