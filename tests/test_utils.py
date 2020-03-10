import pytest
from datamapper.errors import NoResultsError, MultipleResultsError
from datamapper._utils import assert_one, to_list, to_tree


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
