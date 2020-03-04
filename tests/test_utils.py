import pytest
from datamapper.errors import NoResultsError, MultipleResultsError
from datamapper._utils import assert_one, cast_list, expand_preloads


def test_assert_one():
    assert_one([1])
    with pytest.raises(NoResultsError):
        assert_one([])
    with pytest.raises(MultipleResultsError):
        assert_one([1, 2])


def test_cast_list():
    assert cast_list(1) == [1]
    assert cast_list("foo") == ["foo"]
    assert cast_list(["foo"]) == ["foo"]
    assert cast_list(None) == []


def test_expand_preloads():
    assert expand_preloads(["a"]) == {"a": {}}
    assert expand_preloads(["a", "b"]) == {"a": {}, "b": {}}
    assert expand_preloads(["a.b"]) == {"a": {"b": {}}}
    assert expand_preloads(["a.b", "a.c"]) == {"a": {"b": {}, "c": {}}}
