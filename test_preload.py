from typing import List, TypeVar, Union


def expand(paths: List[str]) -> dict:
    result: dict = {}
    for path in paths:
        acc = result
        for key in path.split("."):
            if key not in acc:
                acc[key] = {}
            acc = acc[key]
    return result


T = TypeVar("T")


def to_list(value: Union[T, List[T]]) -> List[T]:
    if not value:
        return []
    if not isinstance(value, list):
        return [value]
    return value


def test_to_list() -> None:
    assert to_list(1) == [1]
    assert to_list("foo") == ["foo"]
    assert to_list(["foo"]) == ["foo"]
    assert to_list(None) == []


def test_expand() -> None:
    assert expand(["a"]) == {"a": {}}
    assert expand(["a", "b"]) == {"a": {}, "b": {}}
    assert expand(["a.b"]) == {"a": {"b": {}}}
    assert expand(["a.b", "a.c"]) == {"a": {"b": {}, "c": {}}}
