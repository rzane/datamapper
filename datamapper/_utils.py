import datamapper.errors as errors
from typing import TypeVar, List, Union

T = TypeVar("T")


def assert_one(values: list) -> None:
    """
    Makes sure that a list contains exactly one item or raises an error.
    """

    if not values:
        raise errors.NoResultsError()

    if len(values) > 1:
        raise errors.MultipleResultsError(len(values))


def to_list(value: Union[T, List[T]]) -> List[T]:
    """
    Coerce a value to a list.
    """

    if isinstance(value, list):
        return value
    return [value] if value else []


def to_tree(paths: List[str]) -> dict:
    """
    Takes a list of preloads and converts them to a dependency tree.

    >>> to_tree(["a.b", "a.c"])
    {"a": {"b": {}, "c": {}}}
    """

    result: dict = {}
    for path in paths:
        current = result
        for key in path.split("."):
            if key not in current:
                current[key] = {}
            current = current[key]
    return result
