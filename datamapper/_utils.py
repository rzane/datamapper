from typing import TypeVar, List, Union
from datamapper.errors import NoResultsError, MultipleResultsError

T = TypeVar("T")


def assert_one(values: list) -> None:
    """
    Makes sure that a list contains exactly one item or raises an error.
    """

    if not values:
        raise NoResultsError()

    if len(values) > 1:
        raise MultipleResultsError(f"Expected at most one result, got {len(values)}")


def cast_list(value: Union[T, List[T]]) -> List[T]:
    """
    Coerce a value to a list.
    """

    if isinstance(value, list):
        return value
    return [value] if value else []


def expand_preloads(paths: List[str]) -> dict:
    """
    Takes a list of preloads and converts them to a dependency tree.

    >>> expand_preloads(["a.b", "a.c"])
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
