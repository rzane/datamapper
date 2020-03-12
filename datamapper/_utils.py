from typing import List, TypeVar, Union

from sqlalchemy import Column, Table
from sqlalchemy.sql.expression import Alias

from datamapper.errors import MultipleResultsError, NoResultsError, UnknownColumnError

T = TypeVar("T")


def assert_one(values: list) -> None:
    """
    Makes sure that a list contains exactly one item or raises an error.
    """

    if not values:
        raise NoResultsError()

    if len(values) > 1:
        raise MultipleResultsError(len(values))


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


def get_column(table: Union[Table, Alias], name: str) -> Column:
    """
    Retrieve a column from a table or table alias.

    If the column is not found, `UnknownColumnError` will be raised.
    """

    try:
        return getattr(table.columns, name)
    except AttributeError:
        table_name = table.name

        if isinstance(table, Alias):
            table_name = table.original.name

        raise UnknownColumnError(table_name, name)
