import datamapper.errors as errors
import datamapper.model as model
from typing import TypeVar, List, Union, Type

T = TypeVar("T")


def assert_one(values: list) -> None:
    """
    Makes sure that a list contains exactly one item or raises an error.
    """

    if not values:
        raise errors.NoResultsError()

    if len(values) > 1:
        raise errors.MultipleResultsError(
            f"Expected at most one result, got {len(values)}"
        )


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


def collect_sql_values(
    model: Union[model.Model, Type[model.Model]], values: dict
) -> dict:
    """
    Convert attributes and association values to values that will be stored
    in the database.
    """

    sql_values = {}
    for key, value in values.items():
        if key in model.__associations__:
            assoc = model.__associations__[key]
            sql_values.update(assoc.values(value))
        else:
            sql_values[key] = value
    return sql_values
