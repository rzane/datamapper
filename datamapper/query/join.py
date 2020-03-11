from __future__ import annotations
from typing import List, Type, Any, Dict
import datamapper.model as model
from datamapper.errors import MissingJoinError


class Join:
    def __init__(self, base: Type[model.Model], path: List[str], outer: bool = False):
        self.base = base
        self.path = path
        self.name = ".".join(self.path)
        self.is_outer = outer

    def find_association(self) -> model.Association:
        assoc = None
        model = self.base
        for key in self.path:
            assoc = model.association(key)
            model = assoc.related
        assert assoc is not None
        return assoc

    def __hash__(self) -> int:
        return hash((self.name, self.is_outer))

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Join):
            return self.name == other.name and self.is_outer == other.is_outer
        else:
            return False

    def __repr__(self) -> str:
        name = self.name
        base = self.base.__name__
        klass = self.__class__.__name__
        return f'<{klass} base={base} name="{name}">'


def to_join_tree(joins: List[Join]) -> dict:
    """
    Take a list of joins and convert them to a dependency tree.

    >>> pets = Join(User, ["pets"])
    >>> owner = Join(User, ["pets.owner"])
    >>> to_join_tree([pets, owner])
    {pets: {owner: {}}}
    """

    result: dict = {}
    cache: Dict[str, Join] = {}

    for join in sorted(joins, key=_get_name):
        current = result
        for i in range(len(join.path) - 1):
            parent_name = ".".join(join.path[: i + 1])

            try:
                parent = cache[parent_name]
                current = current[parent]
            except KeyError:
                raise MissingJoinError(
                    f"Can't join '{join.name}' without joining '{parent_name}'"
                )

        current[join] = {}
        cache[join.name] = join

    return result


def _get_name(join: Join) -> str:
    return join.name
