from __future__ import annotations

from typing import List, Union, Literal, Tuple, Callable, Set

from datamapper.repo import Repo
from datamapper.model import Model
from datamapper.changeset import Changeset

Context = dict
ModelResolver = Union[Model, Callable[[dict], Model]]
ChangesetResolver = Union[Changeset, Callable[[dict], Changeset]]
ModelOrChangesetResolver = Union[ModelResolver, ChangesetResolver]


class InsertOperation:
    def __init__(self, name: str, data: ModelOrChangesetResolver):
        self.name = name
        self.data = data

    async def run(self, repo: Repo, context: Context) -> Model:
        data = self.data

        if callable(data):
            data = data(context)

        return await repo.insert(data)


Operation = Union[
    Tuple[Literal["insert"], ModelOrChangesetResolver],
    Tuple[Literal["update"], ChangesetResolver],
    Tuple[Literal["delete"], ModelResolver],
]


class Multi:
    __slots__ = ["_operations", "_names"]

    _names: Set[str]
    _operations: List[Operation]

    def __init__(self):
        self._names = set()
        self._operations = []

    def insert(self, name: string, value: ModelOrChangesetResolver) -> Multi:
        return self._add(name, "insert",)

    def update(self, name: string, changeset: Changeset) -> Multi:
        ...

    def delete(self, name: string, record: Model) -> Multi:
        ...

    def to_list():
        return to_list(operations)
