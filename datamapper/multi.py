from __future__ import annotations

from typing import List, Union, Callable, Set, TypeVar, Generic, Tuple, TYPE_CHECKING


if TYPE_CHECKING:
    from datamapper.repo import Repo
    from datamapper.model import Model
    from datamapper.changeset import Changeset


T = TypeVar("T")
R = TypeVar("R")

Resolver = Union[T, Callable[[dict], T]]


class Operation(Generic[T, R]):
    name: str
    action: str
    value: Resolver[T]

    def __init__(self, name: str, value: Resolver[T]):
        self.name = name
        self.value = value

    async def _apply(self, repo: "Repo", context: dict) -> dict:
        value = self.value

        if callable(value):
            value = value(context)

        result = await self._execute(repo, value)
        return {**context, self.name: result}

    async def _execute(self, repo: "Repo", value: T) -> R:
        raise NotImplementedError()


class InsertOperation(Operation[Union["Model", "Changeset"], "Model"]):
    action = "insert"

    async def _execute(
        self, repo: "Repo", value: Union["Model", "Changeset"]
    ) -> "Model":
        return await repo.insert(value)


class UpdateOperation(Operation["Changeset", "Model"]):
    action = "update"

    async def _execute(self, repo: "Repo", value: "Changeset") -> "Model":
        return await repo.update(value)


class DeleteOperation(Operation["Model", "Model"]):
    action = "delete"

    async def _execute(self, repo: "Repo", value: "Model") -> "Model":
        return await repo.delete(value)


class Multi:
    _names: Set[str]
    _operations: List[Operation]

    def __init__(self) -> None:
        self._names = set()
        self._operations = []

    def insert(self, name: str, value: Resolver[Union["Model", "Changeset"]]) -> Multi:
        return self._add(InsertOperation(name, value))

    def update(self, name: str, value: Resolver["Changeset"]) -> Multi:
        return self._add(UpdateOperation(name, value))

    def delete(self, name: str, value: Resolver["Model"]) -> Multi:
        return self._add(DeleteOperation(name, value))

    def to_list(
        self,
    ) -> List[Tuple[str, Tuple[str, Resolver[Union["Model", "Changeset"]]]]]:
        result = []
        for op in self._operations:
            result.append((op.name, (op.action, op.value)))
        return result

    def _add(self, operation: Operation) -> Multi:
        if operation.name in self._names:
            raise RuntimeError(
                f"an operation called {operation.name} has already been added to the multi"
            )

        multi = Multi()
        multi._names = self._names.union(operation.name)
        multi._operations = self._operations + [operation]
        return multi

    async def _apply(self, repo: "Repo") -> dict:
        context: dict = {}
        for operation in self._operations:
            context = await operation._apply(repo, context)
        return context
