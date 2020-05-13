from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datamapper.changeset import Changeset  # pragma: no cover

__all__ = [
    "ConflictingAliasError",
    "Error",
    "InvalidExpressionError",
    "InvalidSelectError",
    "MissingJoinError",
    "MultipleResultsError",
    "MultipleResultsError",
    "NoResultsError",
    "NotLoadedError",
    "UnknownAliasError",
    "UnknownAssociationError",
    "UnknownColumnError",
]


class Error(Exception):
    pass


class UnknownColumnError(Error):
    def __init__(self, table: str, name: str):
        super().__init__(f"column '{name}' does not exist for table '{table}'")


class UnknownAssociationError(Error):
    def __init__(self, model: str, name: str):
        super().__init__(f"association '{name}' does not exist for model '{model}'")


class UnknownAliasError(Error):
    def __init__(self, name: str):
        super().__init__(f"alias '{name}' does not exist")


class NoResultsError(Error):
    def __init__(self) -> None:
        super().__init__(f"expected at least one result but got none")


class MultipleResultsError(Error):
    def __init__(self, count: int) -> None:
        super().__init__(f"expected at most one result but got {count}")


class NotLoadedError(Error):
    def __init__(self, model: str, name: str):
        super().__init__(f"association '{name}' is not loaded for model '{model}'")


class MissingJoinError(Error):
    def __init__(self, parent: str, child: str):
        super().__init__(f"can't join '{child}' without joining '{parent}'")


class ConflictingAliasError(Error):
    def __init__(self, name: str):
        super().__init__(f"alias '{name}' conflicts with an existing alias")


class InvalidExpressionError(Error):
    def __init__(self, value: Any):
        super().__init__(f"`{type(value).__name__}` is not a valid query expression")


class InvalidSelectError(Error):
    def __init__(self) -> None:
        super().__init__("expected at least one select expression but got none")


class InvalidChangesetError(Error):
    def __init__(self, action: str, changeset: Changeset):
        super().__init__(
            f"could not perform {action} because changeset is invalid:\n\n{changeset}"
        )

        self.action = action
        self.changeset = changeset
