from typing import Any, Mapping
from typing_extensions import Protocol
from sqlalchemy import Column
from sqlalchemy.sql.expression import Select, Update, Delete


class Queryable(Protocol):
    def to_sql(self) -> Select:
        ...  # pragma: no cover

    def to_update_sql(self) -> Update:
        ...  # pragma: no cover

    def to_delete_sql(self) -> Delete:
        ...  # pragma: no cover

    def column(self, name: str) -> Column:
        ...  # pragma: no cover

    def deserialize(self, row: Mapping) -> Any:
        ...  # pragma: no cover
