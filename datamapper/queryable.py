from typing import Any, Mapping
from typing_extensions import Protocol
from sqlalchemy import Column
from sqlalchemy.sql.expression import Select, Update, Delete


class Queryable(Protocol):
    def to_sql(self) -> Select:
        ...

    def to_update_sql(self) -> Update:
        ...

    def to_delete_sql(self) -> Delete:
        ...

    def column(self, name: str) -> Column:
        ...

    def deserialize(self, row: Mapping) -> Any:
        ...
