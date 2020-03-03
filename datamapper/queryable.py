from typing import Any, Mapping
from typing_extensions import Protocol
from sqlalchemy.sql.expression import Select, Update, Delete


class Queryable(Protocol):
    def to_sql(self) -> Select:
        ...

    def to_update_sql(self) -> Update:
        ...

    def to_delete_sql(self) -> Delete:
        ...

    def deserialize(self, row: Mapping) -> Any:
        ...
