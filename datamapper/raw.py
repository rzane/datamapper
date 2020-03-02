from typing import Union, Optional, Mapping
from sqlalchemy.sql.expression import ClauseElement
from datamapper.model import Model


class Raw:
    def __init__(
        self, statement: Union[str, ClauseElement], model: Optional[Model] = None
    ):
        self._statement = statement
        self._model = model

    def to_query(self) -> Union[str, ClauseElement]:
        return self._statement

    def deserialize_row(self, row: Mapping) -> Union[Mapping, Model]:
        if self._model:
            return self._model.deserialize_row(row)
        else:
            return row
