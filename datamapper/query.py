from __future__ import annotations
import datamapper.model as model
from typing import Any, List, Mapping, Optional, Union, Tuple, Type
from sqlalchemy.sql.expression import ClauseElement, Select, Update, Delete


SEPARATOR = "__"
OPERATIONS = {
    "exact": "__eq__",
    "iexact": "ilike",
    "contains": "like",
    "icontains": "ilike",
    "in": "in_",
    "gt": "__gt__",
    "gte": "__ge__",
    "lt": "__lt__",
    "lte": "__le__",
}


class Query:
    __slots__ = ["_model", "_wheres", "_order_bys", "_limit", "_offset", "_preloads"]

    _model: Type[model.Model]
    _wheres: List[ClauseElement]
    _order_bys: List[str]
    _limit: Optional[int]
    _offset: Optional[int]
    _preloads: List[str]

    def __init__(self, model: Type[model.Model]):
        self._model = model
        self._wheres = []
        self._order_bys = []
        self._limit = None
        self._offset = None
        self._preloads = []

    def to_query(self) -> Query:
        return self

    def to_sql(self) -> Select:
        return self.__build_query(self._model.__table__.select())

    def to_update_sql(self) -> Update:
        return self.__build_query(self._model.__table__.update())

    def to_delete_sql(self) -> Delete:
        return self.__build_query(self._model.__table__.delete())

    def deserialize(self, row: Mapping) -> model.Model:
        return self._model.deserialize(row)

    def limit(self, value: int) -> Query:
        return self.__update(_limit=value)

    def offset(self, value: int) -> Query:
        return self.__update(_offset=value)

    def where(self, *args: ClauseElement, **kwargs: Any) -> Query:
        wheres: List[ClauseElement] = []

        for arg in args:
            assert isinstance(arg, ClauseElement)
            wheres.append(arg)

        for name, value in kwargs.items():
            name, op = _parse_where(name)
            column = self._model.column(name)
            expr = getattr(column, op)
            wheres.append(expr(value))

        return self.__update(_wheres=self._wheres + wheres)

    def order_by(self, *args: Union[str, ClauseElement]) -> Query:
        exprs = []

        for arg in args:
            if isinstance(arg, str) and arg.startswith("-"):
                column = self._model.column(arg[1:])
                exprs.append(column.desc())
            elif isinstance(arg, str):
                column = self._model.column(arg)
                exprs.append(column.asc())
            else:
                assert isinstance(arg, ClauseElement)
                exprs.append(arg)

        return self.__update(_order_bys=self._order_bys + exprs)

    def preload(self, preload: str) -> Query:
        return self.__update(_preloads=self._preloads + [preload])

    def __build_query(self, sql: ClauseElement) -> ClauseElement:
        for where_clause in self._wheres:
            sql = sql.where(where_clause)

        if self._order_bys:
            sql = sql.order_by(*self._order_bys)

        if self._limit is not None:
            sql = sql.limit(self._limit)

        if self._offset is not None:
            sql = sql.offset(self._offset)

        return sql

    def __update(self, **kwargs: Any) -> Query:
        query = self.__class__(self._model)
        for key in self.__class__.__slots__:
            if key in kwargs:
                setattr(query, key, kwargs[key])
            else:
                setattr(query, key, getattr(self, key))
        return query


def _parse_where(name: str) -> Tuple[str, str]:
    op = "__eq__"
    parts = name.split(SEPARATOR)
    name = parts.pop()
    if name in OPERATIONS:
        op = OPERATIONS[name]
        name = parts.pop()
    return (name, op)
