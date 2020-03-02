from typing import Any, List, Mapping, Union, Optional, Type
from datamapper.model import Model
from sqlalchemy import and_, or_, Column
from sqlalchemy.sql.expression import ClauseElement, ClauseList


class Query:
    model: Type[Model]
    _where: Optional[ClauseList]
    _limit: Optional[int]
    _offset: Optional[int]
    _order_by: List[str]

    def __init__(self, model: Type[Model]):
        self.model = model
        self._limit = None
        self._offset = None
        self._where = None
        self._order_by = []

    def to_sql(self) -> ClauseElement:
        return self._build_query(self.model.__table__.select())

    def to_update_sql(self, **values: Any) -> ClauseElement:
        return self._build_query(self.model.__table__.update()).values(**values)

    def to_delete_sql(self) -> ClauseElement:
        return self._build_query(self.model.__table__.delete())

    def limit(self, value: int) -> "Query":
        query = self._clone()
        query._limit = value
        return query

    def offset(self, value: int) -> "Query":
        query = self._clone()
        query._offset = value
        return query

    def where(self, *args: List[ClauseElement], **kwargs: Any) -> "Query":
        exprs = []

        for arg in args:
            if isinstance(arg, ClauseElement):
                exprs.append(arg)

        for name, value in kwargs.items():
            column = self._get_column(name)

            if isinstance(value, list):
                exprs.append(column.in_(value))
            else:
                exprs.append(column == value)

        query = self._clone()
        if query._where is None:
            query._where = and_(*exprs)
        else:
            query._where = and_(query._where, *exprs)
        return query

    def order_by(self, *args: List[Union[str, ClauseElement]]) -> "Query":
        exprs = []

        for arg in args:
            if isinstance(arg, ClauseElement):
                exprs.append(arg)
            elif isinstance(arg, str) and arg.startswith("-"):
                column = self._get_column(arg[1:])
                exprs.append(column.desc())
            elif isinstance(arg, str):
                column = self._get_column(arg)
                exprs.append(column.asc())

        query = self._clone()
        query._order_by = query._order_by + exprs
        return query

    def _get_column(self, name: str) -> Column:
        return self.model.__attributes__[name]

    def _build_query(self, sql: ClauseElement) -> ClauseElement:
        if self._limit is not None:
            sql = sql.limit(self._limit)

        if self._offset is not None:
            sql = sql.offset(self._offset)

        if self._where is not None:
            sql = sql.where(self._where)

        if self._order_by:
            sql = sql.order_by(*self._order_by)

        return sql

    def _clone(self) -> "Query":
        query = self.__class__(self.model)
        query._where = self._where
        query._limit = self._limit
        query._offset = self._offset
        query._order_by = self._order_by
        return query


Queryable = Union[Model, Query]


def to_query(queryable: Queryable) -> Query:
    if isinstance(queryable, Query):
        return queryable

    if isinstance(queryable, type) and issubclass(queryable, Model):
        return Query(queryable)

    raise AssertionError(f"{queryable} is not a queryable object.")
