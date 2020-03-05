from __future__ import annotations
import datamapper.model as model
from typing import Any, List, Mapping, Union, Optional, Type
from sqlalchemy import and_, Column
from sqlalchemy.sql.expression import (
    ClauseElement,
    ClauseList,
    Select,
    Update,
    Delete,
    Join,
    join,
)


class Query:
    _model: Type[model.Model]
    _where: Optional[ClauseList]
    _limit: Optional[int]
    _offset: Optional[int]
    _order_by: List[str]
    _joins: List[Join]
    preloads: List[str]

    def __init__(self, model: Type[model.Model]):
        self._model = model
        self._limit = None
        self._offset = None
        self._where = None
        self._order_by = []
        self._joins = []
        self.preloads = []

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
        query = self.__clone()
        query._limit = value
        return query

    def offset(self, value: int) -> Query:
        query = self.__clone()
        query._offset = value
        return query

    def where(self, *args: ClauseElement, **kwargs: Any) -> Query:
        exprs = []

        for arg in args:
            if isinstance(arg, ClauseElement):
                exprs.append(arg)

        for name, value in kwargs.items():
            column = self.__column(name)

            if isinstance(value, list):
                exprs.append(column.in_(value))
            else:
                exprs.append(column == value)

        query = self.__clone()
        if query._where is None:
            query._where = and_(*exprs)
        else:
            query._where = and_(query._where, *exprs)
        return query

    def order_by(self, *args: Union[str, ClauseElement]) -> Query:
        exprs = []

        for arg in args:
            if isinstance(arg, ClauseElement):
                exprs.append(arg)
            elif isinstance(arg, str) and arg.startswith("-"):
                column = self.__column(arg[1:])
                exprs.append(column.desc())
            elif isinstance(arg, str):
                column = self.__column(arg)
                exprs.append(column.asc())

        query = self.__clone()
        query._order_by = query._order_by + exprs
        return query

    def preload(self, preload: str) -> Query:
        query = self.__clone()
        query.preloads = query.preloads + [preload]
        return query

    def join(self, path: str, outer: bool = False, full: bool = False) -> Query:
        assoc = _resolve_association(self._model, path)
        related_key = assoc.related.column(assoc.related_key)
        owner_key = assoc.owner.column(assoc.owner_key)
        value = join(
            assoc.owner.__table__,
            assoc.related.__table__,
            related_key == owner_key,
            isouter=outer,
            full=full,
        )

        query = self.__clone()
        query._joins = query._joins + [value]
        return query

    def __build_query(self, sql: ClauseElement) -> ClauseElement:
        if self._limit is not None:
            sql = sql.limit(self._limit)

        if self._offset is not None:
            sql = sql.offset(self._offset)

        if self._where is not None:
            sql = sql.where(self._where)

        if self._order_by:
            sql = sql.order_by(*self._order_by)

        if self._joins:
            for join in self._joins:
                sql = sql.select_from(join)

        return sql

    def __column(self, name: str) -> Column:
        return self._model.column(name)

    def __clone(self) -> Query:
        query = self.__class__(self._model)
        query._where = self._where
        query._limit = self._limit
        query._offset = self._offset
        query._order_by = self._order_by
        query._joins = self._joins
        query.preloads = self.preloads
        return query


def _resolve_association(owner: Type[model.Model], path: str) -> model.Association:
    keys = path.split(".")
    last = keys.pop()
    for key in keys:
        assoc = owner.association(key)
        owner = assoc.related
    return owner.association(last)
