from __future__ import annotations
import datamapper.model as model
from typing import Any, List, Mapping, Optional, Union, Tuple, Type
from sqlalchemy import Column, join, Table
from sqlalchemy.sql.expression import ClauseElement, Select, Update, Delete, Join


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
    _model: Type[model.Model]
    _joins: List[ClauseElement]
    _wheres: List[ClauseElement]
    _order_bys: List[str]
    _limit: Optional[int]
    _offset: Optional[int]
    preloads: List[str]

    def __init__(self, model: Type[model.Model]):
        self._model = model
        self._joins = []
        self._wheres = []
        self._order_bys = []
        self._limit = None
        self._offset = None
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
        wheres: List[ClauseElement] = []
        joins: List[Join] = []

        for arg in args:
            assert isinstance(arg, ClauseElement)
            wheres.append(arg)

        for name, value in kwargs.items():
            where, assoc_joins = _resolve_where(self._model, name, value)
            wheres.append(where)
            joins += assoc_joins

        query = self.__clone()
        query._joins = query._joins + joins
        query._wheres = query._wheres + wheres
        return query

    def order_by(self, *args: Union[str, ClauseElement]) -> Query:
        exprs = []

        for arg in args:
            if isinstance(arg, str) and arg.startswith("-"):
                column = self.__column(arg[1:])
                exprs.append(column.desc())
            elif isinstance(arg, str):
                column = self.__column(arg)
                exprs.append(column.asc())
            else:
                assert isinstance(arg, ClauseElement)
                exprs.append(arg)

        query = self.__clone()
        query._order_bys = query._order_bys + exprs
        return query

    def preload(self, preload: str) -> Query:
        query = self.__clone()
        query.preloads = query.preloads + [preload]
        return query

    def __build_query(self, sql: ClauseElement) -> ClauseElement:
        for join_clause in self._joins:
            sql = sql.select_from(join_clause)

        for where_clause in self._wheres:
            sql = sql.where(where_clause)

        if self._order_bys:
            sql = sql.order_by(*self._order_bys)

        if self._limit is not None:
            sql = sql.limit(self._limit)

        if self._offset is not None:
            sql = sql.offset(self._offset)

        return sql

    def __column(self, name: str) -> Column:
        return self._model.column(name)

    def __clone(self) -> Query:
        query = self.__class__(self._model)
        query._joins = self._joins
        query._wheres = self._wheres
        query._order_bys = self._order_bys
        query._limit = self._limit
        query._offset = self._offset
        query.preloads = self.preloads
        return query


def _resolve_where(
    model: Type[model.Model], name: str, value: Any
) -> Tuple[ClauseElement, List[Join]]:
    parts = name.split(SEPARATOR)
    op = "__eq__"
    name = parts.pop()
    joins = []
    assoc = None
    alias = ""

    if name in OPERATIONS:
        op = OPERATIONS[name]
        name = parts.pop()

    for i in range(len(parts)):
        assoc = model.association(parts[i])
        model = assoc.related

        join, alias = _build_join(assoc, parts[: i + 1])
        joins.append(join)

    column = _alias_column(model.column(name), alias)
    clause = getattr(column, op)(value)
    return (clause, joins)


def _build_join(assoc: model.Association, path: List[str]) -> Tuple[Join, str]:
    owner_alias = "_".join(path[:-1])
    owner_table = _alias_table(assoc.owner.__table__, owner_alias)
    owner_column = assoc.owner.column(assoc.owner_key)
    owner_column = _alias_column(owner_column, owner_alias)

    related_alias = "_".join(path)
    related_table = _alias_table(assoc.related.__table__, related_alias)
    related_column = assoc.related.column(assoc.related_key)
    related_column = _alias_column(related_column, related_alias)

    join_clause = join(owner_table, related_table, related_column == owner_column)
    return (join_clause, related_alias)


def _alias_table(table: Table, alias: str) -> Table:
    return table.alias(alias) if alias else table


def _alias_column(column: Column, alias: str) -> Column:
    if alias:
        return column.table.alias(alias).columns.get(column.key)
    else:
        return column
