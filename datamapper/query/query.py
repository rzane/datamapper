from __future__ import annotations
import datamapper.model as model
from datamapper._utils import to_tree
from datamapper.query.alias_tracker import AliasTracker
from typing import Any, List, Mapping, Optional, Union, Tuple, Type
from sqlalchemy import Table
from sqlalchemy.sql.expression import ClauseElement, Select, Update, Delete, FromClause


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

WhereClause = Union[ClauseElement, dict]
OrderClause = Union[ClauseElement, str]


class Query:
    __slots__ = [
        "_model",
        "_wheres",
        "_order_bys",
        "_limit",
        "_offset",
        "_joins",
        "_preloads",
    ]

    _model: Type[model.Model]
    _wheres: List[WhereClause]
    _order_bys: List[OrderClause]
    _joins: List[str]
    _limit: Optional[int]
    _offset: Optional[int]
    _preloads: List[str]

    def __init__(self, model: Type[model.Model]):
        self._model = model
        self._wheres = []
        self._order_bys = []
        self._joins = []
        self._limit = None
        self._offset = None
        self._preloads = []

    def to_query(self) -> Query:
        return self

    def to_sql(self) -> Select:
        return self.__compile(self._model.__table__.select())

    def to_update_sql(self) -> Update:
        return self.__compile(self._model.__table__.update())

    def to_delete_sql(self) -> Delete:
        return self.__compile(self._model.__table__.delete())

    def deserialize(self, row: Mapping) -> model.Model:
        return self._model.deserialize(row)

    def limit(self, value: int) -> Query:
        return self.__update(_limit=value)

    def offset(self, value: int) -> Query:
        return self.__update(_offset=value)

    def where(self, *args: ClauseElement, **kwargs: Any) -> Query:
        return self.__update(_wheres=self._wheres + list(args) + [kwargs])

    def order_by(self, *args: Union[str, ClauseElement]) -> Query:
        return self.__update(_order_bys=self._order_bys + list(args))

    def preload(self, preload: str) -> Query:
        return self.__update(_preloads=self._preloads + [preload])

    def join(self, join: str) -> Query:
        return self.__update(_joins=self._joins + [join])

    def __compile(self, sql: ClauseElement) -> ClauseElement:
        if self._wheres:
            sql = self.__build_where(sql)

        if self._order_bys:
            sql = self.__build_order(sql)

        if self._joins:
            sql = self.__build_joins(sql)

        if self._limit is not None:
            sql = sql.limit(self._limit)

        if self._offset is not None:
            sql = sql.offset(self._offset)

        return sql

    def __build_where(self, sql: ClauseElement) -> ClauseElement:
        for where in self._wheres:
            if isinstance(where, dict):
                for name, value in where.items():
                    name, op = _parse_where(name)
                    column = self._model.column(name)
                    clause = getattr(column, op)(value)
                    sql = sql.where(clause)
            else:
                sql = sql.where(where)
        return sql

    def __build_joins(self, sql: ClauseElement) -> ClauseElement:
        clause = _walk_joins(
            sql,
            self._model,
            self._model.__table__,
            to_tree(self._joins),
            AliasTracker(),
        )
        return sql.select_from(clause)

    def __build_order(self, sql: ClauseElement) -> ClauseElement:
        clauses = []
        for order_by in self._order_bys:
            if isinstance(order_by, str):
                direction = "asc"
                if order_by[0] == "-":
                    direction = "desc"
                    order_by = order_by[1:]
                column = self._model.column(order_by)
                clause = getattr(column, direction)()
                clauses.append(clause)
            else:
                clauses.append(order_by)
        return sql.order_by(*clauses)

    def __update(self, **kwargs: Any) -> Query:
        query = self.__class__(self._model)
        for key in self.__class__.__slots__:
            if key in kwargs:
                setattr(query, key, kwargs[key])
            else:
                setattr(query, key, getattr(self, key))
        return query


def _walk_joins(
    clause: FromClause,
    owner: Type[model.Model],
    owner_table: Table,
    tree: dict,
    tracker: AliasTracker,
) -> ClauseElement:
    for name, subtree in tree.items():
        assoc = owner.association(name)

        related_table = tracker.alias(assoc.related.__table__)
        related_column = getattr(related_table.c, assoc.related_key)
        owner_column = getattr(owner_table.c, assoc.owner_key)

        clause = clause.join(related_table, related_column == owner_column)
        clause = _walk_joins(clause, assoc.related, related_table, subtree, tracker)

    return clause


def _parse_where(name: str) -> Tuple[str, str]:
    op = "__eq__"
    parts = name.split(SEPARATOR)
    name = parts.pop()
    if name in OPERATIONS:
        op = OPERATIONS[name]
        name = parts.pop()
    return (name, op)
