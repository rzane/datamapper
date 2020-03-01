from typing import Any, List, Union
from datamapper.model import Model
from sqlalchemy import and_, or_, Column
from sqlalchemy.sql.expression import Select, TextClause


class Query:
    model: Model
    _where: Any
    _limit: Union[int, None]
    _offset: Union[int, None]
    _order_by: List[str]

    def __init__(self, model: Model):
        self.model = model
        self._limit = None
        self._offset = None
        self._where = None
        self._order_by = []

    def build(self):
        statement = self.model.__table__.select()

        if self._limit is not None:
            statement = statement.limit(self._limit)

        if self._offset is not None:
            statement = statement.offset(self._offset)

        if self._where is not None:
            statement = statement.where(self._where)

        if self._order_by:
            statement = statement.order_by(*self._order_by)

        return statement

    def limit(self, value):
        query = self._clone()
        query._limit = value
        return query

    def offset(self, value):
        query = self._clone()
        query._offset = value
        return query

    def where(self, *args, **kwargs):
        query = self._clone()
        exprs = self._build_where(*args, **kwargs)
        if query._where is None:
            query._where = and_(*exprs)
        else:
            query._where = and_(query._where, *exprs)
        return query

    def or_where(self, *args, **kwargs):
        query = self._clone()
        exprs = self._build_where(*args, **kwargs)
        if query._where is None:
            query._where = and_(*exprs)
        else:
            query._where = or_(query._where, and_(*exprs))
        return query

    def order_by(self, *args):
        exprs = []

        for arg in args:
            if isinstance(arg, TextClause):
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

    def _build_where(self, *args, **kwargs):
        exprs = []

        for arg in args:
            if isinstance(arg, TextClause):
                exprs.append(arg)

        for name, value in kwargs.items():
            column = self._get_column(name)
            if isinstance(value, list):
                exprs.append(column.in_(value))
            else:
                exprs.append(column == value)

        return exprs

    def _get_column(self, name) -> Column:
        return getattr(self.model.__table__.columns, name)

    def _clone(self):
        query = Query(self.model)
        query._where = self._where
        query._limit = self._limit
        query._offset = self._offset
        query._order_by = self._order_by
        return query
