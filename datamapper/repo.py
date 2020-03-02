from typing import Type, Mapping, Union
from databases import Database
from datamapper.model import Model
from datamapper.query import Query
from datamapper.errors import NoResultsError, MultipleResultsError
from sqlalchemy.sql.expression import Select, ClauseElement

Queryable = Union[Model, Query]


class Repo:
    def __init__(self, database: Database):
        self.database = database

    async def all(self, queryable):
        rows = await self.database.fetch_all(_to_sql(queryable))
        return [_build_row(queryable, row) for row in rows]

    async def one(self, queryable):
        rows = await self.database.fetch_all(_to_sql(queryable))

        if len(rows) == 1:
            return _build_row(queryable, rows[0])

        if not rows:
            raise NoResultsError()

        raise MultipleResultsError(f"Expected at most one result, got {count}")

    async def get_by(self, queryable, **values):
        return await self.one(_to_query(queryable).where(**values))

    async def get(self, queryable, id):
        return await self.get_by(queryable, id=id)

    async def count(self, queryable):
        pass

    async def insert(self, model: Type[Model], **values):
        pass

    async def update(self, record, **values):
        pass

    async def delete(self, record, **values):
        pass

    async def update_all(self, queryable, **values) -> int:
        pass

    async def delete_all(self, queryable, **values) -> int:
        pass


def _build_query(queryable):
    if isinstance(queryable, Query):
        return queryable.build()

    if isinstance(queryable, Select):
        return queryable

    if isinstance(queryable, ClauseElement):
        return queryable


def _to_sql(queryable: Queryable) -> ClauseElement:
    if isinstance(queryable, Query):
        return queryable.to_query()
    elif isinstance(queryable, type) and issubclass(queryable, Model):
        return queryable.__table__.select()
    else:
        raise AssertionError(f"{queryable} is not a queryable object.")


def _to_query(queryable: Queryable) -> Query:
    if isinstance(queryable, type) and issubclass(queryable, Model):
        return Query(queryable)
    elif isinstance(queryable, Query):
        return queryable
    else:
        raise AssertionError(f"{queryable} is not a queryable object.")


def _get_model(queryable: Queryable) -> Type[Model]:
    if isinstance(queryable, type) and issubclass(queryable, Model):
        return queryable
    elif isinstance(queryable, Query):
        return queryable.model
    else:
        raise AssertionError(f"{queryable} is not a queryable object.")


def _build_row(queryable: Queryable, row: Mapping) -> Model:
    model = _get_model(queryable)
    attributes = {
        name: row[column.name] for name, column in model.__attributes__.items()
    }
    return model(**attributes)
