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
        model = to_model(queryable)
        rows = await self.database.fetch_all(to_sql(queryable))
        return [deserialize(row, model) for row in rows]

    async def one(self, queryable):
        records = await self.all(queryable)

        if len(records) == 1:
            return records[0]

        if not records:
            raise NoResultsError()

        raise MultipleResultsError(f"Expected at most one result, got {count}")

    async def get_by(self, queryable, **values):
        return await self.one(to_query(queryable).where(**values))

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


def to_sql(queryable: Queryable) -> ClauseElement:
    if isinstance(queryable, Query):
        return queryable.to_query()
    elif isinstance(queryable, type) and issubclass(queryable, Model):
        return queryable.__table__.select()
    else:
        raise AssertionError(f"{queryable} is not a queryable object.")


def to_query(queryable: Queryable) -> Query:
    if isinstance(queryable, type) and issubclass(queryable, Model):
        return Query(queryable)
    elif isinstance(queryable, Query):
        return queryable
    else:
        raise AssertionError(f"{queryable} is not a queryable object.")


def to_model(queryable: Queryable) -> Type[Model]:
    if isinstance(queryable, type) and issubclass(queryable, Model):
        return queryable
    elif isinstance(queryable, Query):
        return queryable.model
    else:
        raise AssertionError(f"{queryable} is not a queryable object.")


def deserialize(row: Mapping, model: Type[Model]) -> Model:
    return model(
        **{name: row[column.name] for name, column in model.__attributes__.items()}
    )
