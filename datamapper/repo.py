from databases import Database
from datamapper.query import Query
from datamapper.errors import NoResultsError, MultipleResultsError
from sqlalchemy.sql.expression import Select, ClauseElement


class Repo:
    def __init__(self, database: Database):
        self.database = database

    async def all(self, queryable):
        rows = await self.database.fetch_all(queryable.to_query())
        return [queryable.deserialize_row(row) for row in rows]

    async def one(self, queryable):
        rows = await self.database.fetch_all(queryable.to_query())
        count = len(rows)

        if count == 1:
            return queryable.deserialize_row(rows[0])

        if count == 0:
            raise NoResultsError()

        raise MultipleResultsError(f"Expected at most one result, got {count}")

    async def get(self, queryable, id):
        pass

    async def get_by(self, queryable, **values):
        pass

    async def count(self, queryable):
        pass

    async def insert(self, model, **values):
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
