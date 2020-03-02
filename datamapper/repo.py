from databases import Database
from datamapper.exceptions import NoResultsError, MultipleResultsError


class Repo:
    def __init__(self, database: Database):
        self.database = database

    async def all(self, queryable):
        return await self.database.fetch_all(queryable)

    async def one(self, queryable):
        rows = await self.database.fetch_all(queryable)
        count = len(rows)

        if count == 1:
            return rows[0]
        elif count == 0:
            raise NoResultsError()
        else:
            raise MultipleResultsError(f"Expected at most one result, got {count}")

    async def get(self, queryable, id):
        pass

    async def get_by(self, queryable, **values):
        pass

    async def count(self, queryable):
        pass

    async def insert(self, model, **values):
        pass

    async def update(self, model, **values):
        pass

    async def delete(self, model, **values):
        pass

    async def update_all(self, model, **values) -> int:
        pass

    async def delete_all(self, queryable) -> int:
        pass
