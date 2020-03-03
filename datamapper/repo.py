from typing import Any, Type, Union, List, Optional, Dict
from databases import Database
from datamapper.queryable import Queryable
from datamapper.model import Model
from datamapper.errors import NoResultsError, MultipleResultsError
from sqlalchemy import func


class Repo:
    def __init__(self, database: Database):
        self.database = database

    async def all(self, query: Queryable) -> List[Model]:
        rows = await self.database.fetch_all(query.to_sql())
        return [query.deserialize(row) for row in rows]

    async def first(self, query: Queryable) -> Optional[Model]:
        rows = await self.database.fetch_one(query.to_sql())

        if rows:
            return query.deserialize(rows[0])
        else:
            return None

    async def one(self, query: Queryable) -> Model:
        rows = await self.database.fetch_all(query.to_sql())
        _assert_one(rows)
        return query.deserialize(rows[0])

    async def get_by(self, query: Queryable, **values: Any) -> Model:
        sql = query.to_sql()

        for key, value in values.items():
            col = query.column(key)
            sql = sql.where(col == value)

        rows = await self.database.fetch_all(query.to_sql())
        _assert_one(rows)
        return query.deserialize(rows[0])

    async def get(self, queryable: Queryable, id: Union[str, int]) -> Model:
        return await self.get_by(queryable, id=id)

    async def count(self, query: Queryable) -> int:
        sql = query.to_sql()
        sql = sql.alias("subquery_for_count")
        sql = func.count().select().select_from(sql)
        return await self.database.fetch_val(sql)

    async def insert(self, model: Type[Model], **values: Any) -> Model:
        sql = model.__table__.insert().values(values)
        record_id = await self.database.execute(sql)
        return model(id=record_id, **values)

    async def update(self, record: Model, **values: Any) -> Model:
        pk = record.__attributes__["id"]
        sql = record.__table__.update()
        sql = sql.where(pk == record.id)
        sql = sql.values(**values)
        await self.database.execute(sql)
        record._attributes.update(**values)
        return record

    async def delete(self, record: Model, **values: Any) -> Model:
        pk = record.__attributes__["id"]
        sql = record.__table__.delete()
        sql = sql.where(pk == record.id)
        await self.database.execute(sql)
        return record

    async def update_all(self, query: Queryable, **values: Any) -> None:
        await self.database.execute(query.to_update_sql().values(**values))

    async def delete_all(self, query: Queryable) -> None:
        await self.database.execute(query.to_delete_sql())

    # TODO: Use `asyncio.gather` to preload concurrently
    async def preload(
        self, records: Union[Model, List[Model]], preload: Union[str, List, Dict],
    ) -> None:
        # If we were given an empty list or `None`, we should stop.
        # In this scenario, we can't detect a model.
        if not records:
            return

        # Make sure the list of records is a list.
        if not isinstance(records, list):
            records = [records]

        # Preload multiple associations concurrently.
        if isinstance(preload, list):
            for value in preload:
                await self.preload(records, value)

        # Preload nested associations.
        elif isinstance(preload, dict):
            for key, value in preload.items():
                children = await self._preload_one(records, key)
                await self.preload(children, value)

        # Preload a single association
        elif isinstance(preload, str):
            await self._preload_one(records, preload)

        else:
            raise TypeError(f"Unable to preload: {preload}")

    async def _preload_one(self, parents: List[Model], preload: str) -> List[Model]:
        assoc = parents[0].__associations__[preload]
        query = assoc.query(parents)
        children = await self.all(query)
        assoc.populate(parents, children, preload)
        return children


def _assert_one(rows: list) -> None:
    if not rows:
        raise NoResultsError()

    if len(rows) > 1:
        raise MultipleResultsError(f"Expected at most one result, got {len(rows)}")
