from typing import Any, Type, Union, List, Optional
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

    async def preload(
        self, records: Union[Model, List[Model]], preloads: Union[str, List[str]],
    ) -> None:
        if not records or not preloads:
            return

        if not isinstance(records, list):
            records = [records]

        if not isinstance(preloads, list):
            preloads = [preloads]

        # FIXME: Avoid loading an association twice
        for preload in preloads:
            parents = records
            for name in preload.split("."):
                assoc = parents[0].__associations__[name]
                query = assoc.query(parents)
                children = await self.all(query)
                assoc.populate(parents, children, name)
                parents = children


def _assert_one(rows: list) -> None:
    if not rows:
        raise NoResultsError()

    if len(rows) > 1:
        raise MultipleResultsError(f"Expected at most one result, got {len(rows)}")
