from typing import Any, Type, Union, List, Optional, Protocol
from sqlalchemy import func
from databases import Database
from datamapper.query import Query
from datamapper.model import Model
from datamapper._utils import cast_list, expand_preloads, assert_one, collect_sql_values


class Queryable(Protocol):
    def to_query(self) -> Query:
        ...  # pragma: no cover


class Repo:
    def __init__(self, database: Database):
        self.database = database

    async def all(self, queryable: Queryable) -> List[Model]:
        query = queryable.to_query()
        rows = await self.database.fetch_all(query.to_sql())
        return [query.deserialize(row) for row in rows]

    async def first(self, queryable: Queryable) -> Optional[Model]:
        query = queryable.to_query().limit(1)
        rows = await self.database.fetch_all(query.to_sql())
        return query.deserialize(rows[0]) if rows else None

    async def one(self, queryable: Queryable) -> Model:
        query = queryable.to_query()
        rows = await self.database.fetch_all(query.to_sql())
        assert_one(rows)
        return query.deserialize(rows[0])

    async def get_by(self, queryable: Queryable, **values: Any) -> Model:
        return await self.one(queryable.to_query().where(**values))

    async def get(self, queryable: Queryable, id: Union[str, int]) -> Model:
        return await self.one(queryable.to_query().where(id=id))

    async def count(self, queryable: Queryable) -> int:
        sql = queryable.to_query().to_sql()
        sql = sql.alias("subquery_for_count")
        sql = func.count().select().select_from(sql)
        return await self.database.fetch_val(sql)

    async def insert(self, model: Type[Model], **values: Any) -> Model:
        sql_values = collect_sql_values(model, values)
        sql = model.__table__.insert().values(**sql_values)
        record_id = await self.database.execute(sql)
        return model(id=record_id, **values)

    async def update(self, record: Model, **values: Any) -> Model:
        pk = record.__attributes__["id"]
        sql_values = collect_sql_values(record, values)
        sql = record.__table__.update()
        sql = sql.where(pk == record.id)
        sql = sql.values(**sql_values)
        await self.database.execute(sql)
        for key, value in values.items():
            setattr(record, key, value)

        return record

    async def delete(self, record: Model, **values: Any) -> Model:
        pk = record.__attributes__["id"]
        sql = record.__table__.delete()
        sql = sql.where(pk == record.id)
        await self.database.execute(sql)
        return record

    async def update_all(self, queryable: Queryable, **values: Any) -> None:
        query = queryable.to_query()
        sql = query.to_update_sql()
        sql = sql.values(**values)
        await self.database.execute(sql)

    async def delete_all(self, queryable: Queryable) -> None:
        query = queryable.to_query()
        sql = query.to_delete_sql()
        await self.database.execute(sql)

    async def preload(
        self, records: Union[Model, List[Model]], preloads: List[str]
    ) -> None:
        records = cast_list(records)
        preloads = expand_preloads(cast_list(preloads))
        await self.__preload(records, preloads)

    async def __preload(self, parents: List[Model], preloads: dict) -> None:
        for name, subpreloads in preloads.items():
            model = parents[0].__class__
            assoc = model.association(name)
            query = assoc.to_query(parents)
            children = await self.all(query)
            assoc.populate(parents, children, name)
            await self.__preload(children, subpreloads)
