from typing import Type, Mapping, Union, List
from databases import Database
from datamapper.model import Model
from datamapper.query import Queryable, to_query
from datamapper.errors import NoResultsError, MultipleResultsError
from sqlalchemy import func
from sqlalchemy.sql.expression import Select, ClauseElement


class Repo:
    def __init__(self, database: Database):
        self.database = database

    async def all(self, queryable: Queryable) -> List[Model]:
        query = to_query(queryable)
        rows = await self.database.fetch_all(query.to_query())
        return [deserialize(row, query.model) for row in rows]

    async def one(self, queryable: Queryable) -> Model:
        records = await self.all(queryable)

        if len(records) == 1:
            return records[0]

        if not records:
            raise NoResultsError()

        raise MultipleResultsError(f"Expected at most one result, got {len(records)}")

    async def get_by(self, queryable: Queryable, **values: dict) -> Model:
        return await self.one(to_query(queryable).where(**values))

    async def get(self, queryable: Queryable, id: Union[str, int]) -> Model:
        return await self.get_by(queryable, id=id)

    async def count(self, queryable) -> int:
        sql = to_query(queryable).to_query().alias("subquery_for_count")
        sql = func.count().select().select_from(sql)
        return await self.database.fetch_val(sql)

    async def insert(self, model: Type[Model], **values: dict) -> Model:
        sql = model.__table__.insert().values(values)
        record_id = await self.database.execute(sql)
        return model(id=record_id, **values)

    async def update(self, record: Model, **values) -> Model:
        pk = record.__attributes__["id"]
        sql = record.__table__.update().where(pk == record.id).values(**values)
        await self.database.execute(sql)
        record._attributes.update(**values)
        return record

    async def delete(self, record: Model, **values: dict) -> Model:
        pk = record.__attributes__["id"]
        sql = record.__table__.delete().where(pk == record.id)
        await self.database.execute(sql)
        return record

    async def update_all(self, queryable: Queryable, **values) -> None:
        sql = to_query(queryable).to_update_query(**values)
        await self.database.execute(sql)

    async def delete_all(self, queryable: Queryable) -> None:
        sql = to_query(queryable).to_delete_query()
        await self.database.execute(sql)


def deserialize(row: Mapping, model: Type[Model]) -> Model:
    return model(
        **{name: row[column.name] for name, column in model.__attributes__.items()}
    )
