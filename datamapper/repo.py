from typing import Any, List, Optional, Type, Union

from databases import Database
from sqlalchemy import func
from typing_extensions import Protocol

from datamapper._utils import assert_one, to_list, to_tree
from datamapper.changeset import Changeset
from datamapper.model import Association, Cardinality, Model
from datamapper.query import Query


class Queryable(Protocol):
    def to_query(self) -> Query:
        ...  # pragma: no cover


class Repo:
    """
    A `Repo` is used to dispatch queries to the database.

    To communicate with the database, `Repo` uses the
    `databases <https://www.encode.io/databases/>`_ package.

    Example::

        import databases
        import datamapper

        database_url = os.environ["DATABASE_URL"]
        database = databases.Database(database_url)
        repo = datamapper.Repo(database)
    """

    def __init__(self, database: Database):
        self.database = database

    async def all(self, queryable: Queryable) -> List[Model]:
        """
        Fetches all entries from the database matching the given query.

        Examples::

            await repo.all(User)
            await repo.all(Query(User).where(name="Fred"))
        """
        query = queryable.to_query()
        rows = await self.database.fetch_all(query.to_sql())
        records = [query.deserialize(row) for row in rows]

        if query._preloads:
            await self.preload(records, query._preloads)

        return records

    async def first(self, queryable: Queryable) -> Optional[Model]:
        """
        Fetches a single result from the query. Returns `None` if no result was found.

        Examples::

            await repo.first(User)
            await repo.first(Query(User).order_by("name"))
        """
        query = queryable.to_query().limit(1)
        records = await self.all(query)
        return records[0] if records else None

    async def one(self, queryable: Queryable) -> Model:
        """
        Fetches a single result from the query. Raises `NoResultsError` when
        no results are found. Raises `MultipleResultsError` when more than one
        result is found.

        Examples::

            await repo.one(Query(User).where(id=1))
        """
        records = await self.all(queryable)
        assert_one(records)
        return records[0]

    async def get_by(self, queryable: Queryable, **values: Any) -> Model:
        """
        Similar to `get`, but allows to query a field other than ID.

        Examples::

            await repo.get_by(User, name="Fred")
        """
        return await self.one(queryable.to_query().where(**values))

    async def get(self, queryable: Queryable, id: Union[str, int]) -> Model:
        """
        Fetches a single result by it's primary key. Raises `NoResultsError`
        when no results are found. Raises `MultipleResultsError` when more than
        one result is found.

        Examples::

            await repo.get_by(User, name="Fred")
        """
        return await self.one(queryable.to_query().where(id=id))

    async def count(self, queryable: Queryable) -> int:
        """
        Get a count of the number of results in the query.

        Examples::

            await repo.count(User)
            await repo.count(Query(User).where(name="Fred"))
        """
        sql = queryable.to_query().to_sql()
        sql = sql.alias("subquery_for_count")
        sql = func.count().select().select_from(sql)
        return await self.database.fetch_val(sql)

    async def insert(
        self, model_or_changeset: Union[Type[Model], Changeset[Model]], **values: Any
    ) -> Union[Model, Changeset]:
        """
        Insert a record into the database.

        Examples::

            await repo.insert(User, name="Fred")
            await repo.insert(changeset)
        """
        changeset = cast_changeset(model_or_changeset, values)
        if not changeset.is_valid:
            return changeset

        model = changeset.data
        sql_values = _collect_sql_values(model, changeset.changes)
        sql = model.__table__.insert().values(**sql_values)
        record_id = await self.database.execute(sql)
        return changeset.change({"id": record_id}).apply_changes()

    async def update(self, changeset: Changeset) -> Union[Model, Changeset]:
        """
        Update a record in the database.

        Examples::

            user = await repo.get(User, 1)
            changeset = Changeset(user).cast({"name": "Fred"}, params=["name"])
            await repo.update(changeset)
        """
        if not changeset.is_valid:
            return changeset

        record = changeset.data
        query = record.to_query()
        sql_values = _collect_sql_values(record, changeset.changes)
        sql = query.to_update_sql().values(**sql_values)
        await self.database.execute(sql)
        return changeset.apply_changes()

    async def delete(self, record: Model, **values: Any) -> Model:
        """
        Delete a record from the database.

        Examples::

            user = await repo.get(User, 1)
            await repo.delete(user)
        """
        query = record.to_query()
        sql = query.to_delete_sql()
        await self.database.execute(sql)
        return record

    async def update_all(self, queryable: Queryable, **values: Any) -> None:
        """
        Update all entries matching the given query.

        Examples::

            await repo.update_all(User, name="Fred")
            await repo.update_all(Query(User).where(name="Fred"), name="Freddy")
        """
        query = queryable.to_query()
        sql = query.to_update_sql()
        sql = sql.values(**values)
        await self.database.execute(sql)

    async def delete_all(self, queryable: Queryable) -> None:
        """
        Delete all entries matching the given query.

        Examples::

            await repo.delete_all(User)
            await repo.delete_all(Query(User).where(name="Fred"))
        """
        query = queryable.to_query()
        sql = query.to_delete_sql()
        await self.database.execute(sql)

    async def preload(
        self, records: Union[Model, List[Model]], preloads: List[str]
    ) -> None:
        """
        Loads associations for the given record or records.

        This is similar to `Query.preload` except it allows you to preload
        associations after records have been fetched from the database.

        Examples::

            authors = await repo.all(Author)
            await repo.preload(authors, ["biography", "posts.comments"])
        """
        records = to_list(records)
        preloads = to_tree(to_list(preloads))
        await self.__preload(records, preloads)

    async def __preload(self, owners: List[Model], preloads: dict) -> None:
        for name, subpreloads in preloads.items():
            model = owners[0].__class__
            assoc = model.association(name)

            values = [getattr(r, assoc.owner_key) for r in owners]
            where = {f"{assoc.related_key}__in": values}
            query = Query(assoc.related).where(**where)
            preloaded = await self.all(query)
            _resolve_preloads(owners, preloaded, assoc)
            await self.__preload(preloaded, subpreloads)


def cast_changeset(
    model_or_changeset: Union[Model, Type[Model], Changeset], values: dict
) -> Changeset:
    if isinstance(model_or_changeset, Model):
        return Changeset(model_or_changeset).cast(values, list(values.keys()))
    elif isinstance(model_or_changeset, type(Model)):
        return Changeset(model_or_changeset()).cast(values, list(values.keys()))
    elif isinstance(model_or_changeset, Changeset):
        return model_or_changeset
    else:
        raise ValueError("Must be Model instance, class or Changeset.")


def _resolve_preloads(
    owners: List[Model], preloaded: List[Model], assoc: Association
) -> None:
    lookup: dict = {}
    for related in preloaded:
        key = getattr(related, assoc.related_key)
        if assoc.cardinality == Cardinality.ONE:
            lookup[key] = related
        elif key in lookup:
            lookup[key].append(related)
        else:
            lookup[key] = [related]
    for owner in owners:
        key = getattr(owner, assoc.owner_key)
        if assoc.cardinality == Cardinality.ONE:
            setattr(owner, assoc.name, lookup.get(key, None))
        else:
            setattr(owner, assoc.name, lookup.get(key, []))


def _collect_sql_values(model: Union[Model, Type[Model]], values: dict) -> dict:
    """
    Convert attributes and association values to values that will be stored
    in the database.
    """

    sql_values = {}
    for key, value in values.items():
        if key in model.__associations__:
            assoc = model.__associations__[key]
            sql_values.update(assoc.values(value))
        else:
            sql_values[key] = value
    return sql_values
