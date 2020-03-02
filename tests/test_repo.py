import pytest
from datamapper import Repo, SQL
from sqlalchemy import text
from tests.support import database, User

repo = Repo(database)


@pytest.mark.asyncio
async def test_all_sql():
    await insert_user(name="Ray")
    rows = await repo.all(SQL("select * from users"))
    assert rows[0]["name"] == "Ray"


@pytest.mark.asyncio
async def test_one_sql():
    await insert_user(name="Ray")
    row = await repo.one(SQL("select * from users"))
    assert row["name"] == "Ray"


async def insert_user(**values):
    await database.execute(User.__table__.insert().values(**values))
