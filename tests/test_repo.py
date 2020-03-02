import pytest
from datamapper import Repo
from sqlalchemy import text
from tests.support import database, User

repo = Repo(database)


@pytest.mark.asyncio
async def test_all():
    rows = await repo.all(text("select 1 as value"))
    assert rows[0]["value"] == 1


@pytest.mark.asyncio
async def test_one():
    rows = await repo.one(text("select 1 as value"))
    assert rows["value"] == 1
