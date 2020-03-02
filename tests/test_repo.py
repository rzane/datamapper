import pytest
from sqlalchemy import text
from datamapper import Repo, Raw, Query
from tests.support import database, User

repo = Repo(database)


@pytest.mark.asyncio
async def test_all():
    await insert_user(name="Ray")

    users = await repo.all(Raw("select * from users"))
    assert users[0]["name"] == "Ray"

    users = await repo.all(Raw("select * from users", model=User))
    assert users[0].name == "Ray"

    users = await repo.all(Raw(User.__table__.select(), model=User))
    assert users[0].name == "Ray"

    users = await repo.all(Query(User))
    assert users[0].name == "Ray"

    users = await repo.all(User)
    assert users[0].name == "Ray"


@pytest.mark.asyncio
async def test_one():
    await insert_user(name="Ray")

    user = await repo.one(Raw("select * from users"))
    assert user["name"] == "Ray"

    user = await repo.one(Raw("select * from users", model=User))
    assert user.name == "Ray"

    user = await repo.one(Raw(User.__table__.select(), model=User))
    assert user.name == "Ray"

    user = await repo.one(Query(User))
    assert user.name == "Ray"

    user = await repo.one(User)
    assert user.name == "Ray"


async def insert_user(**values):
    await database.execute(User.__table__.insert().values(**values))
