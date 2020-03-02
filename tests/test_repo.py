import pytest
from sqlalchemy import text
from datamapper import Repo, Query
from tests.support import database, User

repo = Repo(database)


@pytest.mark.asyncio
async def test_all():
    await repo.insert(User, name="Foo")

    users = await repo.all(Query(User))
    assert users[0].name == "Foo"

    users = await repo.all(User)
    assert users[0].name == "Foo"


@pytest.mark.asyncio
async def test_one():
    await repo.insert(User, name="Foo")

    user = await repo.one(Query(User))
    assert user.name == "Foo"

    user = await repo.one(User)
    assert user.name == "Foo"


@pytest.mark.asyncio
async def test_get_by():
    await repo.insert(User, name="Foo")

    user = await repo.get_by(Query(User), name="Foo")
    assert user.name == "Foo"

    user = await repo.get_by(User, name="Foo")
    assert user.name == "Foo"


@pytest.mark.asyncio
async def test_get():
    user = await repo.insert(User, name="Foo")
    user_id = user.id

    user = await repo.get(Query(User), user_id)
    assert user.id == user_id

    user = await repo.get(User, user_id)
    assert user.id == user_id


@pytest.mark.asyncio
async def test_count():
    assert await repo.count(User) == 0
    assert await repo.insert(User, name="Foo")
    assert await repo.count(User) == 1


@pytest.mark.asyncio
async def test_insert():
    user = await repo.insert(User, name="Foo")
    assert isinstance(user, User)
    assert user.id is not None
    assert await list_users() == ["Foo"]


@pytest.mark.asyncio
async def test_update():
    await repo.insert(User, name="Foo")
    user = await repo.insert(User, name="Bar")
    user = await repo.update(user, name="Changed Bar")
    assert isinstance(user, User)
    assert user.name == "Changed Bar"
    assert await list_users() == ["Foo", "Changed Bar"]


@pytest.mark.asyncio
async def test_delete():
    await repo.insert(User, name="Foo")
    user = await repo.insert(User, name="Bar")
    user = await repo.delete(user)
    assert isinstance(user, User)
    assert await list_users() == ["Foo"]


@pytest.mark.asyncio
async def test_update_all():
    await repo.insert(User, name="Foo")
    await repo.insert(User, name="Bar")
    await repo.insert(User, name="Foo")
    await repo.update_all(Query(User).where(name="Foo"), name="Buzz")
    assert await list_users() == ["Buzz", "Bar", "Buzz"]


@pytest.mark.asyncio
async def test_delete_all():
    await repo.insert(User, name="Foo")
    await repo.insert(User, name="Bar")
    await repo.insert(User, name="Foo")
    await repo.delete_all(Query(User).where(name="Foo"))
    assert await list_users() == ["Bar"]


async def list_users(**values):
    return [user.name for user in await repo.all(Query(User).order_by("id"))]
