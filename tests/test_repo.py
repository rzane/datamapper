import pytest
from datamapper import Repo, Query
from tests.support import database, User, Home, Pet

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


@pytest.mark.asyncio
async def test_preload_belongs_to():
    user = await repo.insert(User)
    home = await repo.insert(Home, owner_id=user.id)

    await repo.preload(home, "owner")
    assert home.owner.id == user.id


@pytest.mark.asyncio
async def test_preload_has_one():
    user = await repo.insert(User)
    home = await repo.insert(Home, owner_id=user.id)

    await repo.preload(user, "home")
    assert user.home.id == home.id


@pytest.mark.asyncio
async def test_preload_has_many():
    user = await repo.insert(User)
    pet = await repo.insert(Pet, owner_id=user.id)

    await repo.preload(user, "pets")
    assert len(user.pets) == 1
    assert user.pets[0].id == pet.id


@pytest.mark.asyncio
async def test_preload_multiple():
    user = await repo.insert(User)
    home = await repo.insert(Home, owner_id=user.id)
    pet = await repo.insert(Pet, owner_id=user.id)

    await repo.preload(user, ["home", "pets"])
    assert user.home.id == home.id
    assert user.pets[0].id == pet.id


@pytest.mark.asyncio
async def test_preload_collection():
    user1 = await repo.insert(User)
    user2 = await repo.insert(User)
    home1 = await repo.insert(Home, owner_id=user1.id)
    home2 = await repo.insert(Home, owner_id=user2.id)

    await repo.preload([user1, user2], "home")
    assert user1.home.id == home1.id
    assert user2.home.id == home2.id


@pytest.mark.asyncio
async def test_preload_nested():
    user = await repo.insert(User)
    home = await repo.insert(Home, owner_id=user.id)
    pet = await repo.insert(Pet, owner_id=user.id)

    await repo.preload(pet, "owner.home.owner")
    assert pet.owner.id == user.id
    assert pet.owner.home.id == home.id
    assert pet.owner.home.owner.id == user.id


async def list_users(**values):
    return [user.name for user in await repo.all(Query(User).order_by("id"))]
