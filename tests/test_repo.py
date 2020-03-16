import pytest
from databases import Database
from sqlalchemy import text

from datamapper import Query, Repo
from datamapper.changeset import Changeset
from tests.support import DATABASE_URLS, Home, Pet, User, provision_database


@pytest.fixture(scope="session", autouse=True)
def setup_database(request):
    """Create database tables before running the test suite"""
    for url in DATABASE_URLS:
        provision_database(url)


@pytest.fixture(scope="function", params=DATABASE_URLS)
async def repo(request):
    async with Database(request.param, force_rollback=True) as database:
        yield Repo(database)


@pytest.mark.asyncio
async def test_all(repo):
    await repo.insert(User(name="Foo"))

    users = await repo.all(Query(User))
    assert users[0].name == "Foo"

    users = await repo.all(User)
    assert users[0].name == "Foo"


@pytest.mark.asyncio
async def test_first(repo):
    user = await repo.first(User)
    assert user is None

    await repo.insert(User(name="Foo"))

    user = await repo.first(Query(User))
    assert user.name == "Foo"

    user = await repo.first(User)
    assert user.name == "Foo"


@pytest.mark.asyncio
async def test_one(repo):
    await repo.insert(User(name="Foo"))

    user = await repo.one(Query(User))
    assert user.name == "Foo"

    user = await repo.one(User)
    assert user.name == "Foo"


@pytest.mark.asyncio
async def test_get_by(repo):
    await repo.insert(User(name="Foo"))

    user = await repo.get_by(Query(User), name="Foo")
    assert user.name == "Foo"

    user = await repo.get_by(User, name="Foo")
    assert user.name == "Foo"


@pytest.mark.asyncio
async def test_get(repo):
    user = await repo.insert(User(name="Foo"))
    user_id = user.id

    user = await repo.get(Query(User), user_id)
    assert user.id == user_id

    user = await repo.get(User, user_id)
    assert user.id == user_id


@pytest.mark.asyncio
async def test_count(repo):
    assert await repo.count(User) == 0
    assert await repo.insert(User(name="Foo"))
    assert await repo.count(User) == 1


@pytest.mark.asyncio
async def test_insert_can_take_a_model(repo):
    user = await repo.insert(User(name="Foo"))
    assert isinstance(user, User)
    assert user.id is not None
    assert await list_users(repo) == ["Foo"]


@pytest.mark.asyncio
async def test_insert_can_take_a_changeset(repo):
    changeset = Changeset(User()).cast({"name": "Foo"}, ["name"])
    await repo.insert(changeset)


@pytest.mark.asyncio
async def test_insert_belongs_to(repo):
    user = await repo.insert(User())

    home_changeset = Changeset(Home()).put_assoc("owner", user)
    home = await repo.insert(home_changeset)
    await repo.preload(home, "owner")
    assert home.owner_id == user.id
    assert home.owner.id == user.id

    home = await repo.get(Home, home.id)
    assert home.owner_id == user.id


@pytest.mark.asyncio
async def test_update(repo):
    await repo.insert(User(name="Foo"))
    user = await repo.insert(User(name="Bar"))
    user = await repo.update(Changeset(user).cast({"name": "Changed Bar"}, ["name"]))
    assert isinstance(user, User)
    assert user.name == "Changed Bar"
    assert await list_users(repo) == ["Foo", "Changed Bar"]


@pytest.mark.asyncio
async def test_update_belongs_to(repo):
    user = await repo.insert(User())
    home = await repo.insert(Home())

    home = await repo.update(Changeset(home).change({"owner": user}))
    assert home.owner_id == user.id
    assert home.owner.id == user.id

    home = await repo.get(Home, home.id)
    assert home.owner_id == user.id


@pytest.mark.asyncio
async def test_delete(repo):
    await repo.insert(User(name="Foo"))
    user = await repo.insert(User(name="Bar"))
    user = await repo.delete(user)
    assert isinstance(user, User)
    assert await list_users(repo) == ["Foo"]


@pytest.mark.asyncio
async def test_update_all(repo):
    await repo.insert(User(name="Foo"))
    await repo.insert(User(name="Foo"))
    await repo.update_all(User, name="Buzz")
    assert await list_users(repo) == ["Buzz", "Buzz"]


@pytest.mark.asyncio
async def test_update_all_query(repo):
    await repo.insert(User(name="Foo"))
    await repo.insert(User(name="Bar"))
    await repo.insert(User(name="Foo"))
    await repo.update_all(Query(User).where(name="Foo"), name="Buzz")
    assert await list_users(repo) == ["Buzz", "Bar", "Buzz"]


@pytest.mark.asyncio
async def test_delete_all(repo):
    await repo.insert(User(name="Foo"))
    await repo.insert(User(name="Bar"))
    await repo.delete_all(User)
    assert await list_users(repo) == []


@pytest.mark.asyncio
async def test_delete_all_query(repo):
    await repo.insert(User(name="Foo"))
    await repo.insert(User(name="Bar"))
    await repo.insert(User(name="Foo"))
    await repo.delete_all(Query(User).where(name="Foo"))
    assert await list_users(repo) == ["Bar"]


@pytest.mark.asyncio
async def test_preload_belongs_to(repo):
    user = await repo.insert(User())
    home = await repo.insert(Home(owner_id=user.id))

    await repo.preload(home, "owner")
    assert home.owner.id == user.id


@pytest.mark.asyncio
async def test_preload_has_one(repo):
    user = await repo.insert(User())
    home = await repo.insert(Home(owner_id=user.id))

    await repo.preload(user, "home")
    assert user.home.id == home.id


@pytest.mark.asyncio
async def test_preload_has_many(repo):
    user = await repo.insert(User())

    await repo.insert(Pet(owner_id=user.id))
    await repo.preload(user, "pets")
    assert len(user.pets) == 1

    await repo.insert(Pet(owner_id=user.id))
    await repo.preload(user, "pets")
    assert len(user.pets) == 2


@pytest.mark.asyncio
async def test_preload_multiple(repo):
    user = await repo.insert(User())
    home = await repo.insert(Home(owner_id=user.id))
    pet = await repo.insert(Pet(owner_id=user.id))

    await repo.preload(user, ["home", "pets"])
    assert user.home.id == home.id
    assert user.pets[0].id == pet.id


@pytest.mark.asyncio
async def test_preload_collection(repo):
    user1 = await repo.insert(User())
    user2 = await repo.insert(User())
    home1 = await repo.insert(Home(owner_id=user1.id))
    home2 = await repo.insert(Home(owner_id=user2.id))

    await repo.preload([user1, user2], "home")
    assert user1.home.id == home1.id
    assert user2.home.id == home2.id


@pytest.mark.asyncio
async def test_preload_nested(repo):
    user = await repo.insert(User())
    home = await repo.insert(Home(owner_id=user.id))
    pet = await repo.insert(Pet(owner_id=user.id))

    await repo.preload(pet, "owner.home.owner")
    assert pet.owner.id == user.id
    assert pet.owner.home.id == home.id
    assert pet.owner.home.owner.id == user.id


@pytest.mark.asyncio
async def test_preload_from_query(repo):
    user = await repo.insert(User())
    home = await repo.insert(Home(owner_id=user.id))

    home = await repo.one(home.to_query().preload("owner"))
    assert home.owner.id == user.id


@pytest.mark.asyncio
async def test_insert_from_valid_changeset(repo):
    changeset = Changeset(User()).cast({"name": "Richard"}, ["name"])
    user = await repo.insert(changeset)

    assert user.id
    assert user.name == "Richard"


@pytest.mark.asyncio
async def test_insert_from_invalid_changeset(repo):
    changeset = (
        Changeset(User()).cast({"name": "Richard"}, ["name"]).validate_required(["foo"])
    )
    invalid_changeset = await repo.insert(changeset)

    assert isinstance(invalid_changeset, Changeset)
    assert not invalid_changeset.is_valid


@pytest.mark.asyncio
async def test_update_from_valid_changeset(repo):
    user = await repo.insert(User())
    changeset = Changeset(user).cast({"name": "Richard"}, ["name"])
    updated_user = await repo.update(changeset)

    assert updated_user.id == user.id
    assert updated_user.name == "Richard"


@pytest.mark.asyncio
async def test_update_from_invalid_changeset(repo):
    user = await repo.insert(User())
    changeset = (
        Changeset(user).cast({"name": "Richard"}, ["name"]).validate_required(["foo"])
    )
    invalid_changeset = await repo.update(changeset)

    assert isinstance(invalid_changeset, Changeset)
    assert not invalid_changeset.is_valid


async def test_select_literal(repo):
    query = Query(User).select(text("1"))
    assert await repo.one(query) == 1


@pytest.mark.asyncio
async def test_select_name(repo):
    user = await repo.insert(User())
    query = Query(User).select("id")
    assert await repo.one(query) == user.id


@pytest.mark.asyncio
async def test_select_join_name(repo):
    user = await repo.insert(User())
    pet = await repo.insert(Pet(owner_id=user.id))
    query = Query(User).join("pets", "p").select("p__id")
    assert await repo.one(query) == pet.id


@pytest.mark.asyncio
async def test_select_list(repo):
    user = await repo.insert(User())
    pet = await repo.insert(Pet(owner_id=user.id))
    query = Query(User).join("pets", "p").select(["id", "p__id"])
    assert await repo.one(query) == [user.id, pet.id]


@pytest.mark.asyncio
async def test_select_tuple(repo):
    user = await repo.insert(User())
    pet = await repo.insert(Pet(owner_id=user.id))
    query = Query(User).join("pets", "p").select(("id", "p__id"))
    assert await repo.one(query) == (user.id, pet.id)


@pytest.mark.asyncio
async def test_select_dict(repo):
    user = await repo.insert(User())
    pet = await repo.insert(Pet(owner_id=user.id))
    query = Query(User).join("pets", "p").select({"user_id": "id", "pet_id": "p__id"})
    assert await repo.one(query) == {"user_id": user.id, "pet_id": pet.id}


@pytest.mark.asyncio
async def test_select_nested(repo):
    user = await repo.insert(User())
    pet = await repo.insert(Pet(owner_id=user.id))
    query = Query(User).join("pets", "p").select(("id", {"pet": [{"id": "p__id"}]}))
    assert await repo.one(query) == (user.id, {"pet": [{"id": pet.id}]})


async def list_users(repo):
    return [user.name for user in await repo.all(Query(User).order_by("id"))]
