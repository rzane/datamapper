import pytest
from datamapper import Multi, Changeset
from datamapper.errors import InvalidChangesetError
from tests.support import User, Pet


@pytest.mark.asyncio
async def test_multi_insert(repo):
    multi = Multi().insert("user", User(name="Ray"))
    result = await repo.transaction(multi)

    assert isinstance(result["user"], User)
    assert result["user"].id


@pytest.mark.asyncio
async def test_multi_insert_chain(repo):
    multi = Multi().insert("user", User(name="Ray")).insert("pet", build_pet)
    result = await repo.transaction(multi)

    assert isinstance(result["pet"], Pet)
    assert result["pet"].id
    assert result["pet"].owner_id == result["user"].id


@pytest.mark.asyncio
async def test_multi_rollback(repo):
    multi = (
        Multi()
        .insert("valid", change_user(name="Ray"))
        .insert("invalid", change_user())
    )

    with pytest.raises(InvalidChangesetError):
        await repo.transaction(multi)

    assert await repo.count(User) == 0


def build_pet(context) -> Pet:
    return Pet(owner_id=context["user"].id)


def change_user(**params) -> Changeset:
    return Changeset(User()).cast(params, ["name"]).validate_required(["name"])
