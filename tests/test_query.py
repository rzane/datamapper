from sqlalchemy import text
from sqlalchemy.sql.expression import Select
from datamapper import Query
from tests.support import User, to_sql


def test_to_sql():
    query = Query(User)
    assert isinstance(query.to_sql(), Select)
    assert "SELECT" in to_sql(query.to_sql())
    assert "FROM users" in to_sql(query.to_sql())


def test_limit():
    query = Query(User).limit(29)
    assert "LIMIT 29" in to_sql(query.to_sql())


def test_offset():
    query = Query(User).offset(29)
    assert "OFFSET 29" in to_sql(query.to_sql())


def test_where():
    query = Query(User).where(id=1)
    assert "WHERE users.id = 1" in to_sql(query.to_sql())


def test_where_list():
    query = Query(User).where(id=[1, 2])
    assert "WHERE users.id IN (1, 2)" in to_sql(query.to_sql())


def test_multi_where():
    query = Query(User).where(id=1, name="Ray")
    assert "WHERE users.id = 1 AND users.name = 'Ray'" in to_sql(query.to_sql())


def test_consecutive_where():
    query = Query(User).where(id=1).where(name="Ray")
    assert "WHERE users.id = 1 AND users.name = 'Ray'" in to_sql(query.to_sql())


def test_where_literal():
    query = Query(User).where(text("users.id = 7"))
    assert "WHERE users.id = 7" in to_sql(query.to_sql())


def test_order_by():
    query = Query(User).order_by("name")
    assert "ORDER BY users.name ASC" in to_sql(query.to_sql())


def test_order_by_desc():
    query = Query(User).order_by("-name")
    assert "ORDER BY users.name DESC" in to_sql(query.to_sql())


def test_order_by_literal():
    query = Query(User).order_by(text("1"))
    assert "ORDER BY 1" in to_sql(query.to_sql())


def test_preload():
    query = Query(User).preload("home")
    assert query.preloads == ["home"]


def test_join():
    query = Query(User).join("home")
    assert "JOIN homes ON homes.owner_id = users.id" in to_sql(query.to_sql())


def test_join_outer():
    query = Query(User).join("home", outer=True)
    assert "OUTER JOIN homes ON homes.owner_id = users.id" in to_sql(query.to_sql())


def test_join_full():
    query = Query(User).join("home", full=True)
    assert "FULL OUTER JOIN homes ON homes.owner_id = users.id" in to_sql(
        query.to_sql()
    )


def test_join_nested():
    query = Query(User).join("home").join("home.owner").join("home.owner.pets")
    sql = to_sql(query.to_sql())
    assert "JOIN homes ON homes.owner_id = users.id" in sql
    assert "JOIN users ON users.id = homes.owner_id" in sql
    assert "JOIN pets ON pets.owner_id = users.id" in sql
