from sqlalchemy import text
from sqlalchemy.sql.expression import Select
from datamapper import Query
from tests.support import User, to_sql


def test_to_query():
    query = Query(User).to_query()
    assert isinstance(query, Select)
    assert "SELECT" in to_sql(query)
    assert "FROM users" in to_sql(query)


def test_deserialize_row():
    row = {"id": 9, "name": "Ray"}
    query = Query(User)
    user = query.deserialize_row(row)
    assert user.id == 9
    assert user.name == "Ray"


def test_limit():
    query = Query(User).limit(29)
    assert "LIMIT 29" in to_sql(query.to_query())


def test_offset():
    query = Query(User).offset(29)
    assert "OFFSET 29" in to_sql(query.to_query())


def test_where():
    query = Query(User).where(id=1)
    assert "WHERE users.id = 1" in to_sql(query.to_query())


def test_where_list():
    query = Query(User).where(id=[1, 2])
    assert "WHERE users.id IN (1, 2)" in to_sql(query.to_query())


def test_multi_where():
    query = Query(User).where(id=1, name="Ray")
    assert "WHERE users.id = 1 AND users.name = 'Ray'" in to_sql(query.to_query())


def test_consecutive_where():
    query = Query(User).where(id=1).where(name="Ray")
    assert "WHERE users.id = 1 AND users.name = 'Ray'" in to_sql(query.to_query())


def test_where_literal():
    query = Query(User).where(text("users.id = 7"))
    assert "WHERE users.id = 7" in to_sql(query.to_query())


def test_order_by():
    query = Query(User).order_by("name")
    assert "ORDER BY users.name ASC" in to_sql(query.to_query())


def test_order_by_desc():
    query = Query(User).order_by("-name")
    assert "ORDER BY users.name DESC" in to_sql(query.to_query())


def test_order_by_literal():
    query = Query(User).order_by(text("1"))
    assert "ORDER BY 1" in to_sql(query.to_query())
