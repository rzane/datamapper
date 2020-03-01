from sqlalchemy.sql.expression import Select
from datamapper import Query
from tests.models import User, to_sql


def test_query():
    query = Query(User)
    assert query.model == User
    assert isinstance(query.statement, Select)


def test_limit():
    query = Query(User).limit(29)
    assert "LIMIT 29" in to_sql(query.statement)


def test_offset():
    query = Query(User).offset(29)
    assert "OFFSET 29" in to_sql(query.statement)


def test_where():
    query = Query(User).where(id=9, name="Ray")
    assert "WHERE users.id = 9 AND users.name = 'Ray'" in to_sql(query.statement)


def test_order_by():
    query = Query(User).order_by("name")
    assert "ORDER BY users.name" in to_sql(query.statement)


def test_order_by_desc():
    query = Query(User).order_by("-name")
    assert "ORDER BY users.name DESC" in to_sql(query.statement)
