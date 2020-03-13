import pytest
from sqlalchemy import text
from sqlalchemy.sql.expression import Select

from datamapper import Query
from datamapper.errors import MissingJoinError
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


def test_where_eq():
    query = Query(User).where(name__eq="Sue")
    assert "WHERE users.name = 'Sue'" in to_sql(query.to_sql())


def test_where_not_eq():
    query = Query(User).where(name__not_eq="Sue")
    assert "WHERE users.name != 'Sue'" in to_sql(query.to_sql())


def test_where_like():
    query = Query(User).where(name__like="Sue")
    assert "WHERE users.name LIKE 'Sue'" in to_sql(query.to_sql())


def test_where_not_like():
    query = Query(User).where(name__not_like="Sue")
    assert "WHERE users.name NOT LIKE 'Sue'" in to_sql(query.to_sql())


def test_where_ilike():
    query = Query(User).where(name__ilike="sue")
    assert "WHERE users.name ILIKE 'sue'" in to_sql(query.to_sql())


def test_where_not_ilike():
    query = Query(User).where(name__not_ilike="sue")
    assert "WHERE users.name NOT ILIKE 'sue'" in to_sql(query.to_sql())


def test_where_contains():
    query = Query(User).where(name__contains="u")
    assert "WHERE (users.name LIKE '%%' || 'u' || '%%')" in to_sql(query.to_sql())


def test_where_startswith():
    query = Query(User).where(name__startswith="S")
    assert "WHERE (users.name LIKE 'S' || '%%')" in to_sql(query.to_sql())


def test_where_endswith():
    query = Query(User).where(name__endswith="e")
    assert "WHERE (users.name LIKE '%%' || 'e')" in to_sql(query.to_sql())


def test_where_in():
    query = Query(User).where(id__in=[1, 2])
    assert "WHERE users.id IN (1, 2)" in to_sql(query.to_sql())


def test_where_gt():
    query = Query(User).where(id__gt=1)
    assert "WHERE users.id > 1" in to_sql(query.to_sql())


def test_where_gte():
    query = Query(User).where(id__gte=1)
    assert "WHERE users.id >= 1" in to_sql(query.to_sql())


def test_where_lt():
    query = Query(User).where(id__lt=1)
    assert "WHERE users.id < 1" in to_sql(query.to_sql())


def test_where_lte():
    query = Query(User).where(id__lte=1)
    assert "WHERE users.id <= 1" in to_sql(query.to_sql())


def test_multi_where():
    query = Query(User).where(id=1, name="Ray")
    assert "WHERE users.id = 1 AND users.name = 'Ray'" in to_sql(query.to_sql())


def test_consecutive_where():
    query = Query(User).where(id=1).where(name="Ray")
    assert "WHERE users.id = 1 AND users.name = 'Ray'" in to_sql(query.to_sql())


def test_where_literal():
    query = Query(User).where(text("users.id = 7"))
    assert "WHERE users.id = 7" in to_sql(query.to_sql())


def test_where_explicit_alias():
    query = Query(User).join("pets", "p").where(p__id=7)
    assert "WHERE p.id = 7" in to_sql(query.to_sql())


def test_where_implicit_alias():
    query = Query(User).join("pets").where(p0__id=7)
    assert "WHERE p0.id = 7" in to_sql(query.to_sql())


def test_order_by():
    query = Query(User).order_by("name")
    assert "ORDER BY users.name ASC" in to_sql(query.to_sql())


def test_order_by_desc():
    query = Query(User).order_by("-name")
    assert "ORDER BY users.name DESC" in to_sql(query.to_sql())


def test_order_by_literal():
    query = Query(User).order_by(text("1"))
    assert "ORDER BY 1" in to_sql(query.to_sql())


def test_order_by_explicit_alias():
    query = Query(User).join("pets", "p").order_by("p__id")
    assert "ORDER BY p.id ASC" in to_sql(query.to_sql())


def test_order_by_implicit_alias():
    query = Query(User).join("pets").order_by("p0__id")
    assert "ORDER BY p0.id ASC" in to_sql(query.to_sql())


def test_preload():
    query = Query(User).preload("home")
    assert query._preloads == ["home"]


def test_join():
    query = Query(User).join("pets")
    sql = to_sql(query.to_sql())
    assert "JOIN pets AS p0 ON p0.owner_id = users.id" in sql


def test_duplicate_join():
    query = Query(User).join("pets").join("pets")
    sql = to_sql(query.to_sql())
    assert sql.count("JOIN") == 1


def test_nested_join():
    query = Query(User).join("pets").join("pets.owner")
    sql = to_sql(query.to_sql())
    assert "JOIN pets AS p0 ON p0.owner_id = users.id" in sql
    assert "JOIN users AS u0 ON u0.id = p0.owner_id" in sql


def test_nested_join_back():
    query = Query(User).join("pets").join("pets.owner").join("pets.owner.pets")
    sql = to_sql(query.to_sql())
    assert "JOIN pets AS p0 ON p0.owner_id = users.id" in sql
    assert "JOIN users AS u0 ON u0.id = p0.owner_id" in sql
    assert "JOIN pets AS p1 ON p1.owner_id = u0.id" in sql


def test_missing_join():
    query = Query(User).join("pets.owner")

    with pytest.raises(MissingJoinError):
        query.to_sql()


def test_outerjoin():
    query = Query(User).outerjoin("pets")
    sql = to_sql(query.to_sql())
    assert "LEFT OUTER JOIN pets AS p0 ON p0.owner_id = users.id" in sql


def test_outerjoin_with_join():
    query = Query(User).join("pets").outerjoin("pets.owner")
    sql = to_sql(query.to_sql())
    assert "JOIN pets AS p0 ON p0.owner_id = users.id" in sql
    assert "LEFT OUTER JOIN users AS u0 ON u0.id = p0.owner_id" in sql


def test_outerjoin_duplicate():
    query = Query(User).join("pets").outerjoin("pets")
    sql = to_sql(query.to_sql())
    assert "JOIN pets AS p0 ON p0.owner_id = users.id" in sql
    assert "LEFT OUTER JOIN pets AS p1 ON p1.owner_id = users.id" in sql


def test_join_alias():
    query = Query(User).join("pets", "p")
    sql = to_sql(query.to_sql())
    assert "JOIN pets AS p ON p.owner_id = users.id" in sql


def test_join_duplicate_with_alias():
    query = Query(User).join("pets", "p").join("pets")
    sql = to_sql(query.to_sql())
    assert "JOIN pets AS p ON p.owner_id = users.id" in sql
    assert "JOIN pets AS p0 ON p0.owner_id = users.id" in sql


def test_select_literal():
    query = Query(User).select(text("1"))

    assert "SELECT 1" in to_sql(query.to_sql())
    assert query.deserialize({"?column?": 1}) == 1


def test_select_name():
    query = Query(User).select("id")

    assert "SELECT users.id" in to_sql(query.to_sql())
    assert query.deserialize({"id": 9}) == 9
