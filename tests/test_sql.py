from datamapper import SQL
from tests.support import User
from sqlalchemy.sql.expression import TextClause


def test_to_query():
    assert isinstance(SQL("").to_query(), TextClause)


def test_deserialize_row():
    row = {"id": 9, "name": "Ray"}
    assert SQL("").deserialize_row(row) == row


def test_deserialize_row_with_model():
    row = {"id": 9, "name": "Ray"}
    query = SQL("", model=User)
    user = query.deserialize_row(row)
    assert user.id == 9
    assert user.name == "Ray"
