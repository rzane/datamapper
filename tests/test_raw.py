from datamapper import Raw
from tests.support import User
from sqlalchemy import text
from sqlalchemy.sql.expression import TextClause


def test_to_query():
    assert isinstance(Raw("").to_query(), str)
    assert isinstance(Raw(text("")).to_query(), TextClause)


def test_deserialize_row():
    row = {"id": 9, "name": "Ray"}
    assert Raw("").deserialize_row(row) == row


def test_deserialize_row_with_model():
    row = {"id": 9, "name": "Ray"}
    query = Raw("", model=User)
    user = query.deserialize_row(row)
    assert user.id == 9
    assert user.name == "Ray"
