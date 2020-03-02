from datamapper.associations import Association
from tests.support import User


def test_association():
    assoc = Association(User, "user_id")
    assert assoc.model == User
    assert assoc.foreign_key == "user_id"
    assert assoc.primary_key == "id"


def test_model_as_string():
    assoc = Association("tests.support.User", "user_id")
    assert assoc.model == User
    assert assoc.primary_key == "id"
