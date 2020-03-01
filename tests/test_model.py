import pytest
from datamapper import Model


class User(Model):
    pass


def test_read_attribute():
    assert User(name="Ray").name == "Ray"


def test_read_invalid_attribute():
    with pytest.raises(AttributeError):
        User().invalid_attribute
