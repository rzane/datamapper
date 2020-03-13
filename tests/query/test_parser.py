from datamapper.query.parser import parse_order, parse_where, parse_select


def test_parse_select() -> None:
    alias_name, name = parse_select("name")
    assert alias_name is None
    assert name == "name"


def test_parse_select_alias() -> None:
    alias_name, name = parse_select("p__name")
    assert alias_name == "p"
    assert name == "name"


def test_parse_order() -> None:
    alias_name, name, direction = parse_order("name")
    assert alias_name is None
    assert name == "name"
    assert direction == "asc"


def test_parse_order_desc() -> None:
    alias_name, name, direction = parse_order("-name")
    assert alias_name is None
    assert name == "name"
    assert direction == "desc"


def test_parse_order_alias() -> None:
    alias_name, name, direction = parse_order("-p__name")
    assert alias_name == "p"
    assert name == "name"
    assert direction == "desc"


def test_parse_where() -> None:
    alias_name, name, op = parse_where("name")
    assert alias_name is None
    assert name == "name"
    assert op == "__eq__"


def test_parse_where_operation() -> None:
    alias_name, name, op = parse_where("name__like")
    assert alias_name is None
    assert name == "name"
    assert op == "like"


def test_parse_where_alias() -> None:
    alias_name, name, op = parse_where("p__name__like")
    assert alias_name == "p"
    assert name == "name"
    assert op == "like"
