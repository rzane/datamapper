from datamapper.query.parser import parse_order, parse_where, parse_select


def test_parse_select() -> None:
    name, alias_name = parse_select("name")
    assert name == "name"
    assert alias_name is None


def test_parse_select_alias() -> None:
    name, alias_name = parse_select("p__name")
    assert name == "name"
    assert alias_name == "p"


def test_parse_order() -> None:
    name, direction, alias_name = parse_order("name")
    assert name == "name"
    assert direction == "asc"
    assert alias_name is None


def test_parse_order_desc() -> None:
    name, direction, alias_name = parse_order("-name")
    assert name == "name"
    assert direction == "desc"
    assert alias_name is None


def test_parse_order_alias() -> None:
    name, direction, alias_name = parse_order("-p__name")
    assert name == "name"
    assert direction == "desc"
    assert alias_name == "p"


def test_parse_where() -> None:
    name, op, alias_name = parse_where("name")
    assert name == "name"
    assert op == "__eq__"
    assert alias_name is None


def test_parse_where_operation() -> None:
    name, op, alias_name = parse_where("name__like")
    assert name == "name"
    assert op == "like"
    assert alias_name is None


def test_parse_where_alias() -> None:
    name, op, alias_name = parse_where("p__name__like")
    assert name == "name"
    assert op == "like"
    assert alias_name == "p"
