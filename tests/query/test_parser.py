from datamapper.query.parser import parse_column, parse_order, parse_where


def test_parse_column() -> None:
    name, alias = parse_column("name")
    assert name == "name"
    assert alias is None


def test_parse_column_alias() -> None:
    name, alias = parse_column("p__name")
    assert name == "name"
    assert alias == "p"


def test_parse_order() -> None:
    name, direction = parse_order("name")
    assert name == "name"
    assert direction == "asc"


def test_parse_order_desc() -> None:
    name, direction = parse_order("-name")
    assert name == "name"
    assert direction == "desc"


def test_parse_order_alias() -> None:
    name, direction = parse_order("-p__name")
    assert name == "p__name"
    assert direction == "desc"


def test_parse_where() -> None:
    name, op = parse_where("name")
    assert name == "name"
    assert op == "__eq__"


def test_parse_where_operation() -> None:
    name, op = parse_where("name__like")
    assert name == "name"
    assert op == "like"


def test_parse_where_alias() -> None:
    name, op = parse_where("p__name__like")
    assert name == "p__name"
    assert op == "like"
