from typing import Optional, Tuple

ASC = "asc"
DESC = "desc"
MINUS = "-"
SEPARATOR = "__"
EQUALS = "__eq__"
OPERATIONS = {
    "eq": EQUALS,
    "like": "like",
    "ilike": "ilike",
    "not_eq": "__ne__",
    "not_like": "notlike",
    "not_ilike": "notilike",
    "contains": "contains",
    "startswith": "startswith",
    "endswith": "endswith",
    "in": "in_",
    "gt": "__gt__",
    "gte": "__ge__",
    "lt": "__lt__",
    "lte": "__le__",
}


def parse_select(value: str) -> Tuple[Optional[str], str]:
    """
    Parse a select expression.

    >>> parse_select("name")
    ("name", None)

    >>> parse_select("blog_posts__title")
    ("title", "blog_posts")
    """
    *parts, name = value.rsplit(SEPARATOR, 1)
    alias_name = parts[0] if parts else None
    return (alias_name, name)


def parse_order(value: str) -> Tuple[Optional[str], str, str]:
    """
    Parse an order expression.

    >>> parse_order("name")
    ("name", "asc", None)

    >>> parse_order("-name")
    ("name", "desc", None)

    >>> parse_order("blog_posts__title")
    ("title", "asc", "blog_posts")
    """

    direction = DESC if value[0] == MINUS else ASC
    value = value[1:] if value[0] == MINUS else value
    alias_name, name = parse_select(value)
    return (alias_name, name, direction)


def parse_where(value: str) -> Tuple[Optional[str], str, str]:
    """
    Parse an where expression.

    >>> parse_where("name")
    ("name", "__eq__", None)

    >>> parse_where("name__in")
    ("name", "in_", None)

    >>> parse_where("blog_posts__id__in")
    ("id", "in_", "blog_posts")
    """

    parts = value.split(SEPARATOR)
    op = EQUALS
    name = parts.pop()
    alias_name = None

    if name in OPERATIONS:
        op = OPERATIONS[name]
        name = parts.pop()

    if parts:
        alias_name = SEPARATOR.join(parts)

    return (alias_name, name, op)
