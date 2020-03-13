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


def parse_column(value: str) -> Tuple[str, Optional[str]]:
    """
    Parse a select expression.

    >>> parse_select("name")
    ("name", None)

    >>> parse_select("blog_posts__title")
    ("title", "blog_posts")
    """
    *parts, name = value.rsplit(SEPARATOR, 1)
    alias_name = parts[0] if parts else None
    return (name, alias_name)


def parse_order(value: str) -> Tuple[str, str]:
    """
    Parse an order expression.

    >>> parse_order("name")
    ("name", "asc")

    >>> parse_order("-name")
    ("name", "desc")

    >>> parse_order("blog_posts__title")
    ("blog_posts__title", "asc")
    """
    if value[0] == MINUS:
        return (value[1:], DESC)
    else:
        return (value, ASC)


def parse_where(value: str) -> Tuple[str, str]:
    """
    Parse an where expression.

    >>> parse_where("name")
    ("name", "__eq__")

    >>> parse_where("name__in")
    ("name", "in_")

    >>> parse_where("blog_posts__id__in")
    ("blog_posts__id", "in_")
    """

    parts = value.split(SEPARATOR)
    if parts[-1] in OPERATIONS:
        op = OPERATIONS[parts.pop()]
    else:
        op = EQUALS
    return (SEPARATOR.join(parts), op)
