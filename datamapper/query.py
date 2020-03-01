from datamapper.model import Model
from sqlalchemy import desc
from sqlalchemy.sql.expression import Select


class Query:
    model: Model
    _value: Select

    def __init__(self, model: Model, _statement: Select = None):
        self.model = model

        if _statement is not None:
            self.statement = _statement
        else:
            self.statement = self.model.__table__.select()

    def limit(self, value):
        return Query(self.model, self.statement.limit(value))

    def offset(self, value):
        return Query(self.model, self.statement.offset(value))

    def where(self, **conditions):
        statement = self.statement
        columns = self.model.__table__.columns

        for key, value in conditions.items():
            column = getattr(columns, key)

            if isinstance(value, list):
                statement = statement.where(column.in_(value))
            else:
                statement = statement.where(column == value)

        return Query(self.model, statement)

    def order_by(self, value):
        if value.startswith("-"):
            value = desc(value[1:])

        return Query(self.model, self.statement.order_by(value))
