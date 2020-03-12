import logging

from IPython import start_ipython

from datamapper import Query, Repo
from tests.support import Home, Pet, User, database

logging.basicConfig(level=logging.DEBUG)

context = {
    "database": database,
    "repo": Repo(database),
    "Query": Query,
    "User": User,
    "Pet": Pet,
    "Home": Home,
}

if __name__ == "__main__":
    start_ipython(argv=[], user_ns=context)
