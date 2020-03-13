import os
import logging

from IPython import start_ipython
from databases import Database

from datamapper import Query, Repo
from tests.support import Home, Pet, User, provision_database

logging.basicConfig(level=logging.DEBUG)

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///console.db")

database = Database(DATABASE_URL)
repo = Repo(database)


context = {
    "database": database,
    "repo": repo,
    "Query": Query,
    "User": User,
    "Pet": Pet,
    "Home": Home,
}

if __name__ == "__main__":
    provision_database(DATABASE_URL)
    start_ipython(argv=[], user_ns=context)
