import logging
import os

from databases import Database
from IPython import start_ipython

from datamapper import Query, Repo
from datamapper.changeset import Changeset
from tests.support import Home, Pet, User, provision_database

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("parso").setLevel(logging.ERROR)

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///console.db")

database = Database(DATABASE_URL)
repo = Repo(database)


context = {
    "database": database,
    "repo": repo,
    "Query": Query,
    "Changeset": Changeset,
    "User": User,
    "Pet": Pet,
    "Home": Home,
}

if __name__ == "__main__":
    provision_database(DATABASE_URL)
    start_ipython(argv=[], user_ns=context)
