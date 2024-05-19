import os

import asyncpg
from async_db_polars.pgdb import PGDB
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ["DATABASE_URL"]


async def init():
    pool = await asyncpg.create_pool(DATABASE_URL)
    if pool is None:
        raise Exception("Could not connect to database")
    return pool


async def close():
    await DB.close()


DB = PGDB(init)
