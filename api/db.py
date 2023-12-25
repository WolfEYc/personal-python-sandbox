import os
from typing import Optional

import asyncpg
import polars as pl
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ["DATABASE_URL"]


class DB:
    pool: asyncpg.Pool

    async def init(self):
        pool = await asyncpg.create_pool(DATABASE_URL)
        if pool is None:
            raise ValueError("Could not create connection pool")
        self.pool = pool

    async def close(self):
        await self.pool.close()

    async def fetch_lf(self, query: str, *args, timeout: Optional[int] = None):
        res = await db.pool.fetch(query, *args, timeout=timeout)
        if len(res) == 0:
            return pl.LazyFrame()
        res_values = map(lambda r: list(r.values()), res)
        res_columns = list(res[0].keys())
        lf = pl.LazyFrame(res_values, res_columns)
        return lf


db = DB()
