import os
from typing import Optional

import asyncpg
import polars as pl
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ["DATABASE_URL"]


def res_to_df(res):
    if not res or len(res) == 0:
        return None
    res_values = map(lambda r: list(r.values()), res)
    res_columns = list(res[0].keys())
    df = pl.DataFrame(res_values, res_columns)
    return df


def get_non_pkey_col_sql(df: pl.DataFrame, pkey_cols: set[str]):
    non_key_cols = set(df.columns).difference(pkey_cols)
    non_key_cols_sqls = map(lambda x: f"{x} = EXCLUDED.{x}", non_key_cols)
    non_key_cols_sql = ", ".join(non_key_cols_sqls)
    return non_key_cols_sql


class DB:
    pool: asyncpg.Pool

    async def init(self):
        pool = await asyncpg.create_pool(DATABASE_URL)
        if pool is None:
            raise ValueError("Could not create connection pool")
        self.pool = pool

    async def close(self):
        await self.pool.close()

    async def fetch(self, query: str, *args, timeout: Optional[int] = None):
        res = await self.pool.fetch(query, *args, timeout=timeout)
        return res_to_df(res)

    async def insert(
        self,
        df: pl.DataFrame,
        table_name: str,
        pkey_cols: Optional[set[str]] = None,
        timeout: Optional[float] = None,
    ):
        """Used to insert a dataframe into a table, optionally upserting on pkey_cols

        Args:
            df (pl.DataFrame): dataframe to insert
            table_name (str): table to insert into
            pkey_cols (Optional[set[str]], optional): if arbitrary pkeys are specified, will be used to attempt upsert operation. Defaults to None.
            timeout (Optional[float], optional): timeout for db connection. Defaults to None.
        """
        cols_sql = ", ".join(df.columns)
        placeholder_fillers = ", ".join(
            map(lambda x: f"${x + 1}", range(len(df.columns)))
        )

        conflict_resolution = (
            f"ON CONFLICT ({', '.join(pkey_cols)}) DO UPDATE SET {get_non_pkey_col_sql(df, pkey_cols)}"
            if pkey_cols
            else "ON CONFLICT DO NOTHING"
        )

        query = f"INSERT INTO {table_name} ({cols_sql}) VALUES ({placeholder_fillers}) {conflict_resolution}"

        data = df.rows()
        await self.pool.executemany(
            query,
            data,
            timeout=timeout,  # type: ignore
        )

    async def delete(self):
        pass


db = DB()
