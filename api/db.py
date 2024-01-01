import os
from functools import reduce
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


def get_non_pkey_col_sql(df: pl.DataFrame, pkey_cols: set[str], table_name: str):
    non_key_cols = set(df.columns).difference(pkey_cols)
    non_key_cols_sqls = map(lambda x: f"{x} = EXCLUDED.{x}", non_key_cols)
    non_key_cols_sql = ", ".join(non_key_cols_sqls)
    where_conditions_sqls = map(
        lambda x: f"{table_name}.{x} != EXCLUDED.{x}", non_key_cols
    )
    where_condition_sql = " OR ".join(where_conditions_sqls)
    upsert_sql = f"{non_key_cols_sql} WHERE {where_condition_sql}"
    return upsert_sql


def kwargs_to_sql(query, **kwargs):
    return reduce(
        lambda acc, x: acc.replace(f":{x[1]}", f"${x[0]}"),
        enumerate(kwargs.keys(), 1),
        query,
    )


POLARS_TO_POSTGRES_TYPE_MAP = {
    str(pl.Object): "TEXT",
    str(pl.Boolean): "BOOLEAN",
    str(pl.UInt8): "SMALLINT",
    str(pl.UInt16): "SMALLINT",
    str(pl.UInt32): "INT",
    str(pl.UInt64): "BIGINT",
    str(pl.Int8): "SMALLINT",
    str(pl.Int16): "SMALLINT",
    str(pl.Int32): "INT",
    str(pl.Int64): "BIGINT",
    str(pl.Float32): "REAL",
    str(pl.Float64): "DOUBLE PRECISION",
    str(pl.Date): "DATE",
    str(pl.Datetime): "TIMESTAMP",
    str(pl.Utf8): "TEXT",
}


class DB:
    pool: asyncpg.Pool

    async def init(self):
        pool = await asyncpg.create_pool(DATABASE_URL)
        if pool is None:
            raise ValueError("Could not create connection pool")
        self.pool = pool

    async def close(self):
        await self.pool.close()

    async def fetch(self, query: str, timeout=None, **kwargs):
        """Used to fetch data from the database into a dataframe.
        \nKeyword arguments are used for query arguments using :query_param syntax.

        Args:
            query (str): SQL query to execute
            timeout (Optional[int], optional): timeout to give up in seconds. Defaults to None.

        Returns:
            (Dataframe | None): returns a dataframe if there are results, otherwise None

        Examples:
            >>> await db.fetch("SELECT * FROM item WHERE id = :id", id=83473)
            shape: (1, 3)
            ┌────────┬─────────┬──────────────┐
            │ id     ┆ name    ┆ price        │
            │ ---    ┆ ---     ┆ ---          │
            │ i64    ┆ str     ┆ decimal[*,2] │
            ╞════════╪═════════╪══════════════╡
            │ 83473  ┆ pumpkin ┆ 12.99        │
            └────────┴─────────┴──────────────┘
            >>> await db.fetch(
                    "SELECT * FROM item WHERE id = ANY(:ids) ORDER BY price LIMIT 10",
                    ids=[83473, 348374],
                )
            shape: (2, 3)
            ┌────────┬─────────┬──────────────┐
            │ id     ┆ name    ┆ price        │
            │ ---    ┆ ---     ┆ ---          │
            │ i64    ┆ str     ┆ decimal[*,2] │
            ╞════════╪═════════╪══════════════╡
            │ 348374 ┆ yogurt  ┆ 3.99         │
            │ 83473  ┆ pumpkin ┆ 12.99        │
            └────────┴─────────┴──────────────┘
            >>> ids = pl.Series("id", [83473, 348374])
            >>> names = pl.Series("name", ["onion", "bbq", "beans"])
            >>> attr_dict = {"ids": ids, "names": names}
            >>> query = \"\"\"--sql
                SELECT *
                FROM item
                WHERE
                    id = ANY(:ids) OR
                    name = ANY(:names)
                ORDER BY price
                LIMIT 10
                \"\"\"

            >>> await db.fetch(
                    query,
                    **attr_dict,
                )
            shape: (5, 3)
            ┌─────────┬─────────┬──────────────┐
            │ id      ┆ name    ┆ price        │
            │ ---     ┆ ---     ┆ ---          │
            │ i64     ┆ str     ┆ decimal[*,2] │
            ╞═════════╪═════════╪══════════════╡
            │ 1965488 ┆ onion   ┆ 2.99         │
            │ 348374  ┆ yogurt  ┆ 3.99         │
            │ 1870644 ┆ bbq     ┆ 6.99         │
            │ 83473   ┆ pumpkin ┆ 12.99        │
            │ 6334    ┆ beans   ┆ 69.99        │
            └─────────┴─────────┴──────────────┘
        """

        numbered_args_query = kwargs_to_sql(query, **kwargs)

        res = await self.pool.fetch(
            numbered_args_query, *kwargs.values(), timeout=timeout
        )
        return res_to_df(res)

    async def insert(
        self,
        df: pl.DataFrame,
        table_name: str,
        pkey_cols: Optional[set[str]] = None,
        return_cols: Optional[set[str]] = None,
        timeout: Optional[float] = None,
    ):
        """Used to insert a dataframe into a table, optionally upserting on pkey_cols

        Args:
            df (pl.DataFrame): dataframe to insert
            table_name (str): table to insert into
            pkey_cols (Optional[set[str]], optional): if arbitrary pkeys are specified, will be used to attempt upsert operation. Defaults to None.
            timeout (Optional[float], optional): timeout for db connection. Defaults to None.
        Returns:
            (pl.DataFrame | None): returns a dataframe if there are results, otherwise None

        >>> await db.fetch("SELECT * FROM item")
        shape: (11, 3)
        ┌──────────┬───────────┬──────────────┐
        │ id       ┆ name      ┆ price        │
        │ ---      ┆ ---       ┆ ---          │
        │ i64      ┆ str       ┆ decimal[*,2] │
        ╞══════════╪═══════════╪══════════════╡
        │ 394789   ┆ beanie    ┆ 16.89        │
        │ 1965488  ┆ onion     ┆ 2.99         │
        │ 1870644  ┆ bbq       ┆ 6.99         │
        │ 69420    ┆ condoms   ┆ 69.42        │
        │ …        ┆ …         ┆ …            │
        │ 22334    ┆ poop      ┆ 100.99       │
        │ 348374   ┆ yogurt    ┆ 3.99         │
        │ 98333829 ┆ chocolate ┆ 4.99         │
        │ 83473    ┆ pumpkin   ┆ 12.99        │
        └──────────┴───────────┴──────────────┘
        >>> items_to_insert = pl.DataFrame(
                {
                    "id": [394789, 3483, 39],
                    "name": ["beanie", "foo", "bar"],
                    "price": [12.66, 10.0, 20.0],
                }
            )

        Note: beanie already exists, but the price is different

        >>> await db.insert(items_to_insert, "item", return_cols={"id"})
        shape: (2, 1)
        ┌──────┐
        │ id   │
        │ ---  │
        │ i64  │
        ╞══════╡
        │ 3483 │
        │ 39   │
        └──────┘

        Note: beanie was not inserted, since it already exists, which is the default action

        I am now going to add id to the pkey_cols,
        so that we can upsert on the constraint column
        >>> await db.insert(
                items_to_insert,
                "item",
                pkey_cols={"id"},
                return_cols={"id"}
            )
        shape: (1, 1)
        ┌────────┐
        │ id     │
        │ ---    │
        │ i64    │
        ╞════════╡
        │ 394789 │
        └────────┘

        Note: The price of beanie is now updated, and is the only row returned since it is the only row that was updated
        """
        cols_sql = ", ".join(df.columns)
        placeholder_fillers = ", ".join(
            map(
                lambda x: f"${x[0] + 1}::{POLARS_TO_POSTGRES_TYPE_MAP[str(x[1][1])]}[]",
                enumerate(df.schema.items()),
            )
        )

        conflict_resolution = (
            f"ON CONFLICT ({', '.join(pkey_cols)}) DO UPDATE SET {get_non_pkey_col_sql(df, pkey_cols, table_name)}"
            if pkey_cols
            else "ON CONFLICT DO NOTHING"
        )

        return_cols_sql = f"RETURNING {", ".join(return_cols)}" if return_cols else ""

        query = f"INSERT INTO {table_name} ({cols_sql}) SELECT * FROM UNNEST({placeholder_fillers}) {conflict_resolution} {return_cols_sql}"
        data = df.get_columns()

        res = await self.pool.fetch(
            query,
            *data,
            timeout=timeout,  # type: ignore
        )

        res_df = res_to_df(res)
        return res_df


db = DB()
