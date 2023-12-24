from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from api.db import db


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init()
    yield
    await db.close()


app = FastAPI(lifespan=lifespan, debug=True)


class Item(BaseModel):
    name: str
    quantity: int


class GetListItemsRes(BaseModel):
    items: list[Item]


ITEMS_IN_LIST_QUERY = """--sql
    SELECT i.name, li.quantity
    FROM item_in_list AS li
    JOIN item i ON li.item_id = i.id
    WHERE li.list_id = $1
    """


@app.get("/lists/{list_id}")
async def get_list_items(list_id: int):
    lf = await db.fetch_lf(ITEMS_IN_LIST_QUERY, list_id)
    df = lf.collect()
    print(df)
    items = df.to_dicts()
    data = {"items": items}
    return GetListItemsRes.model_validate(data)


def main():
    uvicorn.run(app)
