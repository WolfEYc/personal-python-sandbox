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


app = FastAPI(lifespan=lifespan)


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
async def get_list_items(list_id: int) -> GetListItemsRes:
    lf = await db.fetch_lf(ITEMS_IN_LIST_QUERY, list_id)
    print(lf.collect())
    items = lf.collect().to_dicts()
    res = GetListItemsRes.model_validate({"items": items})
    return res


def main():
    uvicorn.run(app)
