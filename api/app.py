from contextlib import asynccontextmanager
from typing import Annotated

import uvicorn
from fastapi import FastAPI, Header
from pydantic import BaseModel

from api.db import close, db, init


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init()
    yield
    await close()


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
    WHERE li.list_id = :list_id
    """


async def get_list_items_helper(list_id: int):
    df = await db.fetch(ITEMS_IN_LIST_QUERY, list_id=list_id)
    if df is None:
        return []
    return df.to_dicts()


@app.get("/lists/{list_id}", response_model=GetListItemsRes)
async def get_list_items(list_id: int):
    items = await get_list_items_helper(list_id)
    return {"items": items}


class FunnyJsonBody(BaseModel):
    apples: int
    carrots: int
    memes: dict[str, str]


@app.post("/super-fast")
async def super_fast(req_body: FunnyJsonBody, X_Token: Annotated[str, Header()]):
    print(X_Token)

    return {
        "data": [
            {"lint": 34, "id": 69, "towers": 2},
            {"lint": 3434, "id": 907842, "towers": 8},
            {"lint": None, "id": 3938478937, "towers": 3},
        ]
    }


def main():
    uvicorn.run("api.app:app", host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
