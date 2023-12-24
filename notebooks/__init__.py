import httpx

headers = {"X-Token": "df3fvu3fujfjdkfj"}
client = httpx.AsyncClient(base_url="http://127.0.0.1:8000", headers=headers)
