FROM python:3.12-slim

WORKDIR /app


COPY ./api /app/api
COPY ./pyproject.toml /app/pyproject.toml
COPY ./poetry.lock /app/poetry.lock

ENV POETRY_VIRTUALENVS_IN_PROJECT=true

RUN pip install poetry
RUN poetry install --only main --no-root -n

ENTRYPOINT [ "./.venv/bin/python", "-m", "api.app" ]