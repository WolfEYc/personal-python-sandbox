FROM python:3.12.1-slim

WORKDIR /app


COPY ./api /app/api
COPY ./pyproject.toml /app/pyproject.toml
COPY ./poetry.lock /app/poetry.lock

ENV POETRY_VIRTUALENVS_IN_PROJECT=true

RUN pip install poetry
RUN poetry install --only main --no-root -n

EXPOSE 8000
ENTRYPOINT [ "./.venv/bin/python", "-m", "api.app" ]