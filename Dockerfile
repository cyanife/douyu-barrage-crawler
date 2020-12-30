FROM python:3.9-alpine

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PYTHONDONTWRITEBYTECODE=1

RUN apk update && apk add --no-cache gcc libffi-dev openssl-dev g++ postgresql-dev make curl

RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python

COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock

RUN source $HOME/.poetry/env && poetry config virtualenvs.create false && poetry install --no-dev --no-ansi --no-root --no-interaction

RUN apk del libffi-dev g++ make curl

COPY . /app

CMD ["python", "/app/worker.py"]

