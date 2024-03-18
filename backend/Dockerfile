# syntax=docker/dockerfile:1
FROM python:3.8-alpine

COPY requirements.txt requirements.txt

RUN apk update
RUN apk add gcc # make gcc g++ python3-dev postgresql-dev musl-dev
RUN pip install -r requirements.txt

COPY . .

# Invoke entrypoint script
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--chdir", "/src", "app:app"]