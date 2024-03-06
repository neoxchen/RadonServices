# syntax=docker/dockerfile:1
FROM python:3.8-alpine

COPY requirements.txt requirements.txt

RUN apk update
RUN apk add make gcc g++ python3-dev postgresql-dev musl-dev gfortran openblas-dev
RUN pip install -r requirements.txt

COPY . .

# Invoke entrypoint script
CMD ["python", "-u", "src/entry.py"]
