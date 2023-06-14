# syntax=docker/dockerfile:1
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt requirements.txt

RUN apt-get update
RUN apt-get install -y build-essential curl software-properties-common
RUN rm -rf /var/lib/apt/lists/*

RUN pip install -r requirements.txt

COPY . .

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health
ENTRYPOINT ["streamlit", "run", "Home.py", "--server.port=8501", "--server.address=0.0.0.0"]
