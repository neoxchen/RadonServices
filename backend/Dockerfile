# syntax=docker/dockerfile:1
FROM python:3.8-slim-buster

# Set the working directory to /app
WORKDIR /app

# Install system dependencies
RUN apt-get update
RUN apt-get install -y gcc
#RUN apk add gcc # make gcc g++ python3-dev postgresql-dev musl-dev

# Clean up package lists to reduce image size
RUN rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the image
COPY dino_generated_requirements.txt requirements.txt

# Install Python dependencies
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Invoke entrypoint script
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--timeout", "120", "--chdir", "./src", "app:app"]
