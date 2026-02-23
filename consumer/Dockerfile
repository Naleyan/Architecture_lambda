# Set python version
ARG BASE_CONTAINER=python:3.11-alpine

# Set the base image
FROM python:3.11-alpine

# Adds metadata to image.
#LABEL maintainer="Laurent"

# dont write pyc files
ENV PYTHONDONTWRITEBYTECODE 1
# dont buffer to stdout/stderr
ENV PYTHONUNBUFFERED 1

EXPOSE 9999


# Sets the user name to use when running the image.
USER root
RUN apk update
RUN apk add --no-cache curl
RUN apk add --no-cache bash
RUN pip install pandas --no-cache-dir
RUN pip install pyspark --no-cache-dir

RUN apk add --update --no-cache netcat-openbsd

# Make a directory for app
WORKDIR /consumer/app

# Install dependencies
COPY requirements.txt .
RUN  pip install --no-cache-dir -r requirements.txt
# RUN pip install --no-cache-dir --user -r /req.txt

# Copy source code
COPY ./app ./app

# Run the application
# CMD ["bash"]
CMD ["python", "app/consumer.py"]