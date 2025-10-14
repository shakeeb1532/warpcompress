FROM python:3.12-slim

# Build deps for python-snappy, etc.
RUN apt-get update && apt-get install -y --no-install-recommends \
      gcc g++ make libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

ENV PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1

WORKDIR /app
COPY . /app

RUN python -m pip install -U pip \
    && python -m pip install .[hashes]

ENTRYPOINT ["warp-compress"]
