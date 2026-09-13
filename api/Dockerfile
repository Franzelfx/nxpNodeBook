# syntax=docker/dockerfile:1.7
FROM python:3.11-slim

WORKDIR /app

# copy metadata first (better cache)
COPY api/pyproject.toml api/requirements.txt ./

# copy code before editable install (required)
COPY api/src ./src
# the schema the API applies on every start (see adapters/database/schema.py)
COPY db/init ./db/init

# git + ssh are needed only to resolve the private nxp-node-contract pin, and
# only in this layer. The key comes from a BuildKit ssh mount, so it is never
# copied into the image and never lands in a layer:
#     docker build --ssh default …   /   docker compose build
RUN apt-get update \
    && apt-get install -y --no-install-recommends git openssh-client \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p -m 0700 /root/.ssh \
    && ssh-keyscan -t rsa,ed25519 github.com >> /root/.ssh/known_hosts 2>/dev/null

RUN --mount=type=ssh pip install --upgrade pip \
    && pip install -r requirements.txt \
    && pip install -e .

ENV BOOK_SCHEMA_DIR=/app/db/init
EXPOSE 9130
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "9130"]
