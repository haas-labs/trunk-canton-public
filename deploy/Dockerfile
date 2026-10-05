# Canton ingestion adapter reference image.
FROM python:3.12-slim

WORKDIR /app

# git is needed to install the Sentinel SDK, which is a git dependency (not on PyPI).
RUN apt-get update && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*

# Dependencies first, for layer caching.
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

# The offset file lives here by default; mount a volume/PVC over it for crash-safe restart.
ENV CANTON_OFFSET_FILE=/data/offset.txt
VOLUME ["/data"]

# Unbuffered stdout/stderr, so every line (logs, tracebacks) reaches `docker logs` / `kubectl logs`
# as it is written instead of sitting in a buffer until the process exits.
ENV PYTHONUNBUFFERED=1

# Runs forever, resuming from the persisted offset. Pass --once to drain and exit.
ENTRYPOINT ["trunk-canton"]
