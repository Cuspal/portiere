# Portiere — local-first clinical data mapping.
#
# Build:    docker build -t portiere .
# Verify:   docker run --rm portiere doctor
# Demo:     docker run --rm portiere quickstart
# Own data: docker run --rm -v "$PWD/data:/data" portiere \
#             profile-report /data/*.csv -o /data/report
#
# The image contains no clinical data and performs no egress; `portiere doctor
# --assert-no-egress` passes in the default configuration. After models are
# cached (mount ~/.portiere), it runs with `--network none`.

FROM python:3.12-slim AS base

# Faster, quieter, reproducible-ish installs
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install the package with the default local stack (polars engine + quality).
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install ".[polars,quality]"

# Non-root user; writable home for model/project caches
RUN useradd --create-home portiere
USER portiere
ENV HOME=/home/portiere

# Model + project caches live under $HOME/.portiere — mount to persist:
#   docker run -v portiere-cache:/home/portiere/.portiere ...
VOLUME ["/home/portiere/.portiere"]

ENTRYPOINT ["portiere"]
CMD ["--help"]
