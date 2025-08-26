FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl unzip procps && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir duckdb==1.3.2 pandas pyarrow
WORKDIR /work
COPY job.py /usr/local/bin/job.py
ENTRYPOINT ["python", "/usr/local/bin/job.py"]