# Deployment Guide

This guide covers deploying the Portiere platform across local development, production Docker Compose, and Kubernetes environments. It includes service configuration, environment variables, health checks, and infrastructure initialization procedures.

---

## Table of Contents

1. [Local Development with Docker Compose](#local-development-with-docker-compose)
2. [Production Deployment with Docker Compose](#production-deployment-with-docker-compose)
3. [Kubernetes Deployment](#kubernetes-deployment)
4. [Environment Variables Reference](#environment-variables-reference)
5. [FAISS Index Initialization](#faiss-index-initialization)
6. [Elasticsearch Population](#elasticsearch-population)
7. [SSL/TLS and Reverse Proxy Setup](#ssltls-and-reverse-proxy-setup)

---

## Local Development with Docker Compose

### Prerequisites

- Docker Engine 20.10 or later
- Docker Compose v2.0 or later
- At least 8 GB of available RAM (Elasticsearch and FAISS require significant memory)

### Starting the Development Stack

```bash
cd medmap/infra/docker
docker-compose up
```

To run in detached mode:

```bash
cd medmap/infra/docker
docker-compose up -d
```

### Services

The development Docker Compose stack includes the following services:

| Service         | Port  | Description                                      |
|-----------------|-------|--------------------------------------------------|
| **api**         | 8000  | FastAPI application server                       |
| **postgres**    | 5432  | PostgreSQL database with asyncpg driver          |
| **redis**       | 6379  | Redis for caching and rate limiting              |
| **elasticsearch** | 9200 | Elasticsearch for BM25 text search              |
| **celery-worker** | --   | Celery worker for async task processing         |
| **celery-beat** | --    | Celery Beat for scheduled tasks                  |
| **portiere-cloud** | 3000 | Next.js cloud dashboard application            |
| **portiere-mapper** | 3001 | Next.js mapper (crowdsourcing) application     |

### Accessing Services

Once the stack is running:

- **API**: http://localhost:8000
- **API docs (Swagger)**: http://localhost:8000/docs
- **API docs (ReDoc)**: http://localhost:8000/redoc
- **Portiere Cloud dashboard**: http://localhost:3000
- **Portiere Mapper**: http://localhost:3001
- **Elasticsearch**: http://localhost:9200
- **PostgreSQL**: `postgresql://localhost:5432/portiere`

### Stopping the Stack

```bash
cd medmap/infra/docker
docker-compose down
```

To remove volumes (this deletes all data):

```bash
cd medmap/infra/docker
docker-compose down -v
```

### Rebuilding After Code Changes

```bash
cd medmap/infra/docker
docker-compose build
docker-compose up -d
```

To rebuild a specific service:

```bash
cd medmap/infra/docker
docker-compose build api
docker-compose up -d api
```

---

## Production Deployment with Docker Compose

### Prerequisites

- A server with Docker Engine and Docker Compose installed
- A domain name with DNS configured
- SSL certificates (see [SSL/TLS and Reverse Proxy Setup](#ssltls-and-reverse-proxy-setup))

### Configuration

Before starting the production stack, set all required environment variables. Create a `.env` file in the `medmap/infra/docker/` directory or export variables in your shell:

```bash
# Database
DB_HOST=postgres
DB_PORT=5432
DB_NAME=portiere
DB_USER=portiere
DB_PASSWORD=<strong-random-password>

# Redis
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_PASSWORD=<strong-random-password>

# API Security
JWT_SECRET=<strong-random-secret>
API_KEY_SALT=<strong-random-salt>

# Elasticsearch
ELASTICSEARCH_URL=http://elasticsearch:9200

# Stripe
STRIPE_SECRET_KEY=sk_live_...

# Supabase (for Cloud and Mapper apps)
NEXT_PUBLIC_SUPABASE_URL=https://your-project.supabase.co
NEXT_PUBLIC_SUPABASE_ANON_KEY=eyJ...
SUPABASE_SERVICE_ROLE_KEY=eyJ...

# Cloud App
NEXT_PUBLIC_API_URL=https://api.yourdomain.com

# Mapper App
NEXT_PUBLIC_LIFF_ID=your-liff-id
LINE_CHANNEL_ID=your-channel-id
ADMIN_LINE_USER_IDS=U1234567890,U0987654321
```

### Starting the Production Stack

```bash
cd medmap/infra/docker
docker-compose -f docker-compose.prod.yaml up -d
```

### Verifying the Deployment

Check that all services are running:

```bash
docker-compose -f docker-compose.prod.yaml ps
```

Verify the API health endpoints:

```bash
# Basic health check
curl https://api.yourdomain.com/health

# Readiness check (includes dependency verification)
curl https://api.yourdomain.com/health/ready

# Liveness check
curl https://api.yourdomain.com/health/live
```

### Updating the Production Stack

```bash
cd medmap/infra/docker
docker-compose -f docker-compose.prod.yaml pull
docker-compose -f docker-compose.prod.yaml up -d
```

### Viewing Logs

```bash
# All services
docker-compose -f docker-compose.prod.yaml logs -f

# Specific service
docker-compose -f docker-compose.prod.yaml logs -f api
```

---

## Kubernetes Deployment

### Helm Chart Overview

The Portiere Helm chart is located at `medmap/infra/helm/portiere/` and deploys the following resources:

- **API Deployment**: FastAPI application with configurable replicas.
- **PostgreSQL StatefulSet**: Database with persistent volume claims.
- **Redis Deployment**: Caching and rate limiting.
- **Elasticsearch StatefulSet**: Search engine with persistent storage.
- **Celery Worker Deployment**: Async task processing workers.
- **Celery Beat Deployment**: Single-instance scheduled task runner.
- **Cloud Deployment**: Portiere Cloud Next.js application.
- **Mapper Deployment**: Portiere Mapper Next.js application.
- **Init Job**: One-time job for FAISS index building and Elasticsearch population.
- **Services, Ingress, ConfigMaps, Secrets**: Supporting Kubernetes resources.

### Installing the Chart

```bash
# Add the Portiere Helm repository (if published)
helm repo add portiere https://charts.portiere.io
helm repo update

# Install from local chart
helm install portiere medmap/infra/helm/portiere/ \
  --namespace portiere \
  --create-namespace \
  -f values-production.yaml
```

### values-production.yaml Example

```yaml
api:
  replicas: 3
  resources:
    requests:
      cpu: 500m
      memory: 1Gi
    limits:
      cpu: 2000m
      memory: 4Gi

cloud:
  replicas: 2
  resources:
    requests:
      cpu: 250m
      memory: 512Mi
    limits:
      cpu: 1000m
      memory: 2Gi

mapper:
  replicas: 2
  resources:
    requests:
      cpu: 250m
      memory: 512Mi
    limits:
      cpu: 1000m
      memory: 2Gi

postgres:
  storage: 50Gi
  resources:
    requests:
      cpu: 500m
      memory: 2Gi
    limits:
      cpu: 2000m
      memory: 8Gi

redis:
  resources:
    requests:
      cpu: 100m
      memory: 256Mi
    limits:
      cpu: 500m
      memory: 1Gi

elasticsearch:
  replicas: 3
  storage: 100Gi
  resources:
    requests:
      cpu: 1000m
      memory: 4Gi
    limits:
      cpu: 2000m
      memory: 8Gi

celery:
  workers: 4
  resources:
    requests:
      cpu: 250m
      memory: 512Mi
    limits:
      cpu: 1000m
      memory: 2Gi

initJob:
  enabled: true
  resources:
    requests:
      cpu: 1000m
      memory: 4Gi
    limits:
      cpu: 2000m
      memory: 8Gi

ingress:
  enabled: true
  className: nginx
  hosts:
    - host: api.yourdomain.com
      paths:
        - path: /
          service: api
    - host: cloud.yourdomain.com
      paths:
        - path: /
          service: cloud
    - host: mapper.yourdomain.com
      paths:
        - path: /
          service: mapper
  tls:
    - secretName: portiere-tls
      hosts:
        - api.yourdomain.com
        - cloud.yourdomain.com
        - mapper.yourdomain.com
```

### Init Job: FAISS Index Build and Elasticsearch Population

The Init Job runs before the API deployment becomes ready. It performs two critical tasks:

1. **Build the FAISS index**: Downloads the SapBERT embedding model (`cambridgeltl/SapBERT-from-PubMedBERT-fulltext`), encodes standard vocabulary concepts, and builds the FAISS dense vector index.
2. **Populate Elasticsearch**: Indexes standard vocabulary concepts into Elasticsearch for BM25 text search.

The Init Job is configured as a Kubernetes Job with `restartPolicy: OnFailure`. It writes the FAISS index to a shared persistent volume that the API pods mount at startup.

```yaml
# Init Job configuration in values.yaml
initJob:
  enabled: true
  vocabularies:
    - SNOMED_CT
    - ICD10CM
    - RxNorm
    - LOINC
  embeddingModel: cambridgeltl/SapBERT-from-PubMedBERT-fulltext
  faissIndexPath: /data/faiss/index
```

### Health Checks

The API exposes three health check endpoints used by Kubernetes probes:

#### `/health` -- General Health

Returns the overall health status of the API. Used for basic monitoring.

```json
{
  "status": "healthy",
  "version": "1.0.0",
  "timestamp": "2025-01-15T10:30:00Z"
}
```

#### `/health/ready` -- Readiness Probe

Verifies that the API is ready to serve requests. Checks connectivity to all dependencies:

- PostgreSQL connection
- Redis connection
- Elasticsearch cluster health
- FAISS index loaded in memory

**Kubernetes configuration:**

```yaml
readinessProbe:
  httpGet:
    path: /health/ready
    port: 8000
  initialDelaySeconds: 30
  periodSeconds: 10
  failureThreshold: 3
```

#### `/health/live` -- Liveness Probe

Verifies that the API process is alive and responsive. This is a lightweight check that does not verify external dependencies.

**Kubernetes configuration:**

```yaml
livenessProbe:
  httpGet:
    path: /health/live
    port: 8000
  initialDelaySeconds: 15
  periodSeconds: 20
  failureThreshold: 3
```

### Resource Limits and Replicas

Refer to the `values-production.yaml` example above for recommended resource limits. Key considerations:

- **API pods**: Memory-intensive due to in-memory FAISS index. Allocate at least 4 GB per replica.
- **Elasticsearch**: Requires significant heap memory. Set `ES_JAVA_OPTS` appropriately (typically 50% of container memory limit).
- **Celery workers**: Memory usage scales with the number of concurrent tasks. Monitor and adjust.
- **Init Job**: Requires substantial CPU and memory for embedding model inference and index building. This is a one-time cost.

### Scaling

```bash
# Scale API replicas
kubectl scale deployment portiere-api --replicas=5 -n portiere

# Scale Celery workers
kubectl scale deployment portiere-celery-worker --replicas=8 -n portiere
```

---

## Environment Variables Reference

### API Service

| Variable                | Required | Default                | Description                                      |
|-------------------------|----------|------------------------|--------------------------------------------------|
| `DB_HOST`               | Yes      | `localhost`            | PostgreSQL host                                  |
| `DB_PORT`               | No       | `5432`                 | PostgreSQL port                                  |
| `DB_NAME`               | Yes      | `portiere`             | PostgreSQL database name                         |
| `DB_USER`               | Yes      | `portiere`             | PostgreSQL user                                  |
| `DB_PASSWORD`           | Yes      | --                     | PostgreSQL password                              |
| `REDIS_HOST`            | Yes      | `localhost`            | Redis host                                       |
| `REDIS_PORT`            | No       | `6379`                 | Redis port                                       |
| `REDIS_PASSWORD`        | No       | --                     | Redis password                                   |
| `ELASTICSEARCH_URL`     | Yes      | `http://localhost:9200`| Elasticsearch URL                                |
| `JWT_SECRET`            | Yes      | --                     | Secret key for JWT token signing                 |
| `API_KEY_SALT`          | Yes      | --                     | Salt for API key hashing                         |
| `EMBEDDING_MODEL`       | No       | `cambridgeltl/SapBERT-from-PubMedBERT-fulltext` | Embedding model for vector search |
| `FAISS_INDEX_PATH`      | No       | `/data/faiss/index`    | Path to FAISS index file                         |
| `CORS_ORIGINS`          | No       | `*`                    | Allowed CORS origins (comma-separated)           |
| `RATE_LIMIT_PER_MINUTE` | No       | `60`                   | API rate limit per API key per minute            |
| `LOG_LEVEL`             | No       | `INFO`                 | Application log level                            |

### Portiere Cloud (Next.js)

| Variable                         | Required | Description                              |
|----------------------------------|----------|------------------------------------------|
| `NEXT_PUBLIC_SUPABASE_URL`       | Yes      | Supabase project URL                     |
| `NEXT_PUBLIC_SUPABASE_ANON_KEY`  | Yes      | Supabase anonymous/public key            |
| `NEXT_PUBLIC_API_URL`            | Yes      | Portiere API base URL                    |
| `STRIPE_SECRET_KEY`              | Yes      | Stripe secret key for billing            |

### Portiere Mapper (Next.js)

| Variable                         | Required | Description                              |
|----------------------------------|----------|------------------------------------------|
| `NEXT_PUBLIC_LIFF_ID`            | Yes      | LINE LIFF application ID                 |
| `LINE_CHANNEL_ID`                | Yes      | LINE channel ID for server-side auth     |
| `NEXT_PUBLIC_SUPABASE_URL`       | Yes      | Supabase project URL                     |
| `SUPABASE_SERVICE_ROLE_KEY`      | Yes      | Supabase service role key (server-side)  |
| `ADMIN_LINE_USER_IDS`            | Yes      | Comma-separated admin LINE user IDs      |
| `STRIPE_SECRET_KEY`              | Yes      | Stripe secret key for payouts            |

### PostgreSQL

| Variable              | Required | Default      | Description                |
|-----------------------|----------|--------------|----------------------------|
| `POSTGRES_DB`         | Yes      | `portiere`   | Database name              |
| `POSTGRES_USER`       | Yes      | `portiere`   | Database user              |
| `POSTGRES_PASSWORD`   | Yes      | --           | Database password          |

### Redis

| Variable              | Required | Default | Description                   |
|-----------------------|----------|---------|-------------------------------|
| `REDIS_PASSWORD`      | No       | --      | Redis authentication password |

### Elasticsearch

| Variable              | Required | Default               | Description                  |
|-----------------------|----------|-----------------------|------------------------------|
| `ES_JAVA_OPTS`        | No       | `-Xms512m -Xmx512m`  | JVM heap settings            |
| `discovery.type`      | No       | `single-node`         | Cluster discovery type       |
| `xpack.security.enabled` | No   | `false`               | Enable X-Pack security       |

---

## FAISS Index Initialization

The FAISS (Facebook AI Similarity Search) index is a critical component of the hybrid search system. It stores dense vector embeddings of standard vocabulary concepts for fast nearest-neighbor retrieval.

### Build Process

1. **Load vocabulary data**: Standard vocabulary concepts (SNOMED CT, ICD-10, RxNorm, LOINC) are loaded from the database or vocabulary files.
2. **Generate embeddings**: Each concept description is encoded using the SapBERT model (`cambridgeltl/SapBERT-from-PubMedBERT-fulltext`) to produce a 768-dimensional dense vector.
3. **Build index**: Vectors are added to a FAISS index (typically `IndexFlatIP` for inner product or `IndexIVFFlat` for larger vocabularies).
4. **Save to disk**: The index is serialized and saved to the configured `FAISS_INDEX_PATH`.

### Init Job Pattern

In Kubernetes deployments, the FAISS index is built by an Init Job that runs before the API pods start:

```
Init Job starts
  --> Downloads SapBERT model
  --> Connects to PostgreSQL / reads vocabulary files
  --> Encodes all concept descriptions
  --> Builds FAISS index
  --> Saves index to shared PersistentVolume
  --> Job completes

API pods start
  --> Mount shared PersistentVolume
  --> Load FAISS index into memory
  --> Ready to serve search requests
```

### Rebuilding the Index

To rebuild the FAISS index (e.g., after adding new vocabulary concepts):

```bash
# Kubernetes
kubectl delete job portiere-init -n portiere
helm upgrade portiere medmap/infra/helm/portiere/ --set initJob.enabled=true

# Docker Compose (run manually)
docker-compose exec api python -m portiere.scripts.build_faiss_index
```

---

## Elasticsearch Population

Elasticsearch provides the BM25 (sparse) search component of the hybrid search system.

### Index Structure

The Elasticsearch index stores standard vocabulary concepts with the following mapping:

```json
{
  "mappings": {
    "properties": {
      "concept_id": { "type": "keyword" },
      "concept_code": { "type": "keyword" },
      "concept_name": { "type": "text", "analyzer": "standard" },
      "vocabulary_id": { "type": "keyword" },
      "domain_id": { "type": "keyword" },
      "concept_class_id": { "type": "keyword" },
      "synonyms": { "type": "text", "analyzer": "standard" },
      "description": { "type": "text", "analyzer": "standard" }
    }
  }
}
```

### Population Process

1. **Create index**: The Elasticsearch index is created with the appropriate mapping.
2. **Bulk index**: Vocabulary concepts are bulk-indexed into Elasticsearch.
3. **Verify**: Index document count is verified against the source vocabulary.

### Repopulating

```bash
# Kubernetes
kubectl exec -it deployment/portiere-api -n portiere -- \
  python -m portiere.scripts.populate_elasticsearch

# Docker Compose
docker-compose exec api python -m portiere.scripts.populate_elasticsearch
```

---

## SSL/TLS and Reverse Proxy Setup

### Nginx Reverse Proxy

For Docker Compose production deployments, use Nginx as a reverse proxy with SSL termination.

#### Example Nginx Configuration

```nginx
upstream api {
    server localhost:8000;
}

upstream cloud {
    server localhost:3000;
}

upstream mapper {
    server localhost:3001;
}

server {
    listen 80;
    server_name api.yourdomain.com cloud.yourdomain.com mapper.yourdomain.com;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl http2;
    server_name api.yourdomain.com;

    ssl_certificate /etc/letsencrypt/live/yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/yourdomain.com/privkey.pem;

    location / {
        proxy_pass http://api;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}

server {
    listen 443 ssl http2;
    server_name cloud.yourdomain.com;

    ssl_certificate /etc/letsencrypt/live/yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/yourdomain.com/privkey.pem;

    location / {
        proxy_pass http://cloud;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}

server {
    listen 443 ssl http2;
    server_name mapper.yourdomain.com;

    ssl_certificate /etc/letsencrypt/live/yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/yourdomain.com/privkey.pem;

    location / {
        proxy_pass http://mapper;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### Let's Encrypt with Certbot

```bash
# Install certbot
sudo apt install certbot python3-certbot-nginx

# Obtain certificates
sudo certbot --nginx -d api.yourdomain.com -d cloud.yourdomain.com -d mapper.yourdomain.com

# Auto-renewal is configured automatically by certbot
# Verify with:
sudo certbot renew --dry-run
```

### Kubernetes Ingress with cert-manager

For Kubernetes deployments, use cert-manager for automatic TLS certificate management:

```bash
# Install cert-manager
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.14.0/cert-manager.yaml

# Create ClusterIssuer for Let's Encrypt
kubectl apply -f - <<EOF
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: letsencrypt-prod
spec:
  acme:
    server: https://acme-v02.api.letsencrypt.org/directory
    email: your-email@yourdomain.com
    privateKeySecretRef:
      name: letsencrypt-prod
    solvers:
      - http01:
          ingress:
            class: nginx
EOF
```

The Helm chart's Ingress resource will automatically request certificates from cert-manager when TLS is configured in the values file.

---

## Related Documentation

- [Portiere Cloud Guide](10-portiere-cloud-guide.md) -- Cloud dashboard usage
- [Portiere Mapper Guide](11-portiere-mapper-guide.md) -- Mapper platform usage
- [Migration from Legacy SDK](13-migration-from-legacy.md) -- Migrate from the old API
- [Quickstart Guide](01-quickstart.md) -- Get started with the Portiere SDK
