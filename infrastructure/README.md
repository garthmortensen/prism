# Prism Infrastructure

Container-based deployment for the Prism Risk Adjustment Analytics Platform.

## Quick Start

### 1. Launch Services

```bash
podman-compose -f infrastructure/docker-compose.yml up -d
```

### 2. What's Running

- **PostgreSQL database** (port 5432)

### 3. Connection Details

Use these credentials to connect from your local environment (e.g., local dbt or Dagster instance):

- **Host**: `localhost`
- **Port**: `5432`
- **User**: `ra_user`
- **Password**: `ra_pass`
- **Database**: `ra_database`
- **Connection String**: `postgresql://ra_user:ra_pass@localhost:5432/ra_database`

## Management

### Check Status

```bash
podman-compose -f infrastructure/docker-compose.yml ps
```

### Stop Services

```bash
podman-compose -f infrastructure/docker-compose.yml down
```

### View Logs

```bash
podman-compose -f infrastructure/docker-compose.yml logs -f
```

### Access Shells

```bash
# Prism application shell
podman-compose -f infrastructure/docker-compose.yml exec prism bash

# PostgreSQL shell
podman-compose -f infrastructure/docker-compose.yml exec postgres psql -U ra_user -d ra_database
```

## Configuration

- **Database:** `ra_database`
- **User:** `ra_user`
- **Password:** `ra_pass`
- **Connection:** `postgresql://ra_user:ra_pass@localhost:5432/ra_database`
