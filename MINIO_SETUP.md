# MinIO (S3-Compatible) Setup for Airbyte

## Overview

This setup creates a local S3-compatible storage server using MinIO to store Airbyte data. MinIO works with Airbyte's **S3 destination connector** and stores data in your project's `raw_data` folder.

**Key Advantages over SFTP:**
- More reliable (no state message bugs)
- Supports date-based file organization
- Better Kubernetes compatibility
- Can use S3 path prefixes for separate files

## Quick Start

**1. Start MinIO server:**
```bash
# Navigate to your project directory
cd /path/to/your/weather-analytics-data-engg

# Start MinIO server
docker-compose -f docker-compose-minio.yml up -d
```

**2. Connect to kind network:**
```bash
# Critical: Connect MinIO server to kind network for Airbyte communication
docker network connect kind airbyte-minio-server
```

**3. Get MinIO IP address:**
```bash
# Get MinIO server IP on kind network
docker inspect airbyte-minio-server --format '{{.NetworkSettings.Networks.kind.IPAddress}}'

# Returns: e.g., 172.18.0.6
```

**4. Access MinIO Console:**
- Open browser: http://localhost:9001
- Login: `minioadmin` / `minioadmin`
- Create a bucket named `airbyte-data`

## Configuration Details

**For Airbyte S3 Destination:**
- **S3 Bucket Name**: `airbyte-data` (create this in MinIO console)
- **S3 Bucket Region**: `us-east-1` (any region works with MinIO)
- **S3 Endpoint**: `http://<IP-from-step-3>:9000` (e.g., `http://172.18.0.6:9000`)
- **Access Key ID**: `minioadmin`
- **Secret Access Key**: `minioadmin`
- **S3 Path Format**: `{{date}}/{{stream}}/` (for date-based organization)
- **S3 Filename Pattern**: `{{date}}_{{stream}}.jsonl`

**Important Settings:**
- Enable **Path-style access** (if available in connector settings)
- Use **Full Refresh | Overwrite** sync mode
- Set **S3 Path Format** to organize files by date

## File Organization

With S3 path format `{{date}}/{{stream}}/`, files will be organized as:
```
raw_data/
  └── airbyte-data/          (S3 bucket)
      └── 2025-11-06/        (date folder)
          └── history.jsonl  (stream file)
```

Or with filename pattern `{{date}}_{{stream}}.jsonl`:
```
raw_data/
  └── airbyte-data/
      └── 2025-11-06_history.jsonl
      └── 2025-11-07_history.jsonl
```

## How to Get the Host IP Address

**Simple command (works on all systems):**
```bash
# Get MinIO server IP on kind network directly
docker inspect airbyte-minio-server --format '{{.NetworkSettings.Networks.kind.IPAddress}}'

# Returns: e.g., 172.18.0.6
```

## Management Commands

**Start MinIO server:**
```bash
docker-compose -f docker-compose-minio.yml up -d
```

**Stop MinIO server:**
```bash
docker-compose -f docker-compose-minio.yml down
```

**View logs:**
```bash
docker logs airbyte-minio-server
```

**Access MinIO Console:**
- URL: http://localhost:9001
- Username: `minioadmin`
- Password: `minioadmin`

## Advantages Over SFTP

1. **No State Message Bugs**: S3 connector is more stable
2. **Date-based Organization**: Automatic file organization by date
3. **Better Error Handling**: More reliable error reporting
4. **Kubernetes Friendly**: Works seamlessly with Kubernetes deployments
5. **File Management**: Easy to browse/manage files via MinIO console

## Migration from SFTP

1. Stop SFTP server: `docker-compose -f docker-compose-sftp.yml down`
2. Start MinIO: `docker-compose -f docker-compose-minio.yml up -d`
3. Connect to kind network: `docker network connect kind airbyte-minio-server`
4. Update Airbyte destination to use S3 connector instead of SFTP
5. Configure with MinIO endpoint and credentials

