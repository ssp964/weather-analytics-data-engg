"""
Connector utilities for Airflow DAGs.

This module contains reusable functions for checking connections and verifying data
across different services (MinIO, Airbyte, Snowflake).
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import os
import requests
import snowflake.connector
from contextlib import contextmanager
from airflow.models import Variable


def get_config_value(var_name: str, env_name: str = None) -> str:
    """
    Get configuration value from Airflow Variables or environment variables.
    Raises ValueError if not found in either location.

    Args:
        var_name: Airflow Variable name
        env_name: Environment variable name (defaults to var_name if not provided)

    Returns:
        Configuration value as string
    """
    if env_name is None:
        env_name = var_name

    # Try Airflow Variable first
    try:
        value = Variable.get(var_name)
        if value:
            return value
    except KeyError:
        pass

    # Fallback to environment variable
    value = os.getenv(env_name)
    if value:
        return value

    # Raise error if not found
    raise ValueError(
        f"Configuration '{var_name}' not found. "
        f"Please set it in Airflow Variables (Admin -> Variables) or as environment variable '{env_name}'"
    )


def get_snowflake_config() -> Dict[str, str]:
    """Get Snowflake configuration from Airflow Variables or environment variables"""
    return {
        "account": get_config_value("SNOWFLAKE_ACCOUNT"),
        "user": get_config_value("SNOWFLAKE_USER"),
        "password": get_config_value("SNOWFLAKE_PASSWORD"),
        "warehouse": get_config_value("SNOWFLAKE_WAREHOUSE"),
        "database": get_config_value("SNOWFLAKE_DATABASE"),
        "schema": get_config_value("SNOWFLAKE_SCHEMA"),
        "role": get_config_value("SNOWFLAKE_ROLE"),
    }


@contextmanager
def get_snowflake_connection():
    """
    Context manager for Snowflake connections.
    Ensures proper connection cleanup even if errors occur.

    Yields:
        snowflake.connector.SnowflakeConnection: Active Snowflake connection
    """
    config = get_snowflake_config()
    conn = None
    try:
        conn = snowflake.connector.connect(
            account=config["account"],
            user=config["user"],
            password=config["password"],
            warehouse=config["warehouse"],
            database=config["database"],
            schema=config["schema"],
            role=config["role"],
        )
        yield conn
    finally:
        if conn:
            conn.close()


def get_airbyte_access_token() -> str:
    """
    Generate an access token for Airbyte API using Client ID and Client Secret.
    Tokens expire in 15 minutes (900 seconds).

    Returns:
        Access token string
    """
    try:
        client_id = get_config_value("AIRBYTE_CLIENT_ID")
        client_secret = get_config_value("AIRBYTE_CLIENT_SECRET")

        token_url = (
            "http://airbyte-abctl-control-plane:80/api/public/v1/applications/token"
        )
        payload = {
            "client_id": client_id,
            "client_secret": client_secret,
        }

        response = requests.post(token_url, json=payload, timeout=30)

        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get("access_token")
            if access_token:
                print("[SUCCESS] Generated Airbyte access token")
                return access_token
            else:
                raise Exception("Access token not found in response")
        else:
            raise Exception(
                f"Failed to generate access token: HTTP {response.status_code} - {response.text}"
            )

    except ValueError as e:
        raise Exception(f"Airbyte Client ID or Client Secret not configured: {str(e)}")
    except Exception as e:
        print(f"[ERROR] Error generating Airbyte access token: {str(e)}")
        raise


def get_airbyte_auth_headers() -> Dict[str, str]:
    """
    Get authentication headers for Airbyte API.
    Uses Bearer token authentication (access token).
    """
    headers = {"Content-Type": "application/json"}

    try:
        access_token = get_airbyte_access_token()
        headers["Authorization"] = f"Bearer {access_token}"
        return headers
    except Exception as e:
        print(f"[ERROR] Failed to get Airbyte authentication: {str(e)}")
        raise


def check_minio_connection(**context) -> bool:
    """Check if MinIO is accessible"""
    try:
        minio_url = "http://airbyte-minio-server:9000/minio/health/live"
        response = requests.get(minio_url, timeout=5)
        if response.status_code == 200:
            print("[SUCCESS] MinIO connection successful")
            return True
        else:
            print(f"[ERROR] MinIO connection failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"[ERROR] MinIO connection error: {str(e)}")
        raise


def get_minio_client():
    """
    Create and return a MinIO S3 client using configured credentials.

    Returns:
        boto3.client: MinIO S3 client
    """
    try:
        import boto3
        from botocore.client import Config
    except ImportError as exc:
        raise ImportError(
            "boto3 is required to interact with MinIO. Install with 'pip install boto3'"
        ) from exc

    minio_endpoint = get_config_value("MINIO_ENDPOINT")
    minio_access_key = get_config_value("MINIO_ACCESS_KEY")
    minio_secret_key = get_config_value("MINIO_SECRET_KEY")

    return boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        config=Config(signature_version="s3v4"),
    )


def get_minio_bucket_name() -> str:
    """Return the configured MinIO bucket name."""
    return get_config_value("MINIO_BUCKET")


def list_minio_objects(
    prefix: Optional[str] = None,
    suffix: Optional[str] = None,
    max_keys: int = 1000,
) -> List[Dict[str, Any]]:
    """
    List objects from the configured MinIO bucket.

    Args:
        prefix: Optional key prefix to filter objects.
        suffix: Optional key suffix (e.g. ".json") to filter objects.
        max_keys: Maximum number of objects to return (per API call).

    Returns:
        List of object metadata dictionaries (Key, LastModified, Size, etc.).
    """
    client = get_minio_client()
    bucket = get_minio_bucket_name()

    objects: List[Dict[str, Any]] = []
    continuation_token = None

    while True:
        request_params: Dict[str, Any] = {
            "Bucket": bucket,
            "MaxKeys": max_keys,
        }
        if prefix:
            request_params["Prefix"] = prefix
        if continuation_token:
            request_params["ContinuationToken"] = continuation_token

        response = client.list_objects_v2(**request_params)

        contents = response.get("Contents", [])
        for obj in contents:
            key = obj.get("Key", "")
            if suffix and not key.endswith(suffix):
                continue
            objects.append(obj)

        if not response.get("IsTruncated"):
            break
        continuation_token = response.get("NextContinuationToken")

    return objects


def read_minio_object(key: str) -> bytes:
    """
    Read a MinIO object and return its raw bytes.

    Args:
        key: Object key in the bucket.

    Returns:
        Object content as bytes.
    """
    client = get_minio_client()
    bucket = get_minio_bucket_name()

    response = client.get_object(Bucket=bucket, Key=key)
    try:
        return response["Body"].read()
    finally:
        response["Body"].close()


def check_airbyte_connection(**context) -> bool:
    """
    Check if Airbyte API is accessible and optionally verify connection exists.
    If AIRBYTE_CONNECTION_ID is set, verifies the connection exists.
    Otherwise, just checks if Airbyte service is healthy.

    Authentication: Uses AIRBYTE_CLIENT_ID/AIRBYTE_CLIENT_SECRET (Bearer token) if configured.
    """
    try:
        # Get authentication headers
        headers = get_airbyte_auth_headers()

        # First, check if Airbyte service is accessible
        health_url = "http://airbyte-abctl-control-plane:80/api/public/v1/health"
        response = requests.get(health_url, headers=headers, timeout=5)
        if response.status_code != 200:
            print(
                f"[ERROR] Airbyte service health check failed: {response.status_code}"
            )
            return False

        print("[SUCCESS] Airbyte service is accessible")

        # If connection ID is configured, verify the connection exists
        try:
            connection_id = get_config_value("AIRBYTE_CONNECTION_ID")
            if connection_id:
                # Verify connection exists by fetching it
                connection_url = f"http://airbyte-abctl-control-plane:80/api/public/v1/connections/{connection_id}"
                conn_response = requests.get(connection_url, headers=headers, timeout=5)

                if conn_response.status_code == 200:
                    connection_data = conn_response.json()
                    print(
                        f"[SUCCESS] Airbyte connection '{connection_id}' exists and is valid"
                    )
                    print(f"Connection name: {connection_data.get('name', 'N/A')}")
                    return True
                elif conn_response.status_code == 401:
                    print(
                        f"[ERROR] Airbyte API authentication failed (401 Unauthorized)"
                    )
                    print("[WARNING] Please configure Airbyte authentication:")
                    print(
                        "  - Set AIRBYTE_CLIENT_ID and AIRBYTE_CLIENT_SECRET (Bearer token)"
                    )
                    return False
                elif conn_response.status_code == 404:
                    print(f"[ERROR] Airbyte connection '{connection_id}' not found")
                    print(
                        "[WARNING] Please create the connection in Airbyte UI or update AIRBYTE_CONNECTION_ID"
                    )
                    return False
                else:
                    print(
                        f"[ERROR] Failed to verify connection: HTTP {conn_response.status_code}"
                    )
                    print(f"Response: {conn_response.text[:200]}")
                    return False
            else:
                print(
                    "[INFO] AIRBYTE_CONNECTION_ID not set. Skipping connection verification."
                )
                print(
                    "[INFO] Airbyte service is healthy, but no connection configured."
                )
                return True
        except ValueError:
            # AIRBYTE_CONNECTION_ID not set - that's okay, just check service health
            print(
                "[INFO] AIRBYTE_CONNECTION_ID not configured. Only checking service health."
            )
            print(
                "[INFO] Airbyte service is healthy. Set AIRBYTE_CONNECTION_ID to verify specific connection."
            )
            return True

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Airbyte connection error: {str(e)}")
        print("[ERROR] Cannot reach Airbyte service. Is it running?")
        raise
    except Exception as e:
        print(f"[ERROR] Unexpected error checking Airbyte: {str(e)}")
        raise


def check_snowflake_connection(**context) -> bool:
    """Check if Snowflake is accessible"""
    try:
        with get_snowflake_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT CURRENT_VERSION()")
                version = cursor.fetchone()[0]
                print(f"[SUCCESS] Snowflake connection successful (Version: {version})")
                return True
    except Exception as e:
        print(f"[ERROR] Snowflake connection error: {str(e)}")
        raise


def verify_minio_data(**context) -> bool:
    """
    Verify data was stored in MinIO.
    Checks if the bucket exists and contains recent files.
    """
    try:
        import boto3
        from botocore.client import Config

        # Get MinIO configuration from Airflow Variables or environment
        try:
            minio_endpoint = get_config_value("MINIO_ENDPOINT")
            minio_access_key = get_config_value("MINIO_ACCESS_KEY")
            minio_secret_key = get_config_value("MINIO_SECRET_KEY")
            minio_bucket = get_config_value("MINIO_BUCKET")
        except ValueError as e:
            print(f"[ERROR] MinIO configuration missing: {str(e)}")
            return False

        # Create S3 client for MinIO
        s3_client = boto3.client(
            "s3",
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            config=Config(signature_version="s3v4"),
        )

        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=minio_bucket)
            print(f"[SUCCESS] MinIO bucket '{minio_bucket}' exists")
        except Exception as e:
            print(f"[ERROR] MinIO bucket '{minio_bucket}' not found: {str(e)}")
            return False

        # List recent objects (last 24 hours)
        cutoff_time = datetime.now() - timedelta(hours=24)
        try:
            response = s3_client.list_objects_v2(Bucket=minio_bucket, MaxKeys=10)
            if "Contents" in response:
                recent_files = [
                    obj["Key"]
                    for obj in response["Contents"]
                    if obj["LastModified"].replace(tzinfo=None) > cutoff_time
                ]
                print(
                    f"[SUCCESS] Found {len(recent_files)} recent files in MinIO bucket"
                )
                if recent_files:
                    print(f"  Recent files: {', '.join(recent_files[:5])}")
                return True
            else:
                print(f"[WARNING] MinIO bucket '{minio_bucket}' is empty")
                return True  # Empty bucket is not necessarily an error
        except Exception as e:
            print(f"[ERROR] Error listing MinIO objects: {str(e)}")
            return False

    except ImportError:
        print("[WARNING] boto3 not installed. Skipping MinIO verification.")
        print("  Install with: pip install boto3")
        return True  # Don't fail if boto3 is not available
    except Exception as e:
        print(f"[ERROR] Error verifying MinIO data: {str(e)}")
        return False


# Snowflake helper functions


def execute_snowflake_query(
    query: str, params: Optional[Dict[str, Any]] = None
) -> List[tuple]:
    """
    Execute a SELECT query and return results.

    Args:
        query: SQL query string
        params: Optional dictionary of parameters for parameterized queries

    Returns:
        List of tuples containing query results
    """
    with get_snowflake_connection() as conn:
        with conn.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()


def execute_snowflake_update(query: str, params: Dict[str, Any]) -> int:
    """
    Execute an UPDATE/INSERT/DELETE query.

    Args:
        query: SQL query string
        params: Dictionary of parameters for parameterized queries

    Returns:
        Number of rows affected
    """
    with get_snowflake_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            rows_affected = cursor.rowcount
            conn.commit()
            return rows_affected
