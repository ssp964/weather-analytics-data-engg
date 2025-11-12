"""
Weather Data Ingestion DAG

This DAG orchestrates the weather data ingestion pipeline:
1. Check connections (Airbyte, Snowflake, MinIO)
2. Query Snowflake to get cities that need coordinates
3. Fetch city coordinates (lat/lon) from OpenWeather API
4. Update Snowflake table with coordinates
5. Trigger Airbyte sync to fetch weather data
6. Store data in MinIO (S3-compatible storage)

Connections required:
- MinIO: Accessible at airbyte-minio-server:9000
- Airbyte: Accessible via kind network
- Snowflake: Configured via Airflow connections or environment variables
- OpenWeather API: Requires API key
"""

from datetime import datetime, timedelta
from contextlib import contextmanager
from typing import List, Dict, Optional, Any
import base64
import os
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import requests
import snowflake.connector

# Default arguments for the DAG
default_args = {
    "owner": "weather_analytics",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    "weather_ingestion_pipeline",
    default_args=default_args,
    description="Ingest weather data from API via Airbyte to MinIO",
    schedule=timedelta(hours=1),  # Run every hour
    start_date=datetime(2025, 11, 9),
    catchup=False,
    tags=["weather", "ingestion", "airbyte", "minio", "snowflake"],
    is_paused_upon_creation=False,  # Unpause DAG when created
)


# Configuration - Uses Airflow Variables with fallback to environment variables
# To set in Airflow UI: Admin -> Variables -> Add new variable
# Priority: Airflow Variable > Environment Variable > Error if not set
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


def get_openweather_api_key() -> str:
    """Get OpenWeather API key from Airflow Variables or environment variables"""
    return get_config_value("OPENWEATHER_API_KEY")


TABLE_NAME = "WEATHERANALYTICS.WEATHER_ANALYTICS.city_registry"
INGESTION_STATE_TABLE = "WEATHERANALYTICS.WEATHER_ANALYTICS.ingestion_state"


# ============================================================================
# Helper Functions for Snowflake Operations
# ============================================================================


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


# ============================================================================
# Connection Check Functions
# ============================================================================


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


# ============================================================================
# Data Processing Functions
# ============================================================================


def update_city_coordinates(**context) -> int:
    """
    Combined task: Get cities from Snowflake, fetch coordinates from OpenWeather API,
    and update Snowflake with coordinates.
    """
    # Step 1: Get cities from Snowflake that need coordinates
    try:
        query = f"""
        SELECT 
            city_name,
            country_code,
            state_code,
            start_date
        FROM {TABLE_NAME}
        WHERE lat IS NULL OR lon IS NULL
        """

        rows = execute_snowflake_query(query)

        cities = [
            {
                "city_name": row[0],
                "country_code": row[1],
                "state_code": row[2],
                "start_date": row[3],
            }
            for row in rows
        ]

        print(f"[SUCCESS] Found {len(cities)} cities that need coordinates")
        if not cities:
            print("No cities to process")
            return 0

        for city in cities:
            print(
                f"  - {city['city_name']}, {city['country_code']} (state: {city['state_code']})"
            )
    except Exception as e:
        print(f"[ERROR] Error querying Snowflake: {str(e)}")
        raise

    # Step 2: Get coordinates from OpenWeather API
    api_key = get_openweather_api_key()
    if not api_key:
        raise ValueError(
            "OPENWEATHER_API_KEY not set in Airflow Variables or environment"
        )

    coordinates = []
    for city in cities:
        city_name = city["city_name"]
        country_code = city["country_code"]
        state_code = city["state_code"]
        start_date = city["start_date"]

        # Build API query string
        if state_code:
            query_string = f"{city_name},{state_code},{country_code}"
        else:
            query_string = f"{city_name},{country_code}"

        api_url = f"http://api.openweathermap.org/geo/1.0/direct?q={query_string}&appid={api_key}"

        try:
            print(f"Fetching coordinates for: {query_string}")
            response = requests.get(api_url, timeout=10)

            if response.status_code != 200:
                print(f"[ERROR] API error for {city_name}: HTTP {response.status_code}")
                continue

            data = response.json()

            if not data or len(data) == 0:
                print(f"[ERROR] No results found for {city_name}, {country_code}")
                continue

            # Find the result that matches the country code
            matched_result = None
            for result in data:
                if result.get("country") == country_code:
                    if state_code:
                        if result.get("state") == state_code:
                            matched_result = result
                            break
                    else:
                        matched_result = result
                        break

            if not matched_result:
                print(
                    f"[WARNING] No matching result for {city_name}, {country_code} (state: {state_code})"
                )
                matched_result = data[0]
                print(
                    f"  Using first result: {matched_result.get('name')}, {matched_result.get('country')}"
                )

            lat = matched_result.get("lat")
            lon = matched_result.get("lon")

            if lat is None or lon is None:
                print(f"[ERROR] Missing coordinates for {city_name}")
                continue

            coordinates.append(
                {
                    "city_name": city_name,
                    "country_code": country_code,
                    "state_code": state_code,
                    "start_date": start_date,
                    "lat": lat,
                    "lon": lon,
                }
            )

            print(f"[SUCCESS] Found coordinates for {city_name}: lat={lat}, lon={lon}")

        except Exception as e:
            print(f"[ERROR] Error fetching coordinates for {city_name}: {str(e)}")
            continue

    print(f"[SUCCESS] Successfully fetched coordinates for {len(coordinates)} cities")

    # Step 3: Update Snowflake with coordinates
    if not coordinates:
        print("No coordinates to update")
        return 0

    try:
        updated_count = 0

        for coord in coordinates:
            # Update query using primary key: city_name, country_code, start_date
            update_query = f"""
            UPDATE {TABLE_NAME}
            SET lat = %(lat)s, lon = %(lon)s
            WHERE city_name = %(city_name)s
              AND country_code = %(country_code)s
              AND start_date = %(start_date)s
            """

            rows_affected = execute_snowflake_update(
                update_query,
                {
                    "lat": coord["lat"],
                    "lon": coord["lon"],
                    "city_name": coord["city_name"],
                    "country_code": coord["country_code"],
                    "start_date": coord["start_date"],
                },
            )

            if rows_affected > 0:
                updated_count += 1
                print(
                    f"[SUCCESS] Updated {coord['city_name']}, {coord['country_code']}: lat={coord['lat']}, lon={coord['lon']}"
                )
            else:
                print(
                    f"[WARNING] No rows updated for {coord['city_name']}, {coord['country_code']} (may not exist)"
                )

        print(f"[SUCCESS] Successfully updated {updated_count} cities in Snowflake")
        return updated_count

    except Exception as e:
        print(f"[ERROR] Error updating Snowflake: {str(e)}")
        raise


# ============================================================================
# Placeholder Functions (To be implemented)
# ============================================================================


def trigger_airbyte_sync(**context) -> Dict[str, Any]:
    """
    Trigger Airbyte sync job.
    Requires AIRBYTE_CONNECTION_ID to be set in Airflow Variables.

    Authentication: Uses AIRBYTE_CLIENT_ID/AIRBYTE_CLIENT_SECRET (Bearer token) if configured.
    """
    try:
        # Get connection ID from Airflow Variables or environment
        try:
            connection_id = get_config_value("AIRBYTE_CONNECTION_ID")
        except ValueError:
            print("[WARNING] AIRBYTE_CONNECTION_ID not set. Skipping sync.")
            return {"status": "skipped", "reason": "connection_id_not_set"}

        # Get authentication headers
        headers = get_airbyte_auth_headers()

        # Use /api/v1/ (not /api/public/v1/) for sync endpoint
        airbyte_url = f"http://airbyte-abctl-control-plane:80/api/v1/connections/sync"
        payload = {
            "connectionId": connection_id,
        }

        print(f"Triggering Airbyte sync for connection: {connection_id}")
        response = requests.post(airbyte_url, json=payload, headers=headers, timeout=30)

        if response.status_code == 200:
            result = response.json()
            # Handle both response formats: job.id (nested) or jobId (flat)
            job_id = None
            if "job" in result and isinstance(result["job"], dict):
                job_id = result["job"].get("id")
            elif "jobId" in result:
                job_id = result.get("jobId")
            else:
                # Fallback: try to find any id field
                job_id = result.get("id")

            if not job_id:
                print(f"[WARNING] Could not extract job ID from response: {result}")
                job_id = "unknown"

            print(f"[SUCCESS] Airbyte sync triggered successfully. Job ID: {job_id}")
            return {
                "status": "triggered",
                "job_id": job_id,
                "connection_id": connection_id,
            }
        elif response.status_code == 409:
            error_data = response.json()
            error_message = error_data.get("message", "A sync is already running")
            print(f"[INFO] {error_message}")
            print(
                "[INFO] Sync is already running for this connection. Skipping new trigger."
            )
            return {
                "status": "already_running",
                "connection_id": connection_id,
                "message": error_message,
            }
        elif response.status_code == 401:
            error_msg = "HTTP 401: Unauthorized - Authentication failed"
            print(f"[ERROR] {error_msg}")
            print("[WARNING] Please configure Airbyte authentication:")
            print("  - Set AIRBYTE_CLIENT_ID and AIRBYTE_CLIENT_SECRET (Bearer token)")
            raise Exception(f"Airbyte API authentication error: {error_msg}")
        else:
            error_msg = f"HTTP {response.status_code}: {response.text}"
            print(f"[ERROR] Failed to trigger Airbyte sync: {error_msg}")
            raise Exception(f"Airbyte API error: {error_msg}")

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Network error while triggering Airbyte sync: {str(e)}")
        raise
    except Exception as e:
        print(f"[ERROR] Error triggering Airbyte sync: {str(e)}")
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


# ============================================================================
# Airbyte Batch Processing Functions
# ============================================================================


def datetime_to_unix_timestamp(dt: datetime) -> int:
    """
    Convert datetime to Unix timestamp (seconds since epoch).
    Airbyte connector uses this format: 1760486400
    """
    return int(dt.timestamp())


def create_time_batches(
    start_date: datetime,
    end_date: datetime,
    batch_hours: int = 169,
    overlap_hours: int = 1,
) -> List[Dict[str, datetime]]:
    """
    Create time batches from start_date to end_date.

    Args:
        start_date: Start datetime
        end_date: End datetime
        batch_hours: Hours per batch (default: 169)
        overlap_hours: Overlap between batches in hours (default: 1)

    Returns:
        List of batch dictionaries with 'start' and 'end' keys
    """
    batches = []
    current_start = start_date

    while current_start < end_date:
        # Calculate batch end (batch_hours from start, but not beyond end_date)
        batch_end = min(current_start + timedelta(hours=batch_hours), end_date)

        batches.append(
            {
                "start": current_start,
                "end": batch_end,
            }
        )

        # Next batch starts 1 hour before current batch ends (overlap)
        if batch_end >= end_date:
            break
        current_start = batch_end - timedelta(hours=overlap_hours)

    return batches


def get_cities_for_ingestion() -> List[Dict[str, Any]]:
    """
    Get cities from city_registry table with start_date, end_date, lat, lon.

    Returns:
        List of city dictionaries with city_name, country_code, state_code,
        start_date, end_date, lat, lon
    """
    try:
        query = f"""
        SELECT 
            city_name,
            country_code,
            state_code,
            start_date,
            end_date,
            lat,
            lon
        FROM {TABLE_NAME}
        WHERE lat IS NOT NULL AND lon IS NOT NULL
        ORDER BY city_name, country_code, start_date
        """

        rows = execute_snowflake_query(query)

        cities = []
        for row in rows:
            cities.append(
                {
                    "city_name": row[0],
                    "country_code": row[1],
                    "state_code": row[2],
                    "start_date": row[3],
                    "end_date": row[4],
                    "lat": float(row[5]) if row[5] is not None else None,
                    "lon": float(row[6]) if row[6] is not None else None,
                }
            )

        print(f"[SUCCESS] Found {len(cities)} cities for ingestion")
        return cities

    except Exception as e:
        print(f"[ERROR] Error querying cities from Snowflake: {str(e)}")
        raise


def get_city_id(city_name: str, country_code: str, start_date: datetime) -> int:
    """
    Generate a unique city_id from city_name, country_code, and start_date.
    Uses hash to create a consistent numeric ID.
    """
    import hashlib

    unique_string = f"{city_name}_{country_code}_{start_date.isoformat()}"
    hash_obj = hashlib.md5(unique_string.encode())
    # Convert first 8 hex digits to integer (fits in NUMBER(10,0))
    return int(hash_obj.hexdigest()[:8], 16) % 10000000000


def update_ingestion_state(
    city_id: int,
    started_at: datetime,
    finished_at: datetime,
    job_status: str,
    preserve_started_at: bool = False,
    preserve_finished_at: bool = False,
) -> None:
    """
    Update or insert ingestion_state record.

    Args:
        city_id: Unique city identifier
        started_at: Batch start datetime (used for INSERT, or preserved if preserve_started_at=True)
        finished_at: Batch end datetime (updated only if preserve_finished_at=False)
        job_status: Status ('running', 'partial', or 'completed')
                     - 'running': API call triggered successfully (not exhausted)
                     - 'partial': API call exhausted (rate limit hit) or batch failed
                     - 'completed': Reached end_date from city_registry
        preserve_started_at: If True, keep existing started_at when updating (don't overwrite)
        preserve_finished_at: If True, keep existing finished_at when updating (don't overwrite)
                             Used when batch fails - don't update finished_at
    """
    try:
        # Use MERGE to update or insert
        # Snowflake MERGE syntax with proper parameter binding
        if preserve_started_at and preserve_finished_at:
            # Only update job_status, preserve both started_at and finished_at
            merge_query = f"""
            MERGE INTO {INGESTION_STATE_TABLE} AS target
            USING (
                SELECT 
                    %(city_id)s AS city_id,
                    %(job_status)s AS job_status
            ) AS source
            ON target.city_id = source.city_id
            WHEN MATCHED THEN
                UPDATE SET
                    job_status = source.job_status
            WHEN NOT MATCHED THEN
                INSERT (city_id, started_at, finished_at, job_status)
                VALUES (%(city_id)s, %(started_at)s, %(finished_at)s, %(job_status)s)
            """
            params = {
                "city_id": city_id,
                "started_at": started_at,
                "finished_at": finished_at,
                "job_status": job_status,
            }
        elif preserve_started_at:
            # Preserve started_at, update finished_at and job_status
            merge_query = f"""
            MERGE INTO {INGESTION_STATE_TABLE} AS target
            USING (
                SELECT 
                    %(city_id)s AS city_id,
                    %(finished_at)s AS finished_at,
                    %(job_status)s AS job_status
            ) AS source
            ON target.city_id = source.city_id
            WHEN MATCHED THEN
                UPDATE SET
                    finished_at = source.finished_at,
                    job_status = source.job_status
            WHEN NOT MATCHED THEN
                INSERT (city_id, started_at, finished_at, job_status)
                VALUES (%(city_id)s, %(started_at)s, %(finished_at)s, %(job_status)s)
            """
            params = {
                "city_id": city_id,
                "started_at": started_at,
                "finished_at": finished_at,
                "job_status": job_status,
            }
        else:
            # Normal update: update all fields
            merge_query = f"""
            MERGE INTO {INGESTION_STATE_TABLE} AS target
            USING (
                SELECT 
                    %(city_id)s AS city_id,
                    %(started_at)s AS started_at,
                    %(finished_at)s AS finished_at,
                    %(job_status)s AS job_status
            ) AS source
            ON target.city_id = source.city_id
            WHEN MATCHED THEN
                UPDATE SET
                    started_at = source.started_at,
                    finished_at = source.finished_at,
                    job_status = source.job_status
            WHEN NOT MATCHED THEN
                INSERT (city_id, started_at, finished_at, job_status)
                VALUES (source.city_id, source.started_at, source.finished_at, source.job_status)
            """
            params = {
                "city_id": city_id,
                "started_at": started_at,
                "finished_at": finished_at,
                "job_status": job_status,
            }

        execute_snowflake_update(merge_query, params)

        print(
            f"[SUCCESS] Updated ingestion_state for city_id={city_id}, "
            f"status={job_status}, started={started_at}, finished={finished_at}"
        )

    except Exception as e:
        print(f"[ERROR] Error updating ingestion_state: {str(e)}")
        raise


def get_airbyte_connection_config(connection_id: str) -> Dict[str, Any]:
    """
    Get current Airbyte connection configuration.

    Args:
        connection_id: Airbyte connection ID

    Returns:
        Connection configuration dictionary
    """
    try:
        headers = get_airbyte_auth_headers()
        url = f"http://airbyte-abctl-control-plane:80/api/public/v1/connections/{connection_id}"

        # Make authenticated request
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            error_msg = "Airbyte API authentication failed (401 Unauthorized)"
            print(f"[ERROR] {error_msg}")
            print(f"[DEBUG] Response: {response.text[:500]}")
            raise Exception(error_msg)
        elif response.status_code == 403:
            error_msg = f"Airbyte API access forbidden (403 Forbidden) for connection '{connection_id}'"
            print(f"[ERROR] {error_msg}")
            print(f"[DEBUG] Response: {response.text[:500]}")
            print("[WARNING] Possible causes:")
            print("  1. User doesn't have permission to access this connection")
            print("  2. Connection belongs to a different workspace")
            print("  3. Authentication credentials are incorrect")
            raise Exception(error_msg)
        elif response.status_code == 404:
            raise Exception(f"Airbyte connection '{connection_id}' not found")
        else:
            error_msg = f"Failed to get connection config: HTTP {response.status_code}"
            print(f"[ERROR] {error_msg}")
            print(f"[DEBUG] Response: {response.text[:500]}")
            raise Exception(error_msg)

    except Exception as e:
        print(f"[ERROR] Error getting Airbyte connection config: {str(e)}")
        raise


def update_airbyte_destination_config(
    destination_id: str,
    start_date_unix: int,
    end_date_unix: int,
    city_name: str = None,
    country_code: str = None,
) -> bool:
    """
    Update Airbyte destination configuration to include city and date range in filename pattern.
    This ensures files for the same city and date range are overwritten instead of creating new files.

    Args:
        destination_id: Airbyte destination ID
        start_date_unix: Start date as Unix timestamp
        end_date_unix: End date as Unix timestamp
        city_name: City name (optional, for filename prefix)
        country_code: Country code (optional, for filename prefix)

    Returns:
        True if successful
    """
    try:
        headers = get_airbyte_auth_headers()

        # Get current destination config
        dest_url = f"http://airbyte-abctl-control-plane:80/api/public/v1/destinations/{destination_id}"
        dest_response = requests.get(dest_url, headers=headers, timeout=30)

        if dest_response.status_code != 200:
            raise Exception(
                f"Failed to get destination config: HTTP {dest_response.status_code}"
            )

        dest_data = dest_response.json()
        dest_config = dest_data.get("configuration", {})

        if not dest_config:
            raise Exception("Destination configuration not found in API response")

        # Preserve secret fields
        secret_fields = [
            "access_key_id",
            "secret_access_key",
            "accessKeyId",
            "secretAccessKey",
        ]
        for field in secret_fields:
            if field in dest_config and dest_config[field]:
                dest_config[field] = "**********"

        # Update S3 path format and filename pattern to create single files
        # The issue: When s3_path_format includes ${STREAM_NAME}/, file_name_pattern becomes a directory
        # Solution: Remove ${STREAM_NAME}/ from path format, use file_name_pattern for the filename

        # Remove ${STREAM_NAME}/ from path format to prevent it from creating a subdirectory
        # Change from: ${YEAR}_${MONTH}_${DAY}/${STREAM_NAME}/
        # To: ${YEAR}_${MONTH}_${DAY}/
        current_path_format = dest_config.get(
            "s3_path_format", "${YEAR}_${MONTH}_${DAY}/${STREAM_NAME}/"
        )
        # Remove ${STREAM_NAME}/ from the path format
        new_path_format = current_path_format.replace("${STREAM_NAME}/", "").replace(
            "${STREAM_NAME}", ""
        )
        # Ensure it ends with / if it doesn't already
        if not new_path_format.endswith("/"):
            new_path_format += "/"
        dest_config["s3_path_format"] = new_path_format

        # Set file_name_pattern to include city and date range
        # This will now be used as the actual filename (not a directory)
        if city_name and country_code:
            # Sanitize city name and country code for filename (remove spaces, special chars)
            safe_city_name = (
                city_name.replace(" ", "_").replace(",", "").replace("/", "_")
            )
            safe_country_code = country_code.replace(" ", "_")
            dest_config["file_name_pattern"] = (
                f"{safe_city_name}_{safe_country_code}_{start_date_unix}_{end_date_unix}.json"
            )
        else:
            # Fallback to date range only if city info not provided
            dest_config["file_name_pattern"] = f"{start_date_unix}_{end_date_unix}.json"

        # Update destination configuration
        update_dest_payload = {
            "name": dest_data.get("name"),
            "definitionId": dest_data.get("definitionId"),
            "workspaceId": dest_data.get("workspaceId"),
            "configuration": dest_config,
        }

        update_dest_url = f"http://airbyte-abctl-control-plane:80/api/public/v1/destinations/{destination_id}"
        response = requests.put(
            update_dest_url, json=update_dest_payload, headers=headers, timeout=30
        )

        if response.status_code == 200:
            pattern = dest_config.get("file_name_pattern", "unknown")
            print(f"[SUCCESS] Updated destination filename pattern: {pattern}")
            time.sleep(2)  # Wait for propagation
            return True
        else:
            error_msg = f"HTTP {response.status_code}: {response.text}"
            print(f"[ERROR] Failed to update destination config: {error_msg}")
            raise Exception(f"Airbyte API error: {error_msg}")

    except Exception as e:
        print(f"[ERROR] Error updating destination config: {str(e)}")
        raise


def update_airbyte_connection_config(
    connection_id: str,
    start_date_unix: int,
    end_date_unix: int,
    lat: float,
    lon: float,
    city_name: str = None,
    country_code: str = None,
) -> bool:
    """
    Update Airbyte connection source configuration with new parameters.

    Args:
        connection_id: Airbyte connection ID
        start_date_unix: Start date as Unix timestamp
        end_date_unix: End date as Unix timestamp
        lat: Latitude
        lon: Longitude

    Returns:
        True if successful
    """
    try:
        headers = get_airbyte_auth_headers()

        # Get current connection config
        connection_config = get_airbyte_connection_config(connection_id)

        # Get destination ID and update destination config first
        destination_id = connection_config.get("destinationId")
        if destination_id:
            update_airbyte_destination_config(
                destination_id, start_date_unix, end_date_unix, city_name, country_code
            )
        else:
            print(
                "[WARNING] Destination ID not found, skipping destination config update"
            )

        # Update source configuration
        # Airbyte API structure: connection has sourceId, and we need to update the source config
        # First, get the source ID
        source_id = connection_config.get("sourceId")
        if not source_id:
            raise Exception("Source ID not found in connection configuration")

        # Get source configuration (may need separate API call)
        source_url = (
            f"http://airbyte-abctl-control-plane:80/api/public/v1/sources/{source_id}"
        )
        source_response = requests.get(source_url, headers=headers, timeout=30)

        if source_response.status_code != 200:
            error_text = (
                source_response.text[:500]
                if source_response.text
                else "No response body"
            )
            raise Exception(
                f"Failed to get source config: HTTP {source_response.status_code} - {error_text}"
            )

        source_data = source_response.json()
        source_config = source_data.get("configuration", {})

        if not source_config:
            raise Exception("Source configuration not found in API response")

        # Preserve secret fields (API keys, passwords) by using placeholder
        # Airbyte API requires "**********" for secret fields to preserve existing values
        # Check for common secret field names and preserve them
        secret_fields = [
            "api_key",
            "apiKey",
            "apikey",
            "appid",
            "password",
            "access_token",
            "accessToken",
        ]
        for field in secret_fields:
            if field in source_config and source_config[field]:
                # If field exists and has a value, preserve it with placeholder
                # (Airbyte will keep the existing secret value)
                source_config[field] = "**********"

        # Update the parameters (field names match Airbyte connector)
        # API expects start and end as strings (Unix timestamp as string)
        source_config["start"] = str(start_date_unix)
        source_config["end"] = str(end_date_unix)
        source_config["lat"] = lat
        source_config["lon"] = lon

        # Update source configuration using PUT endpoint
        # PUT /public/v1/sources/{sourceId} - requires full source object
        update_source_url = (
            f"http://airbyte-abctl-control-plane:80/api/public/v1/sources/{source_id}"
        )

        # Prepare full source update payload (preserve all existing fields)
        # Note: API uses "configuration" not "connectionConfiguration"
        # and "definitionId" not "sourceDefinitionId"
        update_source_payload = {
            "name": source_data.get("name"),
            "definitionId": source_data.get("definitionId"),
            "workspaceId": source_data.get("workspaceId"),
            "configuration": source_config,
        }

        # Use PUT (not PATCH) to update the source
        response = requests.put(
            update_source_url, json=update_source_payload, headers=headers, timeout=30
        )

        if response.status_code == 200:
            print(
                f"[SUCCESS] Updated Airbyte connection config: "
                f"start={start_date_unix}, end={end_date_unix}, lat={lat}, lon={lon}, api_key=preserved"
            )

            # Wait for configuration to propagate (Airbyte may cache source config)
            print("[INFO] Waiting 3 seconds for source configuration to propagate...")
            time.sleep(3)

            # Verify the configuration was updated
            verify_response = requests.get(source_url, headers=headers, timeout=30)
            if verify_response.status_code == 200:
                verify_data = verify_response.json()
                verify_config = verify_data.get("configuration", {})
                if (
                    verify_config.get("start") == str(start_date_unix)
                    and verify_config.get("end") == str(end_date_unix)
                    and verify_config.get("lat") == lat
                    and verify_config.get("lon") == lon
                ):
                    print(
                        "[SUCCESS] Verified source configuration was updated correctly"
                    )
                else:
                    print(
                        "[WARNING] Source configuration may not have updated correctly. "
                        "Values may differ from expected."
                    )
            else:
                print(
                    f"[WARNING] Could not verify source configuration update: "
                    f"HTTP {verify_response.status_code}"
                )

            return True
        else:
            error_msg = f"HTTP {response.status_code}: {response.text}"
            print(f"[ERROR] Failed to update Airbyte connection config: {error_msg}")
            raise Exception(f"Airbyte API error: {error_msg}")

    except Exception as e:
        print(f"[ERROR] Error updating Airbyte connection config: {str(e)}")
        raise


def get_airbyte_job_status(job_id: str) -> Dict[str, Any]:
    """
    Get Airbyte job status by job ID.

    Args:
        job_id: Airbyte job ID

    Returns:
        Job data dictionary with status information

    Raises:
        Exception: If job status cannot be retrieved
    """
    try:
        headers = get_airbyte_auth_headers()

        # Try both endpoints - v1 first (internal API)
        endpoints = [
            f"http://airbyte-abctl-control-plane:80/api/v1/jobs/{job_id}",
            f"http://airbyte-abctl-control-plane:80/api/public/v1/jobs/{job_id}",
        ]

        for url in endpoints:
            try:
                response = requests.get(url, headers=headers, timeout=30)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 404:
                    continue  # Try next endpoint
                elif response.status_code == 401:
                    print(f"[WARNING] Authentication failed for {url}")
                    continue
                else:
                    print(
                        f"[WARNING] Endpoint {url} returned HTTP {response.status_code}"
                    )
            except Exception as e:
                print(f"[WARNING] Error checking {url}: {str(e)}")
                continue

        raise Exception(f"Could not get job status for job {job_id} from any endpoint")

    except Exception as e:
        print(f"[ERROR] Error getting Airbyte job status: {str(e)}")
        raise


def wait_for_airbyte_sync_completion(
    job_id: str, timeout: int = 3600, poll_interval: int = 30
) -> Dict[str, Any]:
    """
    Wait for Airbyte sync job to complete by polling job status.

    Args:
        job_id: Airbyte job ID
        timeout: Maximum time to wait in seconds (default: 3600 = 1 hour)
        poll_interval: Time between status checks in seconds (default: 30)

    Returns:
        Dictionary with final status and job data:
        {
            "status": "succeeded|failed|cancelled|incomplete",
            "job_data": {...},
            "elapsed_seconds": int
        }

    Raises:
        Exception: If sync fails, times out, or is cancelled
    """
    import time as time_module

    start_time = time_module.time()
    print(f"[INFO] Waiting for sync completion (Job ID: {job_id})")
    print(f"[INFO] Timeout: {timeout}s, Poll interval: {poll_interval}s")

    while True:
        elapsed = time_module.time() - start_time
        if elapsed > timeout:
            raise Exception(
                f"Sync timeout after {timeout} seconds (Job ID: {job_id}). "
                f"Sync may still be running in Airbyte."
            )

        try:
            job_data = get_airbyte_job_status(job_id)

            # Extract status from response
            status = None
            if "job" in job_data and isinstance(job_data["job"], dict):
                status = job_data["job"].get("status")
            elif "status" in job_data:
                status = job_data["status"]

            if not status:
                print(f"[WARNING] Could not extract status from response: {job_data}")
                time_module.sleep(poll_interval)
                continue

            print(f"[INFO] Job {job_id} status: {status} (elapsed: {int(elapsed)}s)")

            # Check if job is complete
            if status == "succeeded":
                print(f"[SUCCESS] Sync completed successfully in {int(elapsed)}s")
                return {
                    "status": "succeeded",
                    "job_data": job_data,
                    "elapsed_seconds": int(elapsed),
                }
            elif status in ["failed", "cancelled", "incomplete"]:
                error_msg = (
                    f"Sync {status} (Job ID: {job_id}, elapsed: {int(elapsed)}s)"
                )
                print(f"[ERROR] {error_msg}")
                # Try to get error details from job data
                if "job" in job_data:
                    job_info = job_data["job"]
                    if "streamAggregatedStats" in job_info:
                        print(f"[DEBUG] Job stats: {job_info['streamAggregatedStats']}")
                raise Exception(error_msg)

            # Still running, wait and poll again
            time_module.sleep(poll_interval)

        except Exception as e:
            # If it's already an exception about failure, re-raise it
            if "Sync" in str(e) and (
                "failed" in str(e) or "cancelled" in str(e) or "incomplete" in str(e)
            ):
                raise
            # Otherwise, log warning and continue polling
            print(f"[WARNING] Error checking job status: {str(e)}")
            time_module.sleep(poll_interval)
            continue


def trigger_airbyte_sync_with_params(
    connection_id: str,
    start_date_unix: int,
    end_date_unix: int,
    lat: float,
    lon: float,
    city_name: str = None,
    country_code: str = None,
    timeout: int = 3600,
    poll_interval: int = 30,
) -> Dict[str, Any]:
    """
    Update Airbyte connection config, trigger sync, and wait for completion.

    Args:
        connection_id: Airbyte connection ID
        start_date_unix: Start date as Unix timestamp
        end_date_unix: End date as Unix timestamp
        lat: Latitude
        lon: Longitude
        timeout: Maximum time to wait for sync completion in seconds (default: 3600)
        poll_interval: Time between status checks in seconds (default: 30)

    Returns:
        Result dictionary with status, job_id, and sync completion details:
        {
            "status": "succeeded|already_running|rate_limit_exceeded",
            "job_id": str,
            "connection_id": str,
            "elapsed_seconds": int (if completed)
        }

    Raises:
        Exception: If sync fails, times out, or authentication fails
    """
    try:
        # Update connection configuration
        update_airbyte_connection_config(
            connection_id,
            start_date_unix,
            end_date_unix,
            lat,
            lon,
            city_name,
            country_code,
        )

        # Trigger sync
        headers = get_airbyte_auth_headers()
        # Use /api/v1/ (not /api/public/v1/) for sync endpoint
        airbyte_url = f"http://airbyte-abctl-control-plane:80/api/v1/connections/sync"
        payload = {"connectionId": connection_id}

        print(f"Triggering Airbyte sync for connection: {connection_id}")
        response = requests.post(airbyte_url, json=payload, headers=headers, timeout=30)

        if response.status_code == 200:
            result = response.json()
            # Handle both response formats: job.id (nested) or jobId (flat)
            job_id = None
            if "job" in result and isinstance(result["job"], dict):
                job_id = result["job"].get("id")
            elif "jobId" in result:
                job_id = result.get("jobId")
            else:
                # Fallback: try to find any id field
                job_id = result.get("id")

            if not job_id:
                print(f"[WARNING] Could not extract job ID from response: {result}")
                job_id = "unknown"

            print(f"[SUCCESS] Airbyte sync triggered successfully. Job ID: {job_id}")

            # Wait for sync completion
            print(f"[INFO] Waiting for sync to complete...")
            completion_result = wait_for_airbyte_sync_completion(
                str(job_id), timeout=timeout, poll_interval=poll_interval
            )

            return {
                "status": "succeeded",
                "job_id": str(job_id),
                "connection_id": connection_id,
                "elapsed_seconds": completion_result.get("elapsed_seconds", 0),
            }

        elif response.status_code == 409:
            error_data = response.json()
            error_message = error_data.get("message", "A sync is already running")
            print(f"[INFO] {error_message}")
            print(
                "[INFO] Sync already running - attempting to get job ID and wait for completion"
            )

            # Try to get the running job ID by listing recent jobs for this connection
            try:
                # List recent jobs for this connection
                jobs_url = f"http://airbyte-abctl-control-plane:80/api/v1/jobs/list"
                jobs_payload = {
                    "configTypes": ["sync"],
                    "configId": connection_id,
                    "pagination": {"pageSize": 1},
                }
                jobs_response = requests.post(
                    jobs_url, json=jobs_payload, headers=headers, timeout=30
                )

                if jobs_response.status_code == 200:
                    jobs_data = jobs_response.json()
                    jobs = jobs_data.get("data", {}).get("jobs", [])
                    if jobs:
                        # Get the most recent job (should be the running one)
                        running_job = jobs[0]
                        running_job_id = running_job.get("id")
                        running_status = running_job.get("status", "unknown")

                        if running_job_id and running_status == "running":
                            print(f"[INFO] Found running job ID: {running_job_id}")
                            print(f"[INFO] Waiting for existing sync to complete...")

                            # Wait for the existing sync to complete
                            completion_result = wait_for_airbyte_sync_completion(
                                str(running_job_id),
                                timeout=timeout,
                                poll_interval=poll_interval,
                            )

                            return {
                                "status": "succeeded",
                                "job_id": str(running_job_id),
                                "connection_id": connection_id,
                                "elapsed_seconds": completion_result.get(
                                    "elapsed_seconds", 0
                                ),
                                "message": "Waited for existing sync to complete",
                            }
                        else:
                            print(
                                f"[WARNING] Running job not found or not in running state"
                            )
                    else:
                        print(f"[WARNING] No jobs found for connection")
                else:
                    print(
                        f"[WARNING] Failed to list jobs: HTTP {jobs_response.status_code}"
                    )
            except Exception as e:
                print(f"[WARNING] Error getting running job ID: {str(e)}")

            # If we couldn't get the job ID, return already_running status
            # This will be handled by the caller
            return {
                "status": "already_running",
                "connection_id": connection_id,
                "message": error_message,
            }
        elif response.status_code == 429:
            # API rate limit exceeded (50,000 daily limit)
            error_data = response.json()
            error_message = error_data.get("message", "API rate limit exceeded")
            print(f"[ERROR] API rate limit exceeded: {error_message}")
            return {
                "status": "rate_limit_exceeded",
                "connection_id": connection_id,
                "message": error_message,
            }
        elif response.status_code == 401:
            error_msg = "HTTP 401: Unauthorized - Authentication failed"
            print(f"[ERROR] {error_msg}")
            raise Exception(f"Airbyte API authentication error: {error_msg}")
        else:
            error_msg = f"HTTP {response.status_code}: {response.text}"
            print(f"[ERROR] Failed to trigger Airbyte sync: {error_msg}")
            raise Exception(f"Airbyte API error: {error_msg}")

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Network error while triggering Airbyte sync: {str(e)}")
        raise
    except Exception as e:
        print(f"[ERROR] Error triggering Airbyte sync: {str(e)}")
        raise


def process_weather_ingestion_batches(**context) -> Dict[str, Any]:
    """
    Main task: Process weather data ingestion in batches.

    Steps:
    1. Get cities from city_registry
    2. For each city, create batches of 169 hours with 1 hour overlap
    3. For each batch, update Airbyte connection config and trigger sync
    4. Handle API limit errors (pause DAG)
    5. Update ingestion_state table

    Returns:
        Summary dictionary with processing results
    """
    try:
        # Get connection ID
        try:
            connection_id = get_config_value("AIRBYTE_CONNECTION_ID")
        except ValueError:
            print("[WARNING] AIRBYTE_CONNECTION_ID not set. Skipping batch processing.")
            return {"status": "skipped", "reason": "connection_id_not_set"}

        # Get cities
        cities = get_cities_for_ingestion()
        if not cities:
            print("[INFO] No cities found for ingestion")
            return {"status": "completed", "cities_processed": 0}

        # Process each city sequentially
        total_batches = 0
        successful_batches = 0
        rate_limit_hit = False

        for city in cities:
            city_name = city["city_name"]
            country_code = city["country_code"]
            start_date = city["start_date"]
            end_date = city["end_date"]
            lat = city["lat"]
            lon = city["lon"]

            print(
                f"\n[INFO] Processing city: {city_name}, {country_code} "
                f"({start_date} to {end_date})"
            )

            # Generate city_id
            city_id = get_city_id(city_name, country_code, start_date)

            # Create batches
            batches = create_time_batches(
                start_date, end_date, batch_hours=169, overlap_hours=1
            )
            print(f"[INFO] Created {len(batches)} batches for {city_name}")

            # Track the first batch start date (this will be preserved in ingestion_state)
            first_batch_start = batches[0]["start"] if batches else start_date

            # Process each batch
            for batch_idx, batch in enumerate(batches, 1):
                batch_start = batch["start"]
                batch_end = batch["end"]

                print(
                    f"\n[INFO] Processing batch {batch_idx}/{len(batches)}: "
                    f"{batch_start} to {batch_end}"
                )

                # Convert to Unix timestamps
                start_unix = datetime_to_unix_timestamp(batch_start)
                end_unix = datetime_to_unix_timestamp(batch_end)

                # Trigger sync with updated parameters and wait for completion
                try:
                    sync_result = trigger_airbyte_sync_with_params(
                        connection_id,
                        start_unix,
                        end_unix,
                        lat,
                        lon,
                        city_name,
                        country_code,
                    )

                    total_batches += 1

                    if sync_result["status"] == "succeeded":
                        successful_batches += 1
                        # Update ingestion_state with appropriate status
                        # 'completed' when we reach end_date, 'running' otherwise
                        # Preserve started_at from first batch, only update finished_at
                        is_completed = batch_end >= end_date
                        status = "completed" if is_completed else "running"
                        # Use first_batch_start for started_at, but only on first batch (preserve_started_at=False)
                        # For subsequent batches, preserve the existing started_at (preserve_started_at=True)
                        preserve_start = batch_idx > 1
                        update_ingestion_state(
                            city_id,
                            first_batch_start,
                            batch_end,
                            status,
                            preserve_started_at=preserve_start,
                        )
                        print(
                            f"[SUCCESS] Batch {batch_idx}/{len(batches)} completed successfully "
                            f"(elapsed: {sync_result.get('elapsed_seconds', 0)}s)"
                        )

                    elif sync_result["status"] == "rate_limit_exceeded":
                        # API limit exceeded - pause DAG and stop processing
                        print("[ERROR] API rate limit exceeded. Pausing DAG.")
                        rate_limit_hit = True
                        status = "partial"
                        # Preserve started_at from first batch, preserve finished_at (don't update on failure)
                        preserve_start = batch_idx > 1
                        update_ingestion_state(
                            city_id,
                            first_batch_start,
                            batch_end,
                            status,
                            preserve_started_at=preserve_start,
                            preserve_finished_at=True,  # Don't update finished_at on failure
                        )

                        # Pause the DAG
                        try:
                            dag_run = context.get("dag_run")
                            if dag_run and dag_run.dag:
                                dag_run.dag.pause()
                                print(
                                    "[INFO] DAG has been paused due to API rate limit"
                                )
                        except Exception as pause_error:
                            print(f"[WARNING] Could not pause DAG: {str(pause_error)}")
                            print(
                                "[WARNING] Please manually pause the DAG due to API rate limit"
                            )

                        return {
                            "status": "paused",
                            "reason": "api_rate_limit_exceeded",
                            "cities_processed": len(cities),
                            "total_batches": total_batches,
                            "successful_batches": successful_batches,
                        }

                    elif sync_result["status"] == "already_running":
                        # If we couldn't get the job ID, we can't wait for it
                        # This should be rare - fail the DAG to ensure data integrity
                        error_msg = (
                            "Sync already running but could not get job ID to wait for completion. "
                            "Please wait for the existing sync to complete and retry."
                        )
                        print(f"[ERROR] {error_msg}")
                        # Preserve started_at from first batch, preserve finished_at (don't update on failure)
                        preserve_start = batch_idx > 1
                        update_ingestion_state(
                            city_id,
                            first_batch_start,
                            batch_end,
                            "partial",
                            preserve_started_at=preserve_start,
                            preserve_finished_at=True,  # Don't update finished_at on failure
                        )
                        raise Exception(error_msg)

                    else:
                        # Unknown status - fail the DAG
                        error_msg = (
                            f"Unexpected sync status: {sync_result.get('status')}"
                        )
                        print(f"[ERROR] {error_msg}")
                        # Preserve started_at from first batch, preserve finished_at (don't update on failure)
                        preserve_start = batch_idx > 1
                        update_ingestion_state(
                            city_id,
                            first_batch_start,
                            batch_end,
                            "partial",
                            preserve_started_at=preserve_start,
                            preserve_finished_at=True,  # Don't update finished_at on failure
                        )
                        raise Exception(error_msg)

                except Exception as e:
                    # Any exception (including sync failures, timeouts) will fail the DAG
                    print(f"[ERROR] Error processing batch: {str(e)}")
                    # Update state as partial on error, preserve started_at and finished_at (don't update on failure)
                    preserve_start = batch_idx > 1
                    update_ingestion_state(
                        city_id,
                        first_batch_start,
                        batch_end,
                        "partial",
                        preserve_started_at=preserve_start,
                        preserve_finished_at=True,  # Don't update finished_at on failure
                    )
                    # Re-raise to fail the DAG
                    raise

                # Check if we've reached the end date for this city
                if batch_end >= end_date:
                    print(f"[SUCCESS] Completed all batches for {city_name}")
                    # Final update with completed status (already done above, but ensure it's set)
                    # Preserve started_at from first batch
                    preserve_start = batch_idx > 1
                    update_ingestion_state(
                        city_id,
                        first_batch_start,
                        batch_end,
                        "completed",
                        preserve_started_at=preserve_start,
                    )
                    break

        print(f"\n[SUCCESS] Batch processing completed")
        print(f"  Total batches: {total_batches}")
        print(f"  Successful batches: {successful_batches}")

        return {
            "status": "completed",
            "cities_processed": len(cities),
            "total_batches": total_batches,
            "successful_batches": successful_batches,
        }

    except Exception as e:
        print(f"[ERROR] Error in batch processing: {str(e)}")
        raise


# ============================================================================
# DAG Task Definitions
# ============================================================================

# Task 1: Check MinIO connection
check_minio = PythonOperator(
    task_id="check_minio_connection",
    python_callable=check_minio_connection,
    dag=dag,
)

# Task 2: Check Airbyte connection
check_airbyte = PythonOperator(
    task_id="check_airbyte_connection",
    python_callable=check_airbyte_connection,
    dag=dag,
)

# Task 3: Check Snowflake connection
check_snowflake = PythonOperator(
    task_id="check_snowflake_connection",
    python_callable=check_snowflake_connection,
    dag=dag,
)

# Task 4: Get cities, fetch coordinates, and update Snowflake
update_coordinates = PythonOperator(
    task_id="update_city_coordinates",
    python_callable=update_city_coordinates,
    dag=dag,
)

# Task 5: Process weather ingestion batches
process_batches = PythonOperator(
    task_id="process_weather_ingestion_batches",
    python_callable=process_weather_ingestion_batches,
    dag=dag,
)

# Task 6: Verify data in MinIO
verify_data = PythonOperator(
    task_id="verify_minio_data",
    python_callable=verify_minio_data,
    dag=dag,
)

# Define task dependencies
(
    [check_minio, check_airbyte, check_snowflake]
    >> update_coordinates
    >> process_batches
    >> verify_data
)
