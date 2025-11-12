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


def get_airbyte_auth_headers() -> Dict[str, str]:
    """
    Get authentication headers for Airbyte API.
    Supports Basic Auth (username/password) or Bearer Token (API key).
    """
    headers = {}

    # Try Bearer Token first (API key)
    try:
        api_key = get_config_value("AIRBYTE_API_KEY")
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
            return headers
    except ValueError:
        pass

    # Try Basic Auth (username/password)
    try:
        username = get_config_value("AIRBYTE_USERNAME")
        password = get_config_value("AIRBYTE_PASSWORD")
        if username and password:
            credentials = f"{username}:{password}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            headers["Authorization"] = f"Basic {encoded_credentials}"
            return headers
    except ValueError:
        pass

    # No authentication configured - return empty headers
    # (may work if Airbyte is on trusted network)
    return headers


def check_airbyte_connection(**context) -> bool:
    """
    Check if Airbyte API is accessible and optionally verify connection exists.
    If AIRBYTE_CONNECTION_ID is set, verifies the connection exists.
    Otherwise, just checks if Airbyte service is healthy.

    Authentication: Uses AIRBYTE_API_KEY (Bearer token) or
    AIRBYTE_USERNAME/AIRBYTE_PASSWORD (Basic Auth) if configured.
    """
    try:
        # Get authentication headers
        headers = get_airbyte_auth_headers()

        # First, check if Airbyte service is accessible
        health_url = "http://airbyte-abctl-control-plane:80/api/v1/health"
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
                connection_url = f"http://airbyte-abctl-control-plane:80/api/v1/connections/{connection_id}"
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
                    print("  - Set AIRBYTE_API_KEY (Bearer token) OR")
                    print("  - Set AIRBYTE_USERNAME and AIRBYTE_PASSWORD (Basic Auth)")
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


def get_cities_from_snowflake(**context) -> List[Dict[str, Any]]:
    """Query Snowflake to get cities that need coordinates (where lat or lon is NULL)"""
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
        for city in cities:
            print(
                f"  - {city['city_name']}, {city['country_code']} (state: {city['state_code']})"
            )

        # Store in XCom for next task
        context["ti"].xcom_push(key="cities", value=cities)
        return cities

    except Exception as e:
        print(f"[ERROR] Error querying Snowflake: {str(e)}")
        raise


def get_coordinates_from_api(**context) -> List[Dict[str, Any]]:
    """Get coordinates from OpenWeather API for each city"""
    api_key = get_openweather_api_key()
    if not api_key:
        raise ValueError(
            "OPENWEATHER_API_KEY not set in Airflow Variables or environment"
        )

    # Get cities from previous task
    cities = context["ti"].xcom_pull(key="cities", task_ids="get_cities_from_snowflake")

    if not cities:
        print("No cities to process")
        return []

    results = []

    for city in cities:
        city_name = city["city_name"]
        country_code = city["country_code"]
        state_code = city["state_code"]
        start_date = city["start_date"]

        # Build API query string
        # Format: q={city name},{state code},{country code}
        # If state_code is NULL, don't include it
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
            # If state_code is provided, also match it
            matched_result = None
            for result in data:
                if result.get("country") == country_code:
                    if state_code:
                        # Check if state matches (state field in API response)
                        if result.get("state") == state_code:
                            matched_result = result
                            break
                    else:
                        # No state code, use first match with correct country
                        matched_result = result
                        break

            if not matched_result:
                print(
                    f"[WARNING] No matching result for {city_name}, {country_code} (state: {state_code})"
                )
                # Use first result if no exact match
                matched_result = data[0]
                print(
                    f"  Using first result: {matched_result.get('name')}, {matched_result.get('country')}"
                )

            lat = matched_result.get("lat")
            lon = matched_result.get("lon")

            if lat is None or lon is None:
                print(f"[ERROR] Missing coordinates for {city_name}")
                continue

            results.append(
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

    print(f"[SUCCESS] Successfully fetched coordinates for {len(results)} cities")

    # Store results in XCom
    context["ti"].xcom_push(key="coordinates", value=results)
    return results


def update_snowflake_coordinates(**context) -> int:
    """Update Snowflake table with lat/lon coordinates"""
    # Get coordinates from previous task
    coordinates = context["ti"].xcom_pull(
        key="coordinates", task_ids="get_coordinates_from_api"
    )

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

    Authentication: Uses AIRBYTE_API_KEY (Bearer token) or
    AIRBYTE_USERNAME/AIRBYTE_PASSWORD (Basic Auth) if configured.
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

        airbyte_url = f"http://airbyte-abctl-control-plane:80/api/v1/connections/sync"
        payload = {
            "connectionId": connection_id,
        }

        print(f"Triggering Airbyte sync for connection: {connection_id}")
        response = requests.post(airbyte_url, json=payload, headers=headers, timeout=30)

        if response.status_code == 200:
            result = response.json()
            job_id = result.get("jobId")
            print(f"[SUCCESS] Airbyte sync triggered successfully. Job ID: {job_id}")
            return {
                "status": "triggered",
                "job_id": job_id,
                "connection_id": connection_id,
            }
        elif response.status_code == 401:
            error_msg = "HTTP 401: Unauthorized - Authentication failed"
            print(f"[ERROR] {error_msg}")
            print("[WARNING] Please configure Airbyte authentication:")
            print("  - Set AIRBYTE_API_KEY (Bearer token) OR")
            print("  - Set AIRBYTE_USERNAME and AIRBYTE_PASSWORD (Basic Auth)")
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

# Task 4: Get cities from Snowflake
get_cities = PythonOperator(
    task_id="get_cities_from_snowflake",
    python_callable=get_cities_from_snowflake,
    dag=dag,
)

# Task 5: Get coordinates from OpenWeather API
get_coordinates = PythonOperator(
    task_id="get_coordinates_from_api",
    python_callable=get_coordinates_from_api,
    dag=dag,
)

# Task 6: Update Snowflake with coordinates
update_coordinates = PythonOperator(
    task_id="update_snowflake_coordinates",
    python_callable=update_snowflake_coordinates,
    dag=dag,
)

# Task 7: Trigger Airbyte sync (placeholder)
trigger_sync = PythonOperator(
    task_id="trigger_airbyte_sync",
    python_callable=trigger_airbyte_sync,
    dag=dag,
)

# Task 8: Verify data in MinIO
verify_data = PythonOperator(
    task_id="verify_minio_data",
    python_callable=verify_minio_data,
    dag=dag,
)

# Define task dependencies
(
    [check_minio, check_airbyte, check_snowflake]
    >> get_cities
    >> get_coordinates
    >> update_coordinates
    >> trigger_sync
    >> verify_data
)
