"""
MinIO to Snowflake Loader DAG

This DAG runs on a short interval to move newly generated JSON files from MinIO
into the Snowflake table `WEATHERANALYTICS.WEATHER_ANALYTICS.raw_history_json`.
It operates independently of the main `weather_ingestion_pipeline` DAG so that
data loading into Snowflake happens in parallel as soon as files land in MinIO.
"""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure utils package is importable when running inside Airflow
dag_dir = Path(__file__).parent.resolve()
utils_dir = dag_dir / "utils"
for path in (dag_dir, utils_dir):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

try:
    from connector_utils import (
        list_minio_objects,
        read_minio_object,
        execute_snowflake_query,
        execute_snowflake_update,
    )
except ModuleNotFoundError:
    from utils.connector_utils import (
        list_minio_objects,
        read_minio_object,
        execute_snowflake_query,
        execute_snowflake_update,
    )

RAW_HISTORY_TABLE = "WEATHERANALYTICS.WEATHER_ANALYTICS.raw_history_json"


def chunk_list(items: List[str], chunk_size: int) -> List[List[str]]:
    """Yield successive chunks from a list."""
    for index in range(0, len(items), chunk_size):
        yield items[index : index + chunk_size]


def list_new_minio_files(**context) -> List[str]:
    """
    List JSON files in MinIO that have not yet been loaded into Snowflake.

    Returns:
        List of new object keys to process.
    """
    objects = list_minio_objects(suffix=".json")
    if not objects:
        print("[INFO] No objects found in MinIO bucket.")
        return []

    # Sort by last modified time to process oldest files first
    objects.sort(key=lambda obj: obj.get("LastModified", datetime.min))
    all_keys = [obj.get("Key") for obj in objects if obj.get("Key")]

    if not all_keys:
        print("[INFO] No valid object keys discovered.")
        return []

    existing_keys = set()
    chunk_size = 100

    for chunk in chunk_list(all_keys, chunk_size):
        params = {f"file_{idx}": key for idx, key in enumerate(chunk)}
        placeholders = ", ".join([f"%(file_{idx})s" for idx in range(len(chunk))])
        query = f"""
            SELECT file_name
            FROM {RAW_HISTORY_TABLE}
            WHERE file_name IN ({placeholders})
        """
        rows = execute_snowflake_query(query, params)
        existing_keys.update(row[0] for row in rows)

    new_keys = [key for key in all_keys if key not in existing_keys]

    print(f"[INFO] Found {len(new_keys)} new file(s) to load into Snowflake.")
    if new_keys:
        sample = ", ".join(new_keys[:5])
        print(f"  Sample files: {sample}")

    return new_keys


def load_files_to_snowflake(**context) -> int:
    """
    Download JSON files from MinIO and insert them into Snowflake.

    Returns:
        Number of files successfully inserted.
    """
    ti = context["ti"]
    new_keys: List[str] = ti.xcom_pull(task_ids="list_new_minio_files") or []

    if not new_keys:
        print("[INFO] No new files to load. Skipping.")
        return 0

    inserted_count = 0

    for key in new_keys:
        try:
            raw_bytes = read_minio_object(key)
            json_text = raw_bytes.decode("utf-8").strip()

            def parse_payload(text: str):
                try:
                    return json.loads(text), False
                except json.JSONDecodeError:
                    records = []
                    for line in text.splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        records.append(json.loads(line))
                    if not records:
                        raise
                    return records, True

            json_data, normalized = parse_payload(json_text)
            payload = json.dumps(json_data)

            if normalized:
                print(
                    f"[INFO] Normalized NDJSON content to array before loading '{key}'."
                )

            insert_query = f"""
                INSERT INTO {RAW_HISTORY_TABLE} (payload, file_name, loaded_at)
                SELECT PARSE_JSON(%(payload)s), %(file_name)s, CURRENT_TIMESTAMP()
            """
            params = {
                "payload": payload,
                "file_name": key,
            }

            execute_snowflake_update(insert_query, params)
            inserted_count += 1
            print(f"[SUCCESS] Inserted '{key}' into Snowflake.")

        except Exception as exc:
            print(f"[ERROR] Failed to load '{key}' into Snowflake: {str(exc)}")
            raise

    print(f"[SUCCESS] Loaded {inserted_count} file(s) into Snowflake.")
    return inserted_count


default_args = {
    "owner": "weather_analytics",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="minio_to_snowflake_loader",
    default_args=default_args,
    description="Continuously loads new MinIO JSON files into Snowflake.",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    minio_list_task = PythonOperator(
        task_id="list_new_minio_files",
        python_callable=list_new_minio_files,
    )

    snowflake_load_task = PythonOperator(
        task_id="load_files_to_snowflake",
        python_callable=load_files_to_snowflake,
    )

    minio_list_task >> snowflake_load_task

