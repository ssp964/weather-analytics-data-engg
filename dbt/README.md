# dbt Weather Analytics Project

This dbt project transforms weather data using Snowflake as the data warehouse.

## Architecture

```
Airbyte → MinIO (S3 Storage) → Snowflake → dbt Transformations → Analytics
```

- **MinIO**: S3-compatible storage (stores raw JSONL files from Airbyte)
- **Snowflake**: Cloud data warehouse (stores and processes data)
- **dbt**: Data transformation tool (runs SQL transformations in Snowflake)

## Prerequisites

- Docker and Docker Compose installed
- Snowflake account with credentials
- Data loaded into Snowflake (from MinIO or other sources)

## Setup

### 1. Configure Snowflake Credentials

**Step 1: Copy the example profile file**
```bash
cp dbt/profiles.yml.example dbt/profiles.yml
```

**Step 2: Get Snowflake Configuration Values**

Navigate to Snowflake UI to get your configuration values:

**Navigation Path:**
```
Main Page → Profile → Account → Account Details → Config File
```

**Steps:**
1. On the **Main Page**, click on your **Profile** (top right corner)
2. Click on **Account** section in the dropdown menu
3. Click on **Account Details**
4. Click on **Config File** tab
5. In the Config File section, copy the following values:
   - **Account**: Copy the account identifier value
   - **User**: Copy your username
   - **Role**: Copy your role (e.g., ACCOUNTADMIN)
   - **Warehouse**: Copy your warehouse name (e.g., COMPUTE_WH)
   - **Database**: Select your database from the dropdown and copy the value
   - **Schema**: Select your schema from the dropdown and copy the value

**Step 3: Update profiles.yml**

Edit `dbt/profiles.yml` and replace the placeholder values with the values you copied from Snowflake:

```yaml
weather_analytics:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <paste-account-from-config-file>
      user: <paste-username-from-config-file>
      password: <your-snowflake-password>  # Enter your password manually
      role: <paste-role-from-config-file>
      database: <paste-database-from-config-file>
      warehouse: <paste-warehouse-from-config-file>
      schema: <paste-schema-from-config-file>
      threads: 4
      client_session_keep_alive: false
```

**Note**: The `profiles.yml` file is gitignored and will not be committed to version control for security.

### 2. Start dbt Container

```bash
docker-compose -f docker-compose-dbt.yml build
docker-compose -f docker-compose-dbt.yml up -d
```

### 3. Verify Connection

```bash
docker exec weather-dbt dbt debug --profiles-dir /usr/app
```

### 4. Run dbt Models

```bash
# Run all models
docker exec weather-dbt dbt run --profiles-dir /usr/app

# Run specific model
docker exec weather-dbt dbt run --select raw_weather_history --profiles-dir /usr/app

# Run with full refresh
docker exec weather-dbt dbt run --full-refresh --profiles-dir /usr/app
```

## Project Structure

```
dbt/
├── models/
│   ├── intermediate/   # Intermediate models (cleaned/normalized data)
│   ├── staging/       # Staging models (business logic ready)
│   └── marts/         # Final analytics models
├── seeds/             # Seed files (CSV data)
├── tests/             # Data tests
├── macros/            # dbt macros
├── sources.yml           # Source table definitions (raw tables created outside dbt)
├── dbt_project.yml       # dbt project configuration
├── profiles.yml.example  # Example Snowflake connection profile (template)
└── profiles.yml          # Snowflake connection profiles (gitignored - create from example)
```

## Data Pipeline Layers

1. **SOURCES (RAW schema)**: Raw data tables created outside dbt
   - Tables loaded via COPY INTO from MinIO
   - No dbt materialization - dbt only reads from sources
   - Schema: `RAW` (in Snowflake)

2. **INTERMEDIATE**: Cleaned and normalized data
   - Reads directly from sources
   - Extracts JSON fields
   - Data type conversions
   - Basic cleaning and validation
   - Derived fields (categories, classifications)
   - Schema: `INTERMEDIATE`

3. **STAGING**: Business logic ready
   - Business rules applied
   - Calculated metrics
   - Ready for analytics
   - Schema: `STAGING`

4. **MARTS**: Final analytics models
   - Aggregated data
   - Business-specific metrics
   - Optimized for reporting
   - Schema: `MARTS`

## Common Commands

```bash
# Enter dbt container shell
docker exec -it weather-dbt bash

# Run dbt commands
docker exec weather-dbt dbt run --profiles-dir /usr/app
docker exec weather-dbt dbt test --profiles-dir /usr/app
docker exec weather-dbt dbt docs generate --profiles-dir /usr/app

# Run specific model layers
docker exec weather-dbt dbt run --select intermediate.* --profiles-dir /usr/app
docker exec weather-dbt dbt run --select staging.* --profiles-dir /usr/app
docker exec weather-dbt dbt run --select marts.* --profiles-dir /usr/app

# View logs
docker logs weather-dbt
```

## Troubleshooting

### Snowflake Connection Issues

1. Verify credentials in `profiles.yml`
2. Ensure warehouse is running: `ALTER WAREHOUSE COMPUTE_WH RESUME;`
3. Test connection: `docker exec weather-dbt dbt debug --profiles-dir /usr/app`

### Model Execution Issues

1. Verify data exists in Snowflake source tables
2. Check table structure matches model expectations
3. Review dbt logs: `docker logs weather-dbt`
