## High-Level Architecture

![Architecture Diagram](<Screenshot 2025-03-31 at 10.57.00.png>)

### Stack

- **Apache Airflow** – for orchestration  
- **dbt-core** – for data transformations  
- **Astronomer Cosmos** – for running dbt models via the Airflow UI  
- **AWS S3** – for storing raw data  
- **Snowflake** – main data processing platform  

---

## Getting Started

1. Clone the repository  
2. Ensure you have **Docker**, **docker-compose**, and **Visual Studio Code** installed  
3. In VS Code, install the **Dev Containers** extension  
4. Press `Cmd+Shift+P` (macOS) or `Ctrl+Shift+P` (Windows/Linux) and select:  
   `Dev Containers: Rebuild and Reopen in Container`  
5. Wait for the build to finish  
6. Access Airflow locally at: [http://localhost:8080](http://localhost:8080)  
   Use default credentials: `airflow / airflow`

---

### Airflow Connections

The pipeline requires the following Airflow connections:

- `aws_conn_id` (type: *Amazon Web Services*)  
  — You can follow any public guide for creating IAM roles and users with appropriate permissions.

- `meteomatics_api` (type: *HTTP*)  
  — Use your Meteomatics API login and password.

- `snowflake_conn` (type: *Snowflake*)  
  — Standard Snowflake connection setup.

---

### Snowflake ↔️ S3 External Storage Integration

We use external S3 storage. Follow [Snowflake’s official guide](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration) to set it up.

```sql
CREATE SCHEMA meteomatics_db.dbt_schema;

CREATE OR REPLACE FILE FORMAT meteomatics_json_format
  TYPE = JSON;

CREATE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<your_role_arn>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://meteomatics-data-raw/');

DESC INTEGRATION s3_int;

CREATE OR REPLACE STAGE my_s3_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://meteomatics-data-raw/'
  FILE_FORMAT = meteomatics_json_format;

LIST @my_s3_stage;

SELECT
  METADATA$FILENAME,
  METADATA$FILE_LAST_MODIFIED,
  $1
FROM @meteomatics_db.dbt_schema.my_s3_stage;
```


### Production Readiness Notes
- The pipeline uses the Airflow Worker to call the Weather API. If you scale to global data, consider migrating execution to Kubernetes, AWS Lambda, or Google Cloud Functions for better scalability.

- Due to API limitations (e.g., one location per call), we use a TaskGroup for better visual clarity. However, with a large list of locations, it may clutter the UI and impact Airflow Webserver performance.

- If multiple workers are enabled, the extraction task may fail. Cosider to pack logic into Airflow Custom Operator