## High level architecture

![alt text](<Screenshot 2025-03-31 at 10.57.00.png>)


### Stack
- Airflow For orchestration
- Dbt-core for data transforamtions,
- Astornomer Cosmos - for orchestrating dbt models from Airflow UI
- AWS S3 for raw data storing
- Snowflake maind data processing platform


##Start guide
- Pull the repository
- Make sure you have Docker, docker-compose, Vscode installed on you local machine
- In Vusual studio install devcontainer extention.
- Execute the command cmd+shift+P (macOs) and select 'Rebuild and Reopen in the Container'
- Wait until build and finish and open Airflow local UI: http://localhost:8080
- Use default airflow/airflow creds.


### Airflow Connections

Pipilenices use 3 connections
- 

METEOMATICS_API_LOGIN=acme_kozhaev_gleb
METEOMATICS_API_PASSWORD