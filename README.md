# Exercise: 
<img src="./img/exercise.png">

- Trainee: Lu The Vy
- ID: VyLT4
- Class: HCM22_FR_DATA_02

---
## Requirements
- `Visual Studio Code` - Main IDE.
- `Python` - Main programming language.
- `pgAdmin 4` - UI management software for PostgreSQL Database
- `dockerfile-compose.yml` - YAML file defining services, networks, and volumes for a Docker application
- `Docker Desktop` - Manages images and containers
- `Airflow` - Platform to programmatically author, schedule, and monitor workflows.
---

## Project Structure
- `dags` : contains DAG files.
    + `dags/data` : contains CSV files (files will be auto-generated when runs code)
    + `dags/sql` : contains SQL source code to create tables
    + `dataflow_processing.py` : Python source code
    + `insert_data.py` : Python source code
    + `main.py` : Python source code, main source file of this exercise
    + `weather.db` : Collected weather data (SQLite3) from previous exercise `ETL` 
- `logs/scheduler` : contains log files of running Airflow DAGs.
- `plugins` : generated by `docker-compose.yml`, contains Airflow plugins, usually empty
- `.env` : contains `AIRFLOW_UID` and `AIRFLOW_GID`
- `docker-compose.yml` : YAML file defining services, networks, and volumes for a Docker application
---

## Deploy Airflow by using Docker Composer
- It's required that Airflow package must be downloaded first:
- In Visual Studio Code, click `Terminal` -> `New Terminal` -> choose `powershell`
- Run: 
    ```sh
    >> pip install apache-airflow
    ```
### 1) Create `docker-compose.yml` file
- In Visual Studio Code, click `Terminal` -> `New Terminal` -> choose `Git Bash`
- Run:
    ```sh
    >> curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.3.2/docker-compose.yaml'
    ```
- `docker-compose.yml` contains several service definitions.
    + `airflow-scheduler` : The scheduler monitors all tasks and DAGs, then triggers the task instances once their dependencies are complete.
    + `airflow-webserver` : The webserver is available at http://localhost:8080.
    + `airflow-worker` : The worker that executes the tasks given by the scheduler.
    + `airflow-init` : The initialization service.
    + `postgres` : The database.
    + `redis` : The redis - broker that forwards messages from scheduler to worker.
### 2) Setting the right Airflow user:
- In Visual Studio Code, click `Terminal` -> `New Terminal` -> choose `Git Bash`
- Run:
    ```sh
    >> mkdir -p ./dags ./logs ./plugins
    >> echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```
- After running two line of command, `dags` `logs` `plugins` folder will appear and `.evn` file contains `AIRFLOW_UID` and `AIRFLOW_GID` will appear too
### 3) Initialize the database:
- On all operating systems, we need to run database migrations and create the first user account.
- In Visual Studio Code, click `Terminal` -> `New Terminal` -> choose `Git Bash`
- Run:
    ```sh
    >> docker-compose up airflow-init
    ```
- After initialization is complete, we will see a message:
    ```sh
    airflow-init_1       | Upgrades done
    airflow-init_1       | Admin user airflow created
    airflow-init_1       | 2.3.2
    start_airflow-init_1 exited with code 0
    ```
- The account created with the login name is `airflow` and the password is `airflow`

### 3) Run/Stop Airflow:
- In Visual Studio Code, click `Terminal` -> `New Terminal` -> choose `Git Bash`
-- To run Airflow, use command:
    ```sh
    >> docker-compose up
    ```
- Entire initialization process can takes 1 minutes!
- After that, we can access Airflow webserver via `http://localhost:8080`
-- To stop Airflow, use command:
    ```sh
    >> docker-compose down
    ```
---
## Initialize PostgreSQL Database using pgAdmin ver.4
### 1) Download pgAdmin 4:
- At link https://www.pgadmin.org/download/
- Follow instructions to fully install the software
- The PostgreSQL remote database will run on port `5432`
### 2) Access pgAdmin 4, use Query Tool to create new database named `airflow`:
- Run SQL command: 
    ```sh
    CREATE DATABASE airflow;
    CREATE USER airflow WITH PASSWORD 'airflow';
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
    ```
- Running above commands, we will create new super-user named `airflow`, password `airflow`
---
## Modify `docker-compose.yml` file to connect with PostgreSQL Database
- **AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:** 
    ```sh
    postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
    ```
- **AIRFLOW__CORE__SQL_ALCHEMY_CONN:**
    ```sh
    postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
    ```
- **AIRFLOW__CELERY__RESULT_BACKEND:**
    ```sh
    db+postgresql://airflow:airflow@postgres:5432/airflow
    ```
<img src="./img/modify_yaml.png">

---
## Infrastructure metrics
- DAG monitoring screen:
<img src="./img/DAG_main.png">
- DAG monitoring via graph:
<img src="./img/DAG_graph.png">
- DAG monitoring via log:
<img src="./img/DAG_log.png">
- DAG monitoring via detailed log:
<img src="./img/DAG_log2.png">

---
## Data metrics
- DAG Details:
    + `Status` : status of a task (success/failed)
    + `Total success` : total number of successful runs
    + Displayed color: `green` success, `red` failed
- Data metrics:
    + `Primary key` : satisfies constraint
    + `Data type` : ensures in the right form
