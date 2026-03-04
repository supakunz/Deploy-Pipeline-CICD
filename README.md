# 📘 COVID-19 ETL Data Pipeline

> End-to-end data engineering pipeline with Docker, Apache Airflow, PostgreSQL, Great Expectations, and GitHub Actions CI.

## 🧾 Project Overview

This project demonstrates a complete ETL workflow for COVID-19 data:
- Load raw CSV data into PostgreSQL
- Orchestrate ETL with Airflow
- Clean and standardize data with Python (Pandas)
- Validate data quality using Great Expectations
- Export cleaned output for analytics/notebooks

It also includes automated testing and QA in GitHub Actions.

## ⚙️ Architecture (High Level)

<img width="1292" height="658" alt="Image" src="https://github.com/user-attachments/assets/6040b720-e4f4-4587-aa69-8e36d7b3569d" />

## 💡 Technology Stack

**Programming Languages:**
- Python
- SQL

**Data Processing & Orchestration:**
- Apache Airflow
- Pandas

**Data Storage:**
- PostgreSQL
- MinIO (S3-compatible Object Storage)
- CSV (raw/output)

**Data Quality:**
- Great Expectations

**Infrastructure / DevOps:**
- Docker & Docker Compose
- GitHub Actions (CI)

**Analysis:**
- Jupyter Notebook

## 🐳 Docker Services

Defined in `docker-compose.yml`:
- `postgres` : metadata DB + raw source table (`covid_data`)
- `minio` : object storage for raw/processed pipeline outputs
- `createbuckets` : bootstrap bucket creation (`data-lake`)
- `load-csv` : one-time loader to ingest CSV into PostgreSQL
- `airflow-init` : initialize Airflow DB and create admin user
- `airflow-scheduler` : run DAG scheduling
- `airflow-webserver` : Airflow UI
- `jupyter` : notebook environment for analysis

## 📂 Data Source

- Raw file: `load-csv/data/covid_19_data.csv`
- Loader script: `load-csv/load_csv.py`
- Target source table: `public.covid_data` (PostgreSQL)

## 💾 MinIO Data Lake

MinIO is used as an S3-compatible data lake to store pipeline artifacts from both extraction and load stages.

- Bucket: `data-lake`
- Raw object path: `covid/raw/covid_19_raw.csv`
- Curated object path: `covid/output/covid_19_clean.csv`

<img width="1456" height="493" alt="Image" src="https://github.com/user-attachments/assets/f945f460-c5b9-4f10-b6d3-0b73407d2f0f" />

## 🔄 Airflow ETL Workflow

<img width="1932" height="446" alt="Image" src="https://github.com/user-attachments/assets/45cfaa68-0a7e-4fa3-96f5-f6879e772420" />

Task sequence:
1. `start`
2. `load_data`  
   Read data from PostgreSQL, write to `data/raw/covid_19_raw.csv`, then upload raw file to MinIO (`data-lake/covid/raw/covid_19_raw.csv`)
3. `clean_data`  
   Standardize datetime, normalize column names (snake_case), cast data types
4. `handle_missing_value`  
   Fill missing values, remove invalid datetime rows, fix negative metrics
5. `validate_data`  
   Run Great Expectations checkpoint (`include/gx/checkpoints/covid_checkpoint.yml`)
6. `load_to_warehouse`  
   Export final cleaned dataset to output CSV (`data/output/covid_19_clean.csv`) and upload to MinIO (`data-lake/covid/output/covid_19_clean.csv`)
7. `end`

## 🏬 Warehouse Load Note

- Current pipeline simulates warehouse loading via CSV/MinIO.
- BigQuery is the target warehouse and is planned as the next integration step.

## ✅ Data Quality Rules (Great Expectations)

Expectation suite: `include/gx/expectations/covid_data_suite.json`

Key checks include:
- Required schema/columns must match
- `sno` must be non-null and unique
- Date/time columns must match expected format
- `confirmed`, `deaths`, `recovered` must be non-null, integer, and `>= 0`

## 🧪 Testing & CICD

CI workflow: `.github/workflows/de-pipeline-ci.yml`

On `push` to `main`/`dev` and on `pull_request`, CI runs:
1. Python setup (3.10)
2. Dependency installation
3. Unit tests (`pytest tests/`)
4. Data QA test via `validate_data(...)` on mock dataset

## 🚀 Setup & Run

### 1. Clone repository
```bash
git clone <your-repo-url>
cd Deploy-Pipeline-CICD
```

### 2. Start all services
```bash
docker compose up -d --build
```

### 3. Access services
- Airflow UI: `http://localhost:8080`
  - Username: `admin`
  - Password: `admin`
- Jupyter Lab: `http://localhost:8888`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001` (user/password: `minioadmin` / `minioadmin`)
- PostgreSQL: `localhost:5432`

### 4. Trigger DAG
1. Open Airflow UI
2. Enable `etl_pipeline`
3. Trigger a DAG run manually (or wait for schedule)

### 5. Check outputs
- Raw extracted file: `data/raw/covid_19_raw.csv`
- Cleaned output file: `data/output/covid_19_clean.csv`

## ▶️ Run Tests Locally

```bash
pip install -r airflow/requirements.txt
PYTHONPATH=$(pwd)/plugins pytest tests/ -q
```

## 📁 Project Structure

```text
.
├── .github/workflows/de-pipeline-ci.yml
├── dags/etl_dag.py
├── plugins/
│   ├── load_data.py
│   ├── clean_date.py
│   ├── handle_missing_value.py
│   ├── validate_data.py
│   └── load_to_warehouse.py
├── include/gx/
│   ├── great_expectations.yml
│   ├── checkpoints/covid_checkpoint.yml
│   └── expectations/covid_data_suite.json
├── load-csv/
│   ├── load_csv.py
│   └── data/covid_19_data.csv
├── tests/
│   ├── test_pipeline.py
│   └── mock_data/
└── docker-compose.yml
```

## 🙋‍♂️ Contact

Developed by **Supakun Thata**  
📧 Email: supakunt.thata@gmail.com  
🔗 GitHub: [SupakunZ](https://github.com/SupakunZ)
