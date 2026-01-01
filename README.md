# ETL Weather Pipeline

This project demonstrates a **Data Engineering ETL pipeline** using **Apache Airflow**, **PostgreSQL**, and **Open-Meteo API**. The pipeline extracts weather data for a specific location, transforms it, and loads it into a PostgreSQL database.

---

## Project Overview

The ETL pipeline performs the following steps:

1. **Extract**: Fetch current weather data from the Open-Meteo API for a specified latitude and longitude.
2. **Transform**: Select relevant fields like temperature, windspeed, wind direction, and weather code.
3. **Load**: Store the transformed data into a PostgreSQL database table.

---

## Technologies Used

- Apache Airflow (via Astro CLI)
- Python 3.11
- PostgreSQL (Docker container)
- DBeaver (for database inspection)
- Python libraries: `requests`, `json`, `pandas`, `airflow.providers.http`, `airflow.providers.postgres`

---

## Project Structure

DE ETL_Pipelines/
├── dags/
│ └── etlweather.py # Main Airflow DAG
├── .gitignore
├── README.md
└── requirements.txt # Python dependencies


---

## Setup Instructions

### 1. Install dependencies

- Docker
- Astro CLI (Astronomer)
- PostgreSQL container

### 2. Configure Airflow Connections

- **Open-Meteo API**  
  Conn Id: `open_meteo_api`  
  Conn Type: `HTTP`  
  Host: `https://api.open-meteo.com/`  

- **PostgreSQL**  
  Conn Id: `postgres_default`  
  Conn Type: `Postgres`  
  Host: `<postgres container name>`  
  Schema: `postgres`  
  Login: `postgres`  
  Password: `postgres`  
  Port: `5432`

### 3. Run the Airflow Pipeline

```bash
cd "D:\Notes\STUDY\Industry Project\DE ETL_Pipelines"
astro dev start


