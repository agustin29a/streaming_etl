# 🎬 Streaming Platform - ETL Pipeline

## 📋 Description
Complete ETL pipeline for a streaming platform that processes data from AWS S3 to PostgreSQL, using Apache Airflow for orchestration and Pandas for data transformation.

**Project structure:**
- config/
  - airflow.cfg
- dags/
  - streaming-etl.py
- gold/
  - Data Analysis.ipynb
- plugins/
  - helpers/
    - metadata.py
    - my_utilities.py
- Dockerfile
- Presentation - ETL.pdf
- docker-compose.yaml

## 🏗️ Architecture
S3 (Bronze Layer) → Airflow ETL → S3 (Silver Layer) → PostgreSQL → Analysis

## ✨ Features
- **Orchestration with Airflow**: Automated and schedulable pipeline
- **AWS S3 Storage**: Bronze and Silver layers
- **AWS RDS PostgreSQL Database**: Complete relational schema
- **Processing with Pandas**: Efficient data transformation
- **Advanced Analysis**: SQL queries with window functions

## 🛠️ Technologies
- Apache Airflow: Pipeline orchestration
- Python 3.8+: Main language
- Pandas: Data processing
- PostgreSQL: Database
- AWS S3: Storage
- SQLAlchemy: ORM and connections
- Boto3: AWS client
- Matplotlib/Seaborn: Visualization

## 📊 Analysis Examples
The project includes advanced SQL queries with window functions for:
- Content ranking by genre
- User engagement analysis
- Growth trend analysis
- Subscription analysis
- Series popularity
