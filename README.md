# 🎬 Plataforma de Streaming - ETL Pipeline
## 📋 Descripción

Pipeline ETL completo para una plataforma de streaming que procesa datos desde AWS S3 hasta PostgreSQL, utilizando Apache Airflow para orquestación y Pandas para transformación de datos.

**Estructura del proyecto:**
- config/
  - airflow.cfg
- dags/
  - streaming-etl.py
- gold/
  - Analisis de datos.ipynb
- plugins/
  - helpers/
    - metadata.py
    - my_utilities.py
- Dockerfile
- Presentacion - ETL.pdf
- docker-compose.yaml
  
## 🏗️ Arquitectura
S3 (Bronze Layer) → Airflow ETL → S3 (Silver Layer) → PostgreSQL → Análisis

## ✨ Características

- **Orquestación con Airflow**: Pipeline automatizado y programable
- **Almacenamiento en AWS S3**: Bronze y Silver
- **Base de Datos AWS RDS PostgreSQL**: Esquema relacional completo
- **Procesamiento con Pandas**: Transformación eficiente de datos
- **Análisis Avanzado**: Consultas SQL con funciones de ventana

🛠️ Tecnologías
- Apache Airflow: Orquestación del pipeline
- Python 3.8+: Lenguaje principal
- Pandas: Procesamiento de datos
- PostgreSQL: Base de datos
- AWS S3: Almacenamiento
- SQLAlchemy: ORM y conexiones
- Boto3: Cliente AWS
- Matplotlib/Seaborn: Visualización

📊 Ejemplos de Análisis
El proyecto incluye consultas SQL avanzadas con funciones de ventana para:
- Ranking de contenido por género
- Análisis de engagement por usuario
- Tendencia de crecimiento
- Análisis de suscripciones
- Popularidad de series
