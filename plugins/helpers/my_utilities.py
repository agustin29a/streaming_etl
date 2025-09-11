import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import logging
import boto3
from io import BytesIO
from pathlib import Path

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if not os.getenv("AIRFLOW_HOME"):
    load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")
AWS_S3_BRONZE_FOLDER = os.getenv('AWS_S3_BRONZE_FOLDER', '')
AWS_S3_SILVER_FOLDER = os.getenv('AWS_S3_SILVER_FOLDER', '')
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

def create_aws_session(access_key=None, secret_key=None, region=None):
    """
    Crea y retorna una sesión de AWS boto3.
    
    Args:
        access_key (str, optional): AWS Access Key ID. Si es None, intentará usar variables de entorno.
        secret_key (str, optional): AWS Secret Access Key. Si es None, intentará usar variables de entorno.
        region (str, optional): Región de AWS. Si es None, intentará usar variables de entorno.
    
    Returns:
        boto3.Session: Sesión de AWS configurada
    
    Raises:
        ValueError: Si no se pueden encontrar las credenciales necesarias
    """
    # Obtener valores de las variables de entorno si no se proporcionan
    aws_access_key_id = access_key or AWS_ACCESS_KEY_ID
    aws_secret_access_key = secret_key or AWS_SECRET_ACCESS_KEY
    aws_region = region or AWS_DEFAULT_REGION

    # Verificar que tengamos las credenciales mínimas
    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError("AWS credentials not found. Provide them as parameters or set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.")
    
    # Crear y retornar la sesión
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )
    
    return session

def cargar_datos_desde_memoria(file_content, nombre_archivo, **kwargs):
    """
    Carga datos desde contenido en memoria a DataFrame según la extensión.
    """
    # Obtener la extensión del archivo
    extension = os.path.splitext(nombre_archivo)[1].lower()
    
    # Mapeo de extensiones a funciones de Pandas
    cargadores = {
        ".csv": lambda content, **kw: pd.read_csv(BytesIO(content), **kw),
        ".xlsx": lambda content, **kw: pd.read_excel(BytesIO(content), **kw),
        ".xls": lambda content, **kw: pd.read_excel(BytesIO(content), **kw),
        ".json": lambda content, **kw: pd.read_json(BytesIO(content), **kw),
        ".parquet": lambda content, **kw: pd.read_parquet(BytesIO(content), **kw),
        ".feather": lambda content, **kw: pd.read_feather(BytesIO(content), **kw),
        ".h5": lambda content, **kw: pd.read_hdf(BytesIO(content), **kw),
        ".hdf5": lambda content, **kw: pd.read_hdf(BytesIO(content), **kw),
        ".pkl": lambda content, **kw: pd.read_pickle(BytesIO(content), **kw),
        ".pickle": lambda content, **kw: pd.read_pickle(BytesIO(content), **kw),
    }
    
    # Verificar si la extensión es soportada
    if extension not in cargadores:
        logger.warning(f"Extensión '{extension}' no soportada para: {nombre_archivo}")
        return None
    
    try:
        # Cargar el archivo desde memoria
        df = cargadores[extension](file_content, **kwargs)
        return df
        
    except Exception as e:
        logger.error(f"Error al cargar {nombre_archivo}: {str(e)}")
        return None
    
def cargar_datos_s3(extensiones=None, **kwargs):
    """
    Carga datos desde archivos en un bucket de S3 a DataFrames de Pandas.

    Parámetros:
    -----------
    extensiones : list, optional
        Lista de extensiones a incluir (ej: ['.csv', '.xlsx']). Si es None, incluye todas.
    **kwargs : dict
        Argumentos adicionales para las funciones de Pandas
    
    Retorna:
    --------
    dict
        Diccionario con nombres de archivo como keys y DataFrames como values
    """

    session = create_aws_session()
    
    s3_client = session.client('s3')
    folder_s3 = AWS_S3_BRONZE_FOLDER

    if not folder_s3.endswith('/'):
        folder_s3 += '/'
    
    dataframes = {}
    
    try:
        response = s3_client.list_objects_v2(
            Bucket=AWS_S3_BUCKET,
            Prefix=folder_s3
        )
        
        if 'Contents' not in response:
            return dataframes
        
        for obj in response['Contents']:
            archivo_key = obj['Key']
            
            if archivo_key.endswith('/'):
                continue
            
            nombre_archivo = os.path.basename(archivo_key)
            extension = os.path.splitext(nombre_archivo)[1].lower()
            
            # Filtrar por extensiones si se especificó
            if extensiones and extension not in extensiones:
                continue
            
            try:
                response_obj = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=archivo_key)
                file_content = response_obj['Body'].read()
                
                df = cargar_datos_desde_memoria(file_content, nombre_archivo, **kwargs)
                
                if df is not None:
                    nombre_sin_extension = Path(nombre_archivo).stem
                    dataframes[nombre_sin_extension] = df

            except Exception as e:
                logger.error(f"Error con {archivo_key}: {e}")
                continue
        
        return dataframes
    
    except Exception as e:
        logger.error(f"Error general: {e}")
        return {}    

def limpiar_diccionario(dfs: dict, threshold_fecha: float = 0.1):
    """
    Aplica transformaciones típicas de limpieza a un diccionario de DataFrames.

    Args:
        dfs (dict[str, pd.DataFrame]): Diccionario con DataFrames de pandas.
        threshold_fecha (float): Proporción mínima para considerar que una columna es fecha.

    Returns:
        dict[str, pd.DataFrame]: Nuevo diccionario con DataFrames transformados.
    """
    def es_fecha(serie: pd.Series) -> bool:
      # Primero intentar con el formato más específico (con microsegundos)
      fechas = pd.to_datetime(serie, format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
    
      # Para los que fallaron, intentar con formato de hora sin microsegundos
      mask = fechas.isna()
      fechas_con_hora = pd.to_datetime(serie[mask], format='%Y-%m-%d %H:%M:%S', errors='coerce')
      fechas.update(fechas_con_hora)
    
      # Para los que aún fallan, intentar con solo fecha
      mask = fechas.isna()
      fechas_solo_fecha = pd.to_datetime(serie[mask], format='%Y-%m-%d', errors='coerce')
      fechas.update(fechas_solo_fecha)
    
      return fechas.notna().mean() >= threshold_fecha

    dfs_limpios = {}

    for nombre, df in dfs.items():
        df = df.copy()

        # 🔹 1. Eliminar duplicados
        df = df.drop_duplicates()

        # 🔹 2. Detectar y convertir tipos de datos
        for col in df.columns:
            serie = df[col]

            # Si es fecha
            if serie.dtype == "object" and es_fecha(serie):
                df[col] = pd.to_datetime(serie, errors='coerce')

        # 🔹 3. Manejo de nulos
        for col in df.columns:
            if df[col].dtype in ["float64", "int64"]:
                df[col] = df[col].fillna(0)  # default para numéricos
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].fillna(pd.NaT)  # mantener NaT
            else:
                df[col] = df[col].fillna("")  # default para strings

        dfs_limpios[nombre] = df

    return dfs_limpios

def convertir_dataframes_a_parquet_s3(diccionario_dataframes):
    """
    Convierte DataFrames de un diccionario a archivos Parquet y los guarda en S3
    
    Args:
        diccionario_dataframes (dict): Diccionario con nombres de archivo como keys
                                      y DataFrames de pandas como values
    
    Returns:
        bool: True si todos los archivos se procesaron exitosamente, False si hubo errores
    """
    
    try:
        # Configurar sesión de AWS
        session = create_aws_session()
        
        s3_client = session.client('s3')
        
        # Validar que el bucket existe
        if not AWS_S3_BUCKET:
            logger.error("El nombre del bucket S3 no está configurado en las variables de entorno")
            return False
        
        logger.info(f"Iniciando proceso de conversión para {len(diccionario_dataframes)} DataFrames")
        logger.info(f"Bucket destino: {AWS_S3_BUCKET}")
        logger.info(f"Folder destino: {AWS_S3_SILVER_FOLDER}")
        
        contador_exitosos = 0
        contador_errores = 0
        
        # Recorrer el diccionario de DataFrames
        for nombre_archivo, dataframe in diccionario_dataframes.items():
            try:
                # Validar que el DataFrame no esté vacío
                if dataframe is None:
                    logger.warning(f"DataFrame '{nombre_archivo}' es None, se omite")
                    contador_errores += 1
                    continue
                
                if dataframe.empty:
                    logger.warning(f"DataFrame '{nombre_archivo}' está vacío, se omite")
                    contador_errores += 1
                    continue
                
                # Asegurar que el nombre del archivo tenga extensión .parquet
                if not nombre_archivo.endswith('.parquet'):
                    nombre_archivo += '.parquet'
                
                # Construir la ruta completa en S3
                s3_key = os.path.join(AWS_S3_SILVER_FOLDER, nombre_archivo).replace('\\', '/')
                
                # Convertir DataFrame a bytes en memoria
                parquet_buffer = BytesIO()
                dataframe.to_parquet(parquet_buffer, index=False, engine='pyarrow')
                parquet_buffer.seek(0)  # Volver al inicio del buffer
                
                # Subir a S3
                s3_client.put_object(
                    Bucket=AWS_S3_BUCKET,
                    Key=s3_key,
                    Body=parquet_buffer,
                    ContentType='application/parquet'
                )
                
                logger.info(f"Archivo '{s3_key}' guardado exitosamente en S3")
                contador_exitosos += 1
                
            except Exception as e:
                logger.error(f"Error procesando '{nombre_archivo}': {str(e)}")
                contador_errores += 1
        
        # Resumen del proceso
        logger.info(f"Proceso completado. Exitosos: {contador_exitosos}, Errores: {contador_errores}")
        
        if contador_errores == 0:
            logger.info("Todos los archivos fueron procesados exitosamente")
            return True
        else:
            logger.warning(f"Proceso completado con {contador_errores} errores")
            return False
            
    except Exception as e:
        logger.error(f"Error general en la configuración de AWS: {str(e)}")
        return False


def create_tables_with_constraints(dataframes_dict, metadata_dict, engine):
    """
    Crea tablas en una base de datos PostgreSQL a partir de DataFrames de pandas
    y establece las constraints (PK y FK) definidas en los metadatos.
    Elimina automáticamente el prefijo 'df_' de los nombres de las tablas.

    Parameters:
    -----------
    dataframes_dict : dict
        Diccionario donde las claves son nombres de tablas (con prefijo df_) y los valores son DataFrames
    metadata_dict : dict
        Diccionario con metadatos para cada tabla (PK, FK, if_exists)
    engine : sqlalchemy.engine.Engine
        Objeto engine de SQLAlchemy para la conexión a la base de datos
    """

    # Mapeo de nombres originales a nombres sin df_
    table_name_mapping = {
        original: original.replace("df_", "") for original in dataframes_dict.keys()
    }

    # Primera pasada: crear todas las tablas sin constraints
    for original_name, df in dataframes_dict.items():
        table_name = table_name_mapping[original_name]

        if original_name in metadata_dict:
            table_metadata = metadata_dict[original_name]
            if_exists = table_metadata.get("if_exists", "replace")
        else:
            if_exists = "replace"

        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            method="multi",
        )
        logger.info(f"Tabla '{table_name}' creada exitosamente (original: {original_name})")

    # Segunda pasada: agregar constraints
    with engine.connect() as conn:
        for original_name, metadata in metadata_dict.items():
            if original_name not in dataframes_dict:
                continue

            table_name = table_name_mapping[original_name]

            # Agregar primary keys
            if "primary_keys" in metadata and metadata["primary_keys"]:
                pk_columns = ", ".join(metadata["primary_keys"])
                alter_pk = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({pk_columns});"
                try:
                    with conn.begin():  # maneja commit/rollback automáticamente
                        conn.execute(text(alter_pk))
                    logger.info(f"Primary key agregada a la tabla '{table_name}'")
                except Exception as e:
                    logger.error(f"Error al agregar PK a '{table_name}': {str(e)}")

            # Agregar foreign keys
            if "foreign_keys" in metadata and metadata["foreign_keys"]:
                for fk_column, fk_info in metadata["foreign_keys"].items():
                    referenced_table = fk_info["table"].replace("df_", "")
                    alter_fk = f"""
                    ALTER TABLE {table_name} 
                    ADD CONSTRAINT fk_{table_name}_{fk_column}
                    FOREIGN KEY ({fk_column}) 
                    REFERENCES {referenced_table} ({fk_info['column']});
                    """
                    try:
                        with conn.begin():
                            conn.execute(text(alter_fk))
                        logger.info(
                            f"Foreign key agregada a la tabla '{table_name}' (columna: {fk_column})"
                        )
                    except Exception as e:
                        logger.error(
                            f"Error al agregar FK a '{table_name}.{fk_column}': {str(e)}"
                        )

    logger.info("Proceso completado")


def get_db_engine():
    """
    Crea y devuelve un engine de SQLAlchemy para PostgreSQL configurado con variables de entorno.

    Returns:
        sqlalchemy.engine.Engine: Engine configurado para la conexión a la base de datos
    """

    # Configuración de la conexión
    db_config = {
        "host": DB_HOST,
        "port": int(DB_PORT),
        "user": DB_USER,
        "password": DB_PASSWORD,
        "database": DB_NAME,
    }

    # Crear cadena de conexión para PostgreSQL
    connection_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"

    # Crear y devolver el engine
    engine = create_engine(connection_string)
    return engine


