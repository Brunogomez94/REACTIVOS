import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import glob
import requests
import logging
from datetime import datetime
import shutil
from pathlib import Path

# Configuración de logging
def setup_logging(log_dir="logs"):
    """Configura el sistema de logging"""
    # Crear directorio de logs si no existe
    os.makedirs(log_dir, exist_ok=True)
    
    # Configurar formato de fecha para el nombre del archivo
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"database_import_{timestamp}.log")
    
    # Configurar el logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),  # Especificar UTF-8 para el archivo
            logging.StreamHandler()  # Para mostrar logs en consola también
        ]
    )
    
    logger = logging.getLogger()
    
    # Mensajes iniciales
    logger.info(f"Iniciando proceso. Log guardado en: {log_file}")
    return logger

def descargar_csv(url, directorio_destino, nombre_archivo=None):
    """
    Descarga un archivo CSV desde una URL
    
    Args:
        url: URL del archivo CSV
        directorio_destino: Directorio donde guardar el archivo
        nombre_archivo: Nombre para el archivo descargado (opcional)
        
    Returns:
        Ruta completa al archivo descargado
    """
    logger = logging.getLogger()
    
    # Crear directorio de destino si no existe
    os.makedirs(directorio_destino, exist_ok=True)
    
    # Si no se proporciona nombre de archivo, usar timestamp
    if not nombre_archivo:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nombre_archivo = f"datos_{timestamp}.csv"
    
    ruta_archivo = os.path.join(directorio_destino, nombre_archivo)
    
    # Borrar archivo anterior si existe
    if os.path.exists(ruta_archivo):
        logger.info(f"Eliminando archivo anterior: {ruta_archivo}")
        os.remove(ruta_archivo)
    
    # Descargar el nuevo archivo
    try:
        logger.info(f"Descargando archivo desde: {url}")
        logger.info("La descarga puede tardar varios minutos para archivos grandes...")
        
        # Iniciar tiempo para medir la duración de la descarga
        tiempo_inicio = datetime.now()
        
        # Descargar el archivo con indicador de progreso
        response = requests.get(url, stream=True, timeout=120)  # Aumentado el timeout para archivos grandes
        response.raise_for_status()
        
        # Obtener tamaño total si está disponible
        try:
            tamaño_total = int(response.headers.get('content-length', 0))
            tamaño_mb = tamaño_total / (1024 * 1024)
            if tamaño_mb > 0:
                logger.info(f"Tamaño del archivo: {tamaño_mb:.2f} MB")
        except:
            # Si no podemos obtener el tamaño, continuamos sin esta información
            pass
            
        # Guardar el archivo con actualización de progreso
        descargado = 0
        último_porcentaje = 0
        with open(ruta_archivo, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    descargado += len(chunk)
                    
                    # Mostrar progreso cada 10%
                    if tamaño_total > 0:
                        porcentaje = int((descargado / tamaño_total) * 100)
                        if porcentaje >= último_porcentaje + 10:
                            último_porcentaje = porcentaje
                            logger.info(f"Progreso de descarga: {porcentaje}% completado")
        
        # Calcular tiempo total de descarga
        tiempo_total = (datetime.now() - tiempo_inicio).total_seconds()
        logger.info(f"Archivo descargado en {tiempo_total:.2f} segundos")
        logger.info(f"Archivo guardado en: {ruta_archivo}")
        
        return ruta_archivo
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al descargar el archivo: {str(e)}")
        raise

def conectar_postgresql(host, database, user, password, port=5432):
    """Crea una conexión a PostgreSQL usando SQLAlchemy"""
    logger = logging.getLogger()
    
    try:
        conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(conn_string)
        # Probar la conexión
        with engine.connect() as conn:
            pass
        logger.info(f"Conexión exitosa a PostgreSQL: {host}:{port}/{database}")
        return engine
    except Exception as e:
        logger.error(f"Error al conectar a PostgreSQL: {str(e)}")
        raise

def limpiar_tabla_existente(engine, nombre_tabla, schema=None):
    """
    Elimina los datos de una tabla existente
    
    Args:
        engine: Conexión SQLAlchemy a PostgreSQL
        nombre_tabla: Nombre de la tabla a limpiar
        schema: Esquema de PostgreSQL donde está la tabla (opcional)
    """
    logger = logging.getLogger()
    
    schema_usado = schema if schema else 'public'
    tabla_completa = f"{schema_usado}.{nombre_tabla}"
    
    try:
        with engine.connect() as conn:
            # Verificar si la tabla existe - usando sqlalchemy text()
            from sqlalchemy import text
            
            # Crear consulta parametrizada correctamente
            query = text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = :schema
                    AND table_name = :tabla
                )
            """)
            
            # Ejecutar consulta con parámetros
            resultado = conn.execute(
                query, 
                {"schema": schema_usado, "tabla": nombre_tabla}
            ).scalar()
            
            if resultado:
                # Si la tabla existe, eliminar todos los datos
                truncate_query = text(f"TRUNCATE TABLE {tabla_completa} RESTART IDENTITY")
                conn.execute(truncate_query)
                logger.info(f"Tabla {tabla_completa} limpiada correctamente")
            else:
                logger.info(f"La tabla {tabla_completa} no existe, no es necesario limpiarla")
    except Exception as e:
        logger.error(f"Error al limpiar tabla {tabla_completa}: {str(e)}")
        raise

def detectar_delimitador(ruta_archivo):
    """
    Detecta el delimitador más probable en un archivo CSV
    
    Args:
        ruta_archivo: Ruta al archivo CSV
        
    Returns:
        El delimitador detectado
    """
    logger = logging.getLogger()
    
    try:
        # Leer las primeras líneas para detectar el delimitador
        with open(ruta_archivo, 'r', encoding='utf-8', errors='ignore') as f:
            primer_linea = f.readline().strip()
        
        # Comprobar posibles delimitadores
        posibles_delimitadores = [',', ';', '|', '\t']
        delimitador = ','  # Por defecto
        max_campos = 0
        
        for delim in posibles_delimitadores:
            campos = primer_linea.split(delim)
            if len(campos) > max_campos:
                max_campos = len(campos)
                delimitador = delim
        
        logger.info(f"Delimitador detectado: '{delimitador}' (genera {max_campos} campos)")
        return delimitador
    except Exception as e:
        logger.warning(f"Error al detectar delimitador: {str(e)}. Usando ',' por defecto.")
        return ','

def procesar_csv(ruta_archivo, engine, schema=None, nombre_tabla=None):
    """
    Procesa un archivo CSV y lo carga en PostgreSQL
    
    Args:
        ruta_archivo: Ruta al archivo CSV
        engine: Conexión SQLAlchemy a PostgreSQL
        schema: Esquema de PostgreSQL donde crear la tabla (opcional)
        nombre_tabla: Nombre para la tabla en PostgreSQL (opcional)
    """
    logger = logging.getLogger()
    
    try:
        # Obtener nombre del archivo sin extensión si no se proporciona nombre_tabla
        if not nombre_tabla:
            nombre_tabla = Path(ruta_archivo).stem.lower().replace(" ", "_")
        
        logger.info(f"Procesando archivo: {ruta_archivo}")
        logger.info(f"Se usará el nombre de tabla: {nombre_tabla}")
        
        # Detectar el delimitador
        delimitador = detectar_delimitador(ruta_archivo)
        
        # Intentar leer el CSV con el delimitador detectado
        try:
            # Informar al usuario que estamos leyendo el archivo
            logger.info(f"Leyendo el archivo CSV con delimitador '{delimitador}'...")
            
            # Intentar primera lectura con configuración estándar
            df = pd.read_csv(
                ruta_archivo, 
                sep=delimitador,
                quotechar='"',
                escapechar='\\',
                on_bad_lines='skip',
                low_memory=False
            )
            
            # Si hay pocas columnas, intentar con otra configuración
            if len(df.columns) <= 1 and delimitador == ',':
                logger.warning("Detectadas pocas columnas, probando lectura alternativa...")
                df = pd.read_csv(
                    ruta_archivo,
                    sep=None,  # Intentar detectar automáticamente
                    engine='python',
                    quotechar='"',
                    on_bad_lines='skip'
                )
            
            logger.info(f"CSV leído exitosamente con {len(df.columns)} columnas y {len(df)} filas")
            logger.info(f"Columnas detectadas: {', '.join(df.columns.tolist())}")
            
        except Exception as e:
            logger.error(f"Error al leer CSV con configuración automática: {str(e)}")
            logger.info("Intentando abrir el archivo como un archivo delimitado por comas simple...")
            
            # Intento con encoding alternativo
            df = pd.read_csv(ruta_archivo, sep=';', encoding='latin1', on_bad_lines='skip')
            logger.info(f"CSV leído con configuración alternativa: {len(df.columns)} columnas y {len(df)} filas")
        
        # Limpiar la tabla existente antes de cargar los nuevos datos
        limpiar_tabla_existente(engine, nombre_tabla, schema)
        
        # Cargar el DataFrame a PostgreSQL
        try:
            logger.info(f"Cargando datos en PostgreSQL... (esto puede tardar varios minutos)")
            tiempo_inicio = datetime.now()
            
            df.to_sql(
                nombre_tabla, 
                engine, 
                schema=schema,
                if_exists='replace',  # Reemplazar si ya existe
                index=False,
                chunksize=5000  # Usar chunks para procesar archivos grandes más eficientemente
            )
            
            tiempo_total = (datetime.now() - tiempo_inicio).total_seconds()
            logger.info(f"Datos cargados en tabla: {nombre_tabla}")
            logger.info(f"Total de filas cargadas: {len(df)}")
            logger.info(f"Tiempo de carga: {tiempo_total:.2f} segundos")
            return len(df)
            
        except Exception as e:
            logger.error(f"Error al cargar tabla {nombre_tabla}: {str(e)}")
            raise
            
    except Exception as e:
        logger.error(f"Error al procesar archivo CSV: {str(e)}")
        raise

def procesar_archivos_excel(ruta_directorio, engine, schema=None):
    """
    Procesa todos los archivos Excel en un directorio y los carga en PostgreSQL
    
    Args:
        ruta_directorio: Ruta donde se encuentran los archivos Excel
        engine: Conexión SQLAlchemy a PostgreSQL
        schema: Esquema de PostgreSQL donde crear las tablas (opcional)
    """
    logger = logging.getLogger()
    
    # Obtener lista de archivos Excel
    archivos_excel = glob.glob(os.path.join(ruta_directorio, "*.xlsx"))
    logger.info(f"Se encontraron {len(archivos_excel)} archivos Excel para procesar")
    
    for archivo in archivos_excel:
        nombre_archivo = os.path.basename(archivo).split('.')[0]
        logger.info(f"Procesando archivo Excel: {nombre_archivo}")
        
        try:
            # Leer todas las hojas del archivo Excel
            excel = pd.ExcelFile(archivo)
            
            # Procesar cada hoja
            for nombre_hoja in excel.sheet_names:
                logger.info(f"  - Procesando hoja: {nombre_hoja}")
                
                # Crear un nombre para la tabla en PostgreSQL
                nombre_tabla = f"{nombre_archivo}_{nombre_hoja}".lower().replace(" ", "_")
                
                # Limpiar la tabla existente
                limpiar_tabla_existente(engine, nombre_tabla, schema)
                
                # Leer la hoja como DataFrame
                df = pd.read_excel(excel, sheet_name=nombre_hoja)
                
                # Cargar el DataFrame a PostgreSQL
                try:
                    df.to_sql(
                        nombre_tabla, 
                        engine, 
                        schema=schema,
                        if_exists='replace',# Reemplazar si ya existe
                        index=False,
                        chunksize=5000  # Usar chunks para mejor rendimiento
                    )
                    logger.info(f"    Datos cargados en tabla: {nombre_tabla}")
                    logger.info(f"    Total de filas cargadas: {len(df)}")
                except Exception as e:
                    logger.error(f"    Error al cargar tabla {nombre_tabla}: {str(e)}")
        except Exception as e:
            logger.error(f"Error al procesar archivo Excel {archivo}: {str(e)}")
    
    return len(archivos_excel)
def main():
    # Inicializar logging
    logger = setup_logging()
    
    try:
        # Configuración de la conexión a PostgreSQL
        config = {
            'host': 'localhost',
            'database': 'postgres',
            'user': 'postgres',
            'password': 'Dggies12345',
            'port': 5432
        }
        
        # URL del CSV a descargar
        csv_url = "https://www.contrataciones.gov.py/t/download/SieDocumento/10"
        
        # Directorios de trabajo
        directorio_descargas = 'descargas'
        directorio_excel = 'archivos_excel'  
        
        # Esquema de PostgreSQL (opcional, usar None para el esquema por defecto)
        schema = 'public'
        
        # Nombre fijo para el archivo y la tabla (opcional)
        nombre_archivo_csv = "contrataciones_datos.csv"
        nombre_tabla = "contrataciones_datos"
        
        logger.info("=== Iniciando proceso de importación de datos ===")
        
        # Crear conexión a PostgreSQL
        engine = conectar_postgresql(**config)
        
        # Descargar CSV desde la URL
        ruta_csv = descargar_csv(csv_url, directorio_descargas, nombre_archivo_csv)
        
        # Procesar archivo CSV descargado
        filas_cargadas = procesar_csv(ruta_csv, engine, schema, nombre_tabla)
        
        # Procesar archivos Excel si es necesario
        # Descomenta la siguiente línea si también necesitas procesar archivos Excel
        # excel_procesados = procesar_archivos_excel(directorio_excel, engine, schema)
        # if excel_procesados > 0:
        #     logger.info(f"Se procesaron {excel_procesados} archivos Excel")
        
        logger.info(f"=== Proceso completado con éxito. {filas_cargadas} filas importadas ===")
        
        # Ejecutar una consulta simple para verificar los datos cargados
        from sqlalchemy import text
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{nombre_tabla}")).scalar()
            logger.info(f"Verificación final: {result} filas en tabla {schema}.{nombre_tabla}")
        
    except Exception as e:
        logger.error(f"Error fatal en el proceso principal: {str(e)}")
        import traceback
        logger.error(f"Detalles del error: {traceback.format_exc()}")
    finally:
        logger.info("Proceso finalizado.")
        # No necesitamos cerrar engine explícitamente, SQLAlchemy lo maneja

if __name__ == "__main__":
    main()