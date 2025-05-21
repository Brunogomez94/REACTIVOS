import pandas as pd
import psycopg2
from psycopg2 import sql
import logging
import requests
import time

# Configuración del logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Ruta de descarga
csv_url = "https://www.contrataciones.gov.py/t/download/SieDocumento/10"
csv_path = "C:/Users/Bruno/Downloads/tienda_ordenes.csv"
csv_clean_path = "C:/Users/Bruno/Downloads/tienda_v_limpio.csv"

# Descarga del CSV
logging.info("=== Iniciando proceso ===")
logging.info(f"Descargando archivo desde {csv_url}...")
response = requests.get(csv_url)
with open(csv_path, "wb") as file:
    file.write(response.content)
logging.info(f"✅ Archivo descargado correctamente en: {csv_path}")

# Espera de 2 minutos
logging.info("Esperando 2 minutos antes de continuar...")
time.sleep(120)

# Lectura del archivo CSV
logging.info(f"Leyendo el archivo CSV desde {csv_path}...")
df = pd.read_csv(csv_path, sep=",", encoding='latin1')

# Eliminación de columnas innecesarias
columnas_a_eliminar = ['descripcion_item', 'forma_pago', 'link', 'now', 'producto_nacional', 'id_']
df.drop(columns=[col for col in columnas_a_eliminar if col in df.columns], inplace=True)

# Filtrado de datos
df = df[df['entidad'] == "Ministerio de Salud Pública y Bienestar Social"]

# Guardar CSV limpio
logging.info(f"✅ Filtrado completado: {len(df)} registros encontrados.")
df.to_csv(csv_clean_path, index=False)
logging.info(f"✅ Archivo CSV limpio guardado en: {csv_clean_path}")

# Conexión a PostgreSQL
logging.info("✅ Conexión a PostgreSQL exitosa.")
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    dbname="postgres",
    user="postgres",
    password="Dggies12345"
)
cur = conn.cursor()

# Creación de la tabla si no existe
cur.execute("""
    DROP TABLE IF EXISTS dncp_table;
    CREATE TABLE dncp_table (
        id SERIAL PRIMARY KEY,
        nro_orden_compra VARCHAR(255) NOT NULL,
        fecha_orden_compra DATE NOT NULL,
        nombre_entidad VARCHAR(255) NOT NULL,
        entidad VARCHAR(255),
        uc VARCHAR(255),
        ruc_completo VARCHAR(255),
        estado_orden VARCHAR(100),
        proveedor VARCHAR(255) NOT NULL,
        cc VARCHAR(255),
        cantidad NUMERIC(18, 2),
        codigo_catalogo VARCHAR(255),
        n5 VARCHAR(255),
        precio NUMERIC(18, 2),
        precio_unitario NUMERIC(18, 2),
        precio_total NUMERIC(18, 2),
        fuente_financiamiento_codigo VARCHAR(255),
        cantidad_calculada NUMERIC(18, 2),
        moneda VARCHAR(10) DEFAULT 'PYG'
    );
""")
conn.commit()
logging.info("✅ Tabla dncp_table creada o verificada correctamente.")

# Inserción por lotes (1000 en 1000)
lote = 1000
for i in range(0, len(df), lote):
    df_lote = df.iloc[i:i + lote]
    registros = list(df_lote.itertuples(index=False, name=None))
    
    insert_query = """
    INSERT INTO dncp_table (
        nro_orden_compra, fecha_orden_compra, nombre_entidad, entidad, uc, 
        ruc_completo, estado_orden, proveedor, cc, cantidad, 
        codigo_catalogo, n5, precio, precio_unitario, 
        precio_total, fuente_financiamiento_codigo, cantidad_calculada, moneda
    ) VALUES %s
    """
    
    try:
        psycopg2.extras.execute_values(cur, insert_query, registros, template=None, page_size=100)
        conn.commit()
        logging.info(f"✅ Lote {i // lote + 1}: Insertados {len(registros)} registros")
    except Exception as e:
        logging.error(f"❌ Error al subir los datos a PostgreSQL: {e}")
        conn.rollback()

# Cerrar la conexión
cur.close()
conn.close()
logging.info("✅ Proceso completado correctamente.")
