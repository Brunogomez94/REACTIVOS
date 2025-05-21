import psycopg2
from psycopg2 import sql
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox

# Datos de conexión
DB_NAME = "reactivos_db"
DB_USER = "postgres"  # Cambia si tu usuario es otro
DB_PASSWORD = "Dggies12345"
DB_HOST = "localhost"
DB_PORT = "5432"

# Sentencias para crear tablas (resumidas para ejemplo)
CREATE_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS contratos (
        id SERIAL PRIMARY KEY,
        codigo_reactivo VARCHAR(100),
        id_contrato VARCHAR(50),
        modalidad VARCHAR(100),
        numero_llamado VARCHAR(50),
        anio_llamado INTEGER,
        nombre_llamado TEXT,
        empresa_adjudicada VARCHAR(255),
        fecha_firma_contrato DATE,
        numero_contrato VARCHAR(50),
        vigencia_contrato VARCHAR(100),
        fecha_inicio_poliza DATE,
        fecha_finalizacion_poliza DATE,
        porcentaje_complementarios NUMERIC(10, 2),
        comodato VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS items (
        id SERIAL PRIMARY KEY,
        codigo_reactivo VARCHAR(100),
        estado_lote_item VARCHAR(50),
        lote INTEGER,
        item INTEGER,
        descripcion_producto TEXT,
        presentacion VARCHAR(255),
        marca VARCHAR(255),
        procedencia VARCHAR(255),
        descripcion_completa TEXT,
        unidad_medida VARCHAR(50),
        precio_unitario NUMERIC(15, 2),
        cantidad_minima INTEGER,
        cantidad_maxima INTEGER,
        redistribucion_minima INTEGER,
        redistribucion_maxima INTEGER,
        entradas_adendas INTEGER,
        salidas_adendas INTEGER,
        total_adjudicado INTEGER,
        cantidad_emitida INTEGER,
        saldo_emitir INTEGER,
        porcentaje_emitido NUMERIC(10, 2),
        contrato_id INTEGER REFERENCES contratos(id),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS servicios (
        id SERIAL PRIMARY KEY,
        codigo_servicio VARCHAR(100),
        nombre_servicio VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS ejecucion_por_servicio (
        id SERIAL PRIMARY KEY,
        codigo_reactivo_servicio VARCHAR(255),
        codigo_servicio VARCHAR(100),
        codigo_reactivo VARCHAR(100),
        estado_distribucion VARCHAR(50),
        servicio_beneficiario VARCHAR(255),
        porcentaje_complementarios NUMERIC(10, 2),
        comodato VARCHAR(50),
        estado_lote_item VARCHAR(50),
        lote INTEGER,
        item INTEGER,
        descripcion_completa TEXT,
        unidad_medida VARCHAR(50),
        precio_unitario NUMERIC(15, 2),
        cantidad_minima INTEGER,
        cantidad_maxima INTEGER,
        redistribucion_minima INTEGER,
        redistribucion_maxima INTEGER,
        entradas_adendas INTEGER,
        salidas_adendas INTEGER,
        total_adjudicado INTEGER,
        cantidad_emitida INTEGER,
        saldo_emitir INTEGER,
        porcentaje_emitido_servicio NUMERIC(10, 2),
        porcentaje_global NUMERIC(10, 2),
        observacion TEXT,
        servicio_id INTEGER REFERENCES servicios(id),
        item_id INTEGER REFERENCES items(id),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS ordenes_compra (
        id SERIAL PRIMARY KEY,
        simese_pedido VARCHAR(100),
        numero_orden VARCHAR(50),
        fecha_emision DATE,
        codigo_reactivo_servicio VARCHAR(255),
        codigo_reactivo VARCHAR(100),
        estado_distribucion VARCHAR(50),
        estado_lote_item VARCHAR(50),
        servicio_beneficiario VARCHAR(255),
        comodato VARCHAR(50),
        lote INTEGER,
        item INTEGER,
        cantidad_solicitada INTEGER,
        cantidad_complementaria INTEGER,
        unidad_medida VARCHAR(50),
        descripcion_completa TEXT,
        precio_unitario NUMERIC(15, 2),
        porcentaje_emitido_servicio NUMERIC(10, 2),
        porcentaje_global NUMERIC(10, 2),
        saldo_emitir_servicio INTEGER,
        monto_emitido NUMERIC(15, 2),
        porcentaje_complementarios NUMERIC(10, 2),
        observaciones TEXT,
        servicio_id INTEGER REFERENCES servicios(id),
        item_id INTEGER REFERENCES items(id),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
]

def conectar_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        messagebox.showerror("Error de Conexión", f"No se pudo conectar a la base de datos:\n{e}")
        return None

def crear_tablas(conn):
    try:
        cur = conn.cursor()
        for query in CREATE_TABLES_SQL:
            cur.execute(query)
        conn.commit()
        cur.close()
    except Exception as e:
        messagebox.showerror("Error al crear tablas", str(e))

def seleccionar_archivo():
    root = tk.Tk()
    root.withdraw()
    archivo = filedialog.askopenfilename(
        title="Selecciona el archivo Excel",
        filetypes=[("Archivos Excel", "*.xlsx *.xls")]
    )
    return archivo

def leer_excel(ruta):
    try:
        df = pd.read_excel(ruta)
        return df
    except Exception as e:
        messagebox.showerror("Error al leer Excel", f"No se pudo leer el archivo:\n{e}")
        return None

def insertar_contratos(conn, df):
    # Ejemplo simple de inserción, adaptar según columnas del Excel
    cur = conn.cursor()
    for _, row in df.iterrows():
        try:
            cur.execute("""
                INSERT INTO contratos (
                    codigo_reactivo, id_contrato, modalidad, numero_llamado,
                    anio_llamado, nombre_llamado, empresa_adjudicada,
                    fecha_firma_contrato, numero_contrato, vigencia_contrato,
                    fecha_inicio_poliza, fecha_finalizacion_poliza,
                    porcentaje_complementarios, comodato
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row.get("codigo_reactivo"), row.get("id_contrato"), row.get("modalidad"),
                row.get("numero_llamado"), row.get("anio_llamado"), row.get("nombre_llamado"),
                row.get("empresa_adjudicada"), row.get("fecha_firma_contrato"), row.get("numero_contrato"),
                row.get("vigencia_contrato"), row.get("fecha_inicio_poliza"), row.get("fecha_finalizacion_poliza"),
                row.get("porcentaje_complementarios"), row.get("comodato")
            ))
        except Exception as e:
            print(f"Error insertando fila: {e}")
    conn.commit()
    cur.close()

def main():
    conn = conectar_db()
    if not conn:
        return
    
    crear_tablas(conn)
    
    archivo = seleccionar_archivo()
    if not archivo:
        messagebox.showinfo("Cancelado", "No se seleccionó ningún archivo.")
        return
    
    df = leer_excel(archivo)
    if df is None:
        return
    
    # Aquí debes adaptar la función para insertar en la tabla que corresponda
    # Por ejemplo, inserto contratos para que veas cómo sería
    insertar_contratos(conn, df)
    
    messagebox.showinfo("Éxito", "Datos insertados correctamente en la tabla contratos.")
    conn.close()

if __name__ == "__main__":
    main()
