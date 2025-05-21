import pandas as pd
import psycopg2
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import re
import os

# Configuración de la conexión a PostgreSQL
DB_HOST = "localhost"
DB_NAME = "reactivos_db"
DB_USER = "postgres"
DB_PASSWORD = "Dggies12345"  # Cambia esto por tu contraseña
DB_PORT = "5432"

def clean_column_name(name):
    """Limpia los nombres de columnas para que sean válidos en PostgreSQL"""
    if name is None or isinstance(name, float):
        return "columna_sin_nombre"
    
    # Reemplazar caracteres no válidos con guiones bajos
    name = str(name).strip()
    name = re.sub(r'[^\w\s]', '_', name)
    name = re.sub(r'\s+', '_', name)
    name = name.lower()
    
    # Asegurarse de que no empiece con número
    if name[0].isdigit():
        name = "col_" + name
    
    # Evitar nombres muy largos (PostgreSQL tiene un límite)
    if len(name) > 63:
        name = name[:63]
    
    return name

def create_database(conn_string):
    """Crea la base de datos si no existe"""
    # Conectarse a la base de datos por defecto para poder crear una nueva
    conn = psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        database="postgres"  # Nos conectamos a la BD por defecto
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Verificar si la base de datos existe
    cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DB_NAME}'")
    exists = cursor.fetchone()
    
    if not exists:
        print(f"Creando base de datos {DB_NAME}...")
        cursor.execute(f"CREATE DATABASE {DB_NAME}")
        print(f"Base de datos {DB_NAME} creada exitosamente")
    else:
        print(f"La base de datos {DB_NAME} ya existe")
    
    cursor.close()
    conn.close()

from sqlalchemy import create_engine, text

# Datos de conexión
DB_HOST = "localhost"
DB_NAME = "reactivos_db"
DB_USER = "postgres"
DB_PASSWORD = "Dggies12345"
DB_PORT = "5432"

# Crear el engine para SQLAlchemy
connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

def create_tables(engine):
    print("🔄 Creando tablas en la base de datos...")

    # Aquí se definen todas las queries DENTRO de la función
    query_contratos = """
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
    """

    query_items = """
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
    """

    query_servicios = """
    CREATE TABLE IF NOT EXISTS servicios (
        id SERIAL PRIMARY KEY,
        codigo_servicio VARCHAR(100),
        nombre_servicio VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    query_ejecucion_servicio = """
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
    """

    query_ordenes_compra = """
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

    try:
        with engine.connect() as connection:
            connection.execute(text(query_contratos))
            connection.execute(text(query_items))
            connection.execute(text(query_servicios))
            connection.execute(text(query_ejecucion_servicio))
            connection.execute(text(query_ordenes_compra))
        print("✅ Tablas creadas exitosamente")
    except Exception as e:
        print(f"❌ Error al crear las tablas: {e}")

def process_ejecucion_general(engine, excel_file):
    """Procesa la hoja de EJECUCION GENERAL"""
    print("Procesando hoja EJECUCION GENERAL...")
    
    # Leer la hoja de Excel
    df = pd.read_excel(excel_file, sheet_name='EJECUCION GENERAL')
    
    # Limpiar nombres de columnas
    df.columns = [clean_column_name(col) for col in df.columns]
    
    # Mapear nombres de columnas del Excel a nombres de columnas en la BD
    column_mapping = {
        'codigo_de_reactivos_insumos': 'codigo_reactivo',
        'i_d_': 'id_contrato',
        'modalidad': 'modalidad',
        'numero_de_llamado': 'numero_llamado',
        'año_del_llamado': 'anio_llamado',
        'nombre_del_llamado': 'nombre_llamado',
        'empresa_adjudicada': 'empresa_adjudicada',
        'fecha_de_firma_del_contrato': 'fecha_firma_contrato',
        'n_de_contrato_año': 'numero_contrato',
        'vigencia_del_contrato': 'vigencia_contrato',
        'fecha_de_inicio_de_poliza': 'fecha_inicio_poliza',
        'fecha_de_finalizacion_de_poliza': 'fecha_finalizacion_poliza',
        'porcentaje_para_emision_de_complementarios_uso_interno': 'porcentaje_complementarios',
        'comodato_sin_comodato': 'comodato',
        'estado_del_lote_item': 'estado_lote_item',
        'lote': 'lote',
        'item': 'item',
        'descripcion_del_producto': 'descripcion_producto',
        'presentacion': 'presentacion',
        'marca': 'marca',
        'procedencia': 'procedencia',
        'descripcion_del_producto_marca_procedencia': 'descripcion_completa',
        'unidad_de_medida': 'unidad_medida',
        'precio_unitario': 'precio_unitario',
        'cantidad_minima': 'cantidad_minima',
        'cantidad_maxima': 'cantidad_maxima',
        'redistribucion_cantidad_minima': 'redistribucion_minima',
        'redistribucion_cantidad_maxima': 'redistribucion_maxima',
        'entradas_20_adendas_de_ampliacion': 'entradas_adendas',
        'salidas_adendas_de_disminucion': 'salidas_adendas',
        'total_adjudicado': 'total_adjudicado',
        'cantidad_emitida': 'cantidad_emitida',
        'saldo_a_emitir': 'saldo_emitir',
        'porcentaje_emitido': 'porcentaje_emitido'
    }
    
    # Extraer las columnas necesarias para contratos
    contratos_cols = [
        'codigo_reactivo', 'id_contrato', 'modalidad', 'numero_llamado', 'anio_llamado',
        'nombre_llamado', 'empresa_adjudicada', 'fecha_firma_contrato', 'numero_contrato',
        'vigencia_contrato', 'fecha_inicio_poliza', 'fecha_finalizacion_poliza',
        'porcentaje_complementarios', 'comodato'
    ]
    
    # Extraer las columnas necesarias para items
    items_cols = [
        'codigo_reactivo', 'estado_lote_item', 'lote', 'item', 'descripcion_producto',
        'presentacion', 'marca', 'procedencia', 'descripcion_completa', 'unidad_medida',
        'precio_unitario', 'cantidad_minima', 'cantidad_maxima', 'redistribucion_minima',
        'redistribucion_maxima', 'entradas_adendas', 'salidas_adendas', 'total_adjudicado',
        'cantidad_emitida', 'saldo_emitir', 'porcentaje_emitido'
    ]
    
    # Crear DataFrames para cada tabla
    df_contratos = pd.DataFrame()
    df_items = pd.DataFrame()
    
    # Renombrar columnas según el mapeo
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            df[new_col] = df[old_col]
    
    # Extraer datos para contratos (eliminando duplicados)
    for col in contratos_cols:
        if col in df.columns:
            df_contratos[col] = df[col]
    
    # Eliminar filas duplicadas basadas en id_contrato y codigo_reactivo
    if 'id_contrato' in df_contratos.columns and 'codigo_reactivo' in df_contratos.columns:
        df_contratos = df_contratos.drop_duplicates(subset=['id_contrato', 'codigo_reactivo'])
    
    # Extraer datos para items
    for col in items_cols:
        if col in df.columns:
            df_items[col] = df[col]
    
    # Insertar datos en las tablas
    try:
        # Insertar contratos
        if not df_contratos.empty:
            df_contratos.to_sql('contratos', engine, if_exists='append', index=False)
            print(f"Se insertaron {len(df_contratos)} registros en la tabla contratos")
        
        # Obtener IDs de contratos para asignar a los items
        with engine.connect() as connection:
            result = connection.execute("SELECT id, codigo_reactivo, id_contrato FROM contratos")
            contratos_map = {(row[1], row[2]): row[0] for row in result}
        
        # Asignar contrato_id a los items
        df_items['contrato_id'] = df_items.apply(
            lambda row: contratos_map.get((row['codigo_reactivo'], df.loc[row.name, 'id_contrato'] 
                                          if 'id_contrato' in df.columns else None)), 
            axis=1
        )
        
        # Insertar items
        if not df_items.empty:
            df_items.to_sql('items', engine, if_exists='append', index=False)
            print(f"Se insertaron {len(df_items)} registros en la tabla items")
        
    except Exception as e:
        print(f"Error al insertar datos de EJECUCION GENERAL: {e}")

def process_ejecucion_servicio(engine, excel_file):
    """Procesa la hoja de EJECUCION POR SERVICIO"""
    print("Procesando hoja EJECUCION POR SERVICIO...")
    
    # Leer la hoja de Excel
    df = pd.read_excel(excel_file, sheet_name='EJECUCION POR SERVICIO')
    
    # Limpiar nombres de columnas
    df.columns = [clean_column_name(col) for col in df.columns]
    
    # Mapear nombres de columnas
    column_mapping = {
        'codigo_de_reactivos_insumos_codigo_de_servicio_beneficiario': 'codigo_reactivo_servicio',
        'codigo_para_servicio_beneficiario': 'codigo_servicio',
        'codigo_de_reactivos_insumos': 'codigo_reactivo',
        'estado_según_distribucion_interna': 'estado_distribucion',
        'servicio_beneficiario': 'servicio_beneficiario',
        'porcentaje_para_emision_de_complementarios_uso_interno': 'porcentaje_complementarios',
        'comodato_sin_comodato': 'comodato',
        'estado_del_lote_item': 'estado_lote_item',
        'lote': 'lote',
        'item': 'item',
        'descripcion_del_producto_marca_procedencia': 'descripcion_completa',
        'unidad_de_medida': 'unidad_medida',
        'precio_unitario': 'precio_unitario',
        'cantidad_minima': 'cantidad_minima',
        'cantidad_maxima': 'cantidad_maxima',
        'redistribucion_cantidad_minima': 'redistribucion_minima',
        'redistribucion_cantidad_maxima': 'redistribucion_maxima',
        'entradas_20_adendas_de_ampliacion': 'entradas_adendas',
        'salidas_adendas_de_disminucion': 'salidas_adendas',
        'total_adjudicado': 'total_adjudicado',
        'cantidad_emitida': 'cantidad_emitida',
        'saldo_a_emitir': 'saldo_emitir',
        'porcentaje_emitido_por_servicio_sanitario': 'porcentaje_emitido_servicio',
        'porcentaje_del_lote_item_global': 'porcentaje_global',
        'observacion': 'observacion'
    }
    
    # Renombrar columnas según el mapeo
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            df[new_col] = df[old_col]
    
    # Extraer servicios beneficiarios únicos
    df_servicios = pd.DataFrame({
        'codigo_servicio': df['codigo_servicio'].unique(),
        'nombre_servicio': df['servicio_beneficiario'].unique()
    })
    
    # Insertar datos en las tablas
    try:
        # Insertar servicios
        if not df_servicios.empty:
            df_servicios = df_servicios.dropna(subset=['codigo_servicio'])
            df_servicios.to_sql('servicios', engine, if_exists='append', index=False)
            print(f"Se insertaron {len(df_servicios)} registros en la tabla servicios")
        
        # Obtener IDs de servicios
        with engine.connect() as connection:
            result = connection.execute("SELECT id, codigo_servicio FROM servicios")
            servicios_map = {row[1]: row[0] for row in result}
        
        # Obtener IDs de items
        with engine.connect() as connection:
            result = connection.execute("SELECT id, codigo_reactivo, lote, item FROM items")
            items_map = {(row[1], row[2], row[3]): row[0] for row in result}
        
        # Preparar datos para ejecucion_por_servicio
        df_ejecucion = df.copy()
        
        # Asignar servicio_id e item_id
        df_ejecucion['servicio_id'] = df_ejecucion['codigo_servicio'].map(servicios_map)
        df_ejecucion['item_id'] = df_ejecucion.apply(
            lambda row: items_map.get((row['codigo_reactivo'], row['lote'], row['item']), None), 
            axis=1
        )
        
        # Insertar datos en ejecucion_por_servicio
        columns_to_insert = [
            'codigo_reactivo_servicio', 'codigo_servicio', 'codigo_reactivo', 'estado_distribucion',
            'servicio_beneficiario', 'porcentaje_complementarios', 'comodato', 'estado_lote_item',
            'lote', 'item', 'descripcion_completa', 'unidad_medida', 'precio_unitario',
            'cantidad_minima', 'cantidad_maxima', 'redistribucion_minima', 'redistribucion_maxima',
            'entradas_adendas', 'salidas_adendas', 'total_adjudicado', 'cantidad_emitida',
            'saldo_emitir', 'porcentaje_emitido_servicio', 'porcentaje_global', 'observacion',
            'servicio_id', 'item_id'
        ]
        
        # Filtrar solo las columnas que existen
        existing_columns = [col for col in columns_to_insert if col in df_ejecucion.columns]
        df_ejecucion = df_ejecucion[existing_columns]
        
        if not df_ejecucion.empty:
            # Eliminar filas con valores nulos en keys foráneas
            df_ejecucion = df_ejecucion.dropna(subset=['codigo_reactivo', 'lote', 'item'])
            df_ejecucion.to_sql('ejecucion_por_servicio', engine, if_exists='append', index=False)
            print(f"Se insertaron {len(df_ejecucion)} registros en la tabla ejecucion_por_servicio")
        
    except Exception as e:
        print(f"Error al insertar datos de EJECUCION POR SERVICIO: {e}")

def process_orden_compra(engine, excel_file):
    """Procesa la hoja de ORDEN DE COMPRA"""
    print("Procesando hoja ORDEN DE COMPRA...")
    
    # Leer la hoja de Excel
    df = pd.read_excel(excel_file, sheet_name='ORDEN DE COMPRA')
    
    # Limpiar nombres de columnas
    df.columns = [clean_column_name(col) for col in df.columns]
    
    # Mapear nombres de columnas
    column_mapping = {
        'simese_pedido': 'simese_pedido',
        'n_orden_de_compra': 'numero_orden',
        'fecha_de_emision': 'fecha_emision',
        'codigo_de_reactivos_insumos_codigo_de_servicio_beneficiario': 'codigo_reactivo_servicio',
        'codigo_de_reactivos_insumos': 'codigo_reactivo',
        'estado_según_distribucion_interna': 'estado_distribucion',
        'estado_del_lote_item': 'estado_lote_item',
        'servicio_beneficiario': 'servicio_beneficiario',
        'comodato_sin_comodato': 'comodato',
        'lote': 'lote',
        'item': 'item',
        'cantidad_solicitada': 'cantidad_solicitada',
        'cantidad_complementaria_solicitada': 'cantidad_complementaria',
        'unidad_de_medida': 'unidad_medida',
        'descripcion_del_producto_marca_procedencia': 'descripcion_completa',
        'precio_unitario': 'precio_unitario',
        'porcentaje_emitido_servicio_beneficiario': 'porcentaje_emitido_servicio',
        'porcentaje_del_lote_item_global': 'porcentaje_global',
        'saldo_a_emitir_del_servicio_sanitario': 'saldo_emitir_servicio',
        'monto_emitido': 'monto_emitido',
        'porcentaje_para_emision_de_complementarios_uso_interno': 'porcentaje_complementarios',
        'observaciones': 'observaciones'
    }
    
    # Renombrar columnas según el mapeo
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            df[new_col] = df[old_col]
    
    # Insertar datos en las tablas
    try:
        # Obtener IDs de servicios
        with engine.connect() as connection:
            result = connection.execute("SELECT id, codigo_servicio FROM servicios")
            servicios_map = {row[1]: row[0] for row in result}
        
        # Obtener IDs de items
        with engine.connect() as connection:
            result = connection.execute("SELECT id, codigo_reactivo, lote, item FROM items")
            items_map = {(row[1], row[2], row[3]): row[0] for row in result}
        
        # Preparar datos para ordenes_compra
        df_ordenes = df.copy()
        
        # Extraer el código de servicio del código combinado
        if 'codigo_reactivo_servicio' in df_ordenes.columns:
            df_ordenes['codigo_servicio'] = df_ordenes['codigo_reactivo_servicio'].apply(
                lambda x: str(x).split('+')[1] if isinstance(x, str) and '+' in x else None
            )
        
        # Asignar servicio_id e item_id
        if 'codigo_servicio' in df_ordenes.columns:
            df_ordenes['servicio_id'] = df_ordenes['codigo_servicio'].map(servicios_map)
        
        df_ordenes['item_id'] = df_ordenes.apply(
            lambda row: items_map.get((row['codigo_reactivo'], row['lote'], row['item']), None), 
            axis=1
        )
        
        # Insertar datos en ordenes_compra
        columns_to_insert = [
            'simese_pedido', 'numero_orden', 'fecha_emision', 'codigo_reactivo_servicio',
            'codigo_reactivo', 'estado_distribucion', 'estado_lote_item', 'servicio_beneficiario',
            'comodato', 'lote', 'item', 'cantidad_solicitada', 'cantidad_complementaria',
            'unidad_medida', 'descripcion_completa', 'precio_unitario', 'porcentaje_emitido_servicio',
            'porcentaje_global', 'saldo_emitir_servicio', 'monto_emitido', 'porcentaje_complementarios',
            'observaciones', 'servicio_id', 'item_id'
        ]
        
        # Filtrar solo las columnas que existen
        existing_columns = [col for col in columns_to_insert if col in df_ordenes.columns]
        df_ordenes = df_ordenes[existing_columns]
        
        if not df_ordenes.empty:
            # Eliminar filas con valores nulos en keys foráneas
            df_ordenes = df_ordenes.dropna(subset=['codigo_reactivo', 'lote', 'item'])
            df_ordenes.to_sql('ordenes_compra', engine, if_exists='append', index=False)
            print(f"Se insertaron {len(df_ordenes)} registros en la tabla ordenes_compra")
        
    except Exception as e:
        print(f"Error al insertar datos de ORDEN DE COMPRA: {e}")

def create_views(engine):
    """Crea vistas útiles para consultas comunes"""
    print("Creando vistas para facilitar consultas...")
    
    # Vista que combina información de contratos e items
    query_vista_contratos_items = """
    CREATE OR REPLACE VIEW vista_contratos_items AS
    SELECT 
        c.id as contrato_id,
        c.codigo_reactivo,
        c.id_contrato,
        c.modalidad,
        c.numero_llamado,
        c.anio_llamado,
        c.nombre_llamado,
        c.empresa_adjudicada,
        c.fecha_firma_contrato,
        c.numero_contrato,
        c.vigencia_contrato,
        c.fecha_inicio_poliza,
        c.fecha_finalizacion_poliza,
        c.porcentaje_complementarios,
        c.comodato,
        i.id as item_id,
        i.estado_lote_item,
        i.lote,
        i.item,
        i.descripcion_producto,
        i.presentacion,
        i.marca,
        i.procedencia,
        i.descripcion_completa,
        i.unidad_medida,
        i.precio_unitario,
        i.cantidad_minima,
        i.cantidad_maxima,
        i.redistribucion_minima,
        i.redistribucion_maxima,
        i.entradas_adendas,
        i.salidas_adendas,
        i.total_adjudicado,
        i.cantidad_emitida,
        i.saldo_emitir,
        i.porcentaje_emitido
    FROM 
        contratos c
    JOIN 
        items i ON c.id = i.contrato_id;
    """
    
    # Vista que combina información de ejecución por servicio
    query_vista_ejecucion_servicio = """
    CREATE OR REPLACE VIEW vista_ejecucion_servicio AS
    SELECT 
        eps.id as ejecucion_id,
        s.codigo_servicio,
        s.nombre_servicio,
        i.codigo_reactivo,
        i.lote,
        i.item,
        i.descripcion_completa,
        i.precio_unitario,
        eps.cantidad_minima,
        eps.cantidad_maxima,
        eps.cantidad_emitida,
        eps.saldo_emitir,
        eps.porcentaje_emitido_servicio,
        eps.porcentaje_global,
        eps.observacion,
        c.empresa_adjudicada,
        c.numero_contrato
    FROM 
        ejecucion_por_servicio eps
    JOIN 
        servicios s ON eps.servicio_id = s.id
    JOIN 
        items i ON eps.item_id = i.id
    JOIN 
        contratos c ON i.contrato_id = c.id;
    """
    
    # Vista que combina información de órdenes de compra
    query_vista_ordenes_compra = """
    CREATE OR REPLACE VIEW vista_ordenes_compra AS
    SELECT 
        oc.id as orden_id,
        oc.simese_pedido,
        oc.numero_orden,
        oc.fecha_emision,
        s.codigo_servicio,
        s.nombre_servicio,
        i.codigo_reactivo,
        i.lote,
        i.item,
        i.descripcion_completa,
        i.precio_unitario,
        oc.cantidad_solicitada,
        oc.cantidad_complementaria,
        oc.monto_emitido,
        c.empresa_adjudicada,
        c.numero_contrato
    FROM 
        ordenes_compra oc
    JOIN 
        servicios s ON oc.servicio_id = s.id
    JOIN 
        items i ON oc.item_id = i.id
    JOIN 
        contratos c ON i.contrato_id = c.id;
    """
    
    # Ejecutar las consultas
    with engine.connect() as connection:
        connection.execute(query_vista_contratos_items)
        connection.execute(query_vista_ejecucion_servicio)
        connection.execute(query_vista_ordenes_compra)
        
    print("Vistas creadas exitosamente")

def main():
    """Función principal para ejecutar todo el proceso de migración"""

    # 🔄 Ruta del archivo Excel
    excel_file = "C:/Users/Bruno/Desktop/CHACO INTERNACIONAL - DISTRIBUCION.xlsx"

    # ✅ Verificación de la existencia del archivo antes de usarlo
    if os.path.exists(excel_file):
        print(f"✅ Usando archivo Excel: {excel_file}")
    else:
        print(f"❌ Archivo no encontrado: {excel_file}")
        return  # ⛔ Salir de la función si no se encuentra el archivo

    # 🔄 Cargar el Excel en un DataFrame de pandas
    try:
        print("🔄 Cargando datos desde el archivo Excel...")
        df = pd.read_excel(excel_file)
        print("✅ Datos cargados exitosamente:")
        print(df.head())  # Muestra las primeras filas para verificar
    except Exception as e:
        print(f"❌ Error al cargar el archivo Excel: {e}")
        return
    
    # Si el archivo no tiene extensión, intentamos con .xlsx y .xls
    if not os.path.exists(excel_file):
        if os.path.exists(excel_file + ".xlsx"):
            excel_file += ".xlsx"
        elif os.path.exists(excel_file + ".xls"):
            excel_file += ".xls"
        else:
            print(f"Error: No se encuentra el archivo '{excel_file}' con ninguna extensión común de Excel.")
            print("Por favor, verifica la ruta y el nombre del archivo.")
            return
    
    print(f"Usando archivo Excel: {excel_file}")
    
    # Crear string de conexión para SQLAlchemy
try:
    # Intentar crear el engine
    print("🔄 Intentando conectar con la base de datos...")
    conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(conn_string)
    with engine.connect() as connection:
        print("✅ Conexión exitosa a PostgreSQL")
except Exception as e:
    print(f"❌ Error al conectar con PostgreSQL: {e}")
    engine = None
if engine is not None:
    with engine.connect() as connection:
        print("🔄 Creando tablas en la base de datos...")
        try:
            connection.execute(text(query_contratos))
            connection.execute(text(query_items))
            connection.execute(text(query_servicios))
            connection.execute(text(query_ejecucion_servicio))
            connection.execute(text(query_ordenes_compra))
            print("✅ Tablas creadas exitosamente.")
        except Exception as e:
            print(f"❌ Error al crear las tablas: {e}")


    
    # Crear la base de datos si no existe
    create_database(conn_string)
    
    # Conectar a la base de datos
    try:
        # Crear un engine de SQLAlchemy para PostgreSQL
        engine = create_engine(conn_string)
        print("Conexión exitosa a PostgreSQL")
        
        # Crear las tablas
        create_tables(engine)
        
        # Procesar cada hoja del Excel
        process_ejecucion_general(engine, excel_file)
        process_ejecucion_servicio(engine, excel_file)
        process_orden_compra(engine, excel_file)
        
        # Crear vistas
        create_views(engine)
        
        print("\nMigración completada exitosamente.")
        print(f"Puedes conectarte a la base de datos '{DB_NAME}' usando DBeaver con los siguientes parámetros:")
        print(f"Host: {DB_HOST}")
        print(f"Puerto: {DB_PORT}")
        print(f"Usuario: {DB_USER}")
        print(f"Contraseña: ****")
        print(f"Base de datos: {DB_NAME}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()