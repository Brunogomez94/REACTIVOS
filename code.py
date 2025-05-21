import streamlit as st
import pandas as pd
import os
from sqlalchemy import create_engine, text, inspect
from datetime import datetime, timedelta
import psycopg2
import io
import time
import threading

# -------- CONFIG DB --------
DB_USER = "postgres"
DB_PASS = "Dggies12345"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "postgres"

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Registro CSV archivo
REGISTRO_CSV = "registro.csv"
if not os.path.exists(REGISTRO_CSV):
    pd.DataFrame(columns=["Licitacion", "Empresa", "Esquema", "Ubicacion", "Cargado", "UltimaActualizacion"]).to_csv(REGISTRO_CSV, index=False)

# Intervalo de actualización automática (en minutos)
INTERVALO_ACTUALIZACION = 5

# Columnas para cada tabla con los nombres EXACTOS que vienen en el Excel:
COLS_EJECUCION_GENERAL = [
    "CODIGO DE REACTIVOS / INSUMOS", "I.D.", "MODALIDAD", "NUMERO DE LLAMADO", "AÑO DEL LLAMADO",
    "NOMBRE DEL LLAMADO", "EMPRESA ADJUDICADA", "FECHA DE FIRMA DEL CONTRATO",
    "N° de Contrato / Año", "Vigencia del Contrato", "Fecha de Inicio de Poliza",
    "Fecha de Finalizacion de Poliza", "Porcentaje para emision de complementarios (USO INTERNO)",
    "COMODATO // SIN COMODATO", "ESTADO DEL LOTE / ITEM", "LOTE", "ITEM", "DESCRIPCION DEL PRODUCTO",
    "PRESENTACION", "MARCA", "PROCEDENCIA", "DESCRIPCION DEL PRODUCTO // MARCA // PROCEDENCIA",
    "UNIDAD DE MEDIDA", "PRECIO UNITARIO", "CANTIDAD MINIMA", "CANTIDAD MAXIMA",
    "REDISTRIBUCION (CANTIDAD MINIMA)", "REDISTRIBUCION (CANTIDAD MAXIMA)", "ENTRADAS 20% (ADENDAS DE AMPLIACION)",
    "SALIDAS (ADENDAS DE DISMINUCION)", "TOTAL ADJUDICADO", "CANTIDAD EMITIDA", "SALDO A EMITIR", "PORCENTAJE EMITIDO"
]

COLS_EJECUCION_POR_ZONAS = [
    "CODIGO DE REACTIVOS / INSUMOS + CODIGO DE SERVICIO BENEFICIARIO", "CODIGO PARA SERVICIO BENEFICIARIO",
    "CODIGO DE REACTIVOS / INSUMOS", "ESTADO SEGÚN DISTRIBUCION INTERNA", "SERVICIO BENEFICIARIO",
    "Porcentaje para emision de complementarios (USO INTERNO)", "COMODATO // SIN COMODATO", "ESTADO DEL LOTE / ITEM",
    "LOTE", "ITEM", "DESCRIPCION DEL PRODUCTO // MARCA // PROCEDENCIA", "UNIDAD DE MEDIDA", "PRECIO UNITARIO",
    "CANTIDAD MINIMA", "CANTIDAD MAXIMA", "REDISTRIBUCION (CANTIDAD MINIMA)", "REDISTRIBUCION (CANTIDAD MAXIMA)",
    "ENTRADAS 20% (ADENDAS DE AMPLIACION)", "SALIDAS (ADENDAS DE DISMINUCION)", "TOTAL ADJUDICADO", "CANTIDAD EMITIDA",
    "SALDO A EMITIR", "PORCENTAJE EMITIDO POR SERVICIO SANITARIO", "PORCENTAJE DEL LOTE ITEM / GLOBAL", "OBSERVACION"
]

COLS_ORDEN_DE_COMPRA = [
    "SIMESE (PEDIDO)", "N° ORDEN DE COMPRA", "FECHA DE EMISION",
    "CODIGO DE REACTIVOS / INSUMOS + CODIGO DE SERVICIO BENEFICIARIO", "CODIGO DE REACTIVOS / INSUMOS",
    "ESTADO SEGÚN DISTRIBUCION INTERNA", "ESTADO DEL LOTE / ITEM", "SERVICIO BENEFICIARIO", "COMODATO // SIN COMODATO",
    "LOTE", "ITEM", "CANTIDAD SOLICITADA", "CANTIDAD COMPLEMENTARIA SOLICITADA", "UNIDAD DE MEDIDA",
    "DESCRIPCION DEL PRODUCTO // MARCA // PROCEDENCIA", "PRECIO UNITARIO", "PORCENTAJE EMITIDO SERVICIO BENEFICIARIO",
    "PORCENTAJE DEL LOTE ITEM / GLOBAL", "SALDO A EMITIR DEL SERVICIO SANITARIO", "MONTO EMITIDO",
    "Porcentaje para emision de complementarios (USO INTERNO)", "Observaciones"
]

# Nueva tabla para los datos del llamado
COLS_LLAMADO = [
    "I.D.", "MODALIDAD", "NUMERO DE LLAMADO", "AÑO DEL LLAMADO",
    "NOMBRE DEL LLAMADO", "EMPRESA ADJUDICADA", "FECHA DE FIRMA DEL CONTRATO",
    "N° de Contrato / Año", "Vigencia del Contrato", "Fecha de Inicio de Poliza",
    "Fecha de Finalizacion de Poliza"
]

def iniciar_actualizacion_automatica():
    """Inicia una tarea en segundo plano para actualizar cada INTERVALO_ACTUALIZACION minutos"""
    if 'ultima_actualizacion' not in st.session_state:
        st.session_state.ultima_actualizacion = datetime.now()
        # Realizar la primera sincronización al iniciar
        sincronizar_csv_con_postgres()
    
    # Verificar si es hora de actualizar (cada INTERVALO_ACTUALIZACION minutos)
    tiempo_actual = datetime.now()
    tiempo_transcurrido = tiempo_actual - st.session_state.ultima_actualizacion
    
    if tiempo_transcurrido.total_seconds() > (INTERVALO_ACTUALIZACION * 60):
        # Es hora de actualizar
        with st.spinner(f"Actualizando automáticamente información de archivos fuente..."):
            result = sincronizar_csv_con_postgres()
            if "error" not in result:
                st.session_state.ultima_actualizacion = tiempo_actual
                return True
    
    return False

def crear_esquema(esquema):
    with engine.connect() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{esquema}"'))

def limpiar_columnas(df, cols):
    # Dejamos solo columnas que existan y en el orden esperado
    cols_validas = [c for c in cols if c in df.columns]
    return df[cols_validas]

def guardar_tabla(df, esquema, tabla):
    # Guarda tabla en esquema con nombre tabla, reemplazando si existe
    df.to_sql(tabla, con=engine, schema=esquema, if_exists='replace', index=False)

def analizar_excel(file):
    """
    Analiza la estructura de un archivo Excel mostrando sus hojas y columnas.
    
    Args:
        file (UploadedFile): Archivo Excel cargado por el usuario
    """
    try:
        xls = pd.ExcelFile(file)
        st.write("### Estructura del archivo Excel")
        
        # Mostrar lista de hojas
        st.write(f"**Hojas encontradas ({len(xls.sheet_names)}):**")
        for i, sheet_name in enumerate(xls.sheet_names, 1):
            st.write(f"{i}. '{sheet_name}'")
        
        # Permitir al usuario ver una muestra de cada hoja
        sheet_to_preview = st.selectbox(
            "Selecciona una hoja para previsualizar:", 
            options=xls.sheet_names
        )
        
        if sheet_to_preview:
            # Leer una muestra pequeña de la hoja (primeras 5 filas)
            sample_df = xls.parse(sheet_to_preview, nrows=5)
            
            # Mostrar columnas
            st.write(f"**Columnas en '{sheet_to_preview}' ({len(sample_df.columns)}):**")
            st.write(", ".join(sample_df.columns.tolist()))
            
            # Mostrar muestra de datos
            st.write(f"**Muestra de datos de '{sheet_to_preview}':**")
            st.dataframe(sample_df)
            
            # Comparar con columnas esperadas según el tipo de hoja
            expected_columns = None
            if sheet_to_preview.lower() == "ejecucion_general":
                expected_columns = COLS_EJECUCION_GENERAL
            elif sheet_to_preview.lower() == "ejecucion_por_zonas":
                expected_columns = COLS_EJECUCION_POR_ZONAS
            elif sheet_to_preview.lower() == "orden_de_compra":
                expected_columns = COLS_ORDEN_DE_COMPRA
            elif sheet_to_preview.lower() == "llamado":
                expected_columns = COLS_LLAMADO
            
            if expected_columns:
                # Columnas faltantes y extras
                missing_cols = [col for col in expected_columns if col not in sample_df.columns]
                extra_cols = [col for col in sample_df.columns if col not in expected_columns]
                
                if missing_cols:
                    st.warning(f"**Columnas faltantes ({len(missing_cols)}):**")
                    st.write(", ".join(missing_cols))
                
                if extra_cols:
                    st.info(f"**Columnas adicionales ({len(extra_cols)}):**")
                    st.write(", ".join(extra_cols))
                
                if not missing_cols and not extra_cols:
                    st.success("✅ Las columnas de esta hoja coinciden exactamente con el formato esperado.")
        
    except Exception as e:
        st.error(f"Error al analizar el archivo Excel: {str(e)}")
        return False
    
    return True

def cargar_archivo(esquema, file):
    """
    Carga un archivo Excel en la base de datos PostgreSQL, verificando las hojas disponibles.
    
    Args:
        esquema (str): Nombre del esquema en PostgreSQL donde se guardarán las tablas
        file (UploadedFile): Archivo Excel cargado por el usuario
    """
    try:
        # Leer Excel para verificar hojas disponibles
        xls = pd.ExcelFile(file)
        sheet_names = [name.lower() for name in xls.sheet_names]
        
        # Verificar si existen las hojas necesarias (comparación case-insensitive)
        expected_sheets = ["ejecucion_general", "ejecucion_por_zonas", "orden_de_compra", "llamado"]
        missing_sheets = []
        actual_sheet_mapping = {}  # Mapeo de nombres esperados a los reales
        
        for expected in expected_sheets:
            found = False
            for actual in xls.sheet_names:
                if expected == actual.lower():
                    actual_sheet_mapping[expected] = actual
                    found = True
                    break
            if not found:
                missing_sheets.append(expected)
        
        # Si faltan hojas, mostrar un error informativo
        if missing_sheets:
            st.error(f"El archivo Excel no contiene las siguientes hojas requeridas: {', '.join(missing_sheets)}")
            st.info(f"Hojas encontradas en el archivo: {', '.join(xls.sheet_names)}")
            st.info("Por favor, verifica que el archivo tiene las hojas correctas: 'ejecucion_general', 'ejecucion_por_zonas', 'orden_de_compra', 'llamado'")
            
            # Si falta solo la hoja "llamado", ofrecer crearla automáticamente
            if missing_sheets == ["llamado"] and "ejecucion_general" in actual_sheet_mapping:
                # Preguntar si quiere crear la hoja llamado automáticamente
                st.warning("La hoja 'llamado' no existe, pero se puede generar automáticamente a partir de 'ejecucion_general'")
                if st.button("Generar hoja 'llamado' automáticamente"):
                    try:
                        # Extraer datos de ejecucion_general para crear la tabla llamado
                        ejec_general_df = xls.parse(actual_sheet_mapping["ejecucion_general"])
                        
                        # Crear DataFrame para la tabla llamado extrayendo datos únicos relevantes
                        llamado_data = ejec_general_df[COLS_LLAMADO].drop_duplicates().reset_index(drop=True)
                        
                        # Crear esquema y guardar la tabla llamado generada
                        crear_esquema(esquema)
                        guardar_tabla(llamado_data, esquema, "llamado")
                        
                        # Continuar con las otras tablas existentes
                        ejec_general = limpiar_columnas(ejec_general_df, COLS_EJECUCION_GENERAL)
                        ejec_zonas = limpiar_columnas(xls.parse(actual_sheet_mapping["ejecucion_por_zonas"]), COLS_EJECUCION_POR_ZONAS)
                        orden_compra = limpiar_columnas(xls.parse(actual_sheet_mapping["orden_de_compra"]), COLS_ORDEN_DE_COMPRA)
                        
                        # Guardar tablas existentes
                        guardar_tabla(ejec_general, esquema, "ejecucion_general")
                        guardar_tabla(ejec_zonas, esquema, "ejecucion_por_zonas")
                        guardar_tabla(orden_compra, esquema, "orden_de_compra")
                        
                        st.success("✅ Se generó la hoja 'llamado' automáticamente y se cargaron todas las tablas con éxito.")
                        return True
                    except Exception as e:
                        st.error(f"Error al generar la hoja 'llamado' automáticamente: {str(e)}")
                        return False
            
            return False
        
        # Si todas las hojas existen, cargar las tablas
        crear_esquema(esquema)
        
        # Cargar cada tabla filtrando columnas (usando los nombres reales encontrados)
        ejec_general = limpiar_columnas(xls.parse(actual_sheet_mapping["ejecucion_general"]), COLS_EJECUCION_GENERAL)
        ejec_zonas = limpiar_columnas(xls.parse(actual_sheet_mapping["ejecucion_por_zonas"]), COLS_EJECUCION_POR_ZONAS)
        orden_compra = limpiar_columnas(xls.parse(actual_sheet_mapping["orden_de_compra"]), COLS_ORDEN_DE_COMPRA)
        llamado = limpiar_columnas(xls.parse(actual_sheet_mapping["llamado"]), COLS_LLAMADO)
        
        # Guardar cada tabla en la base de datos
        guardar_tabla(ejec_general, esquema, "ejecucion_general")
        guardar_tabla(ejec_zonas, esquema, "ejecucion_por_zonas")
        guardar_tabla(orden_compra, esquema, "orden_de_compra")
        guardar_tabla(llamado, esquema, "llamado")
        
        return True
        
    except Exception as e:
        st.error(f"Error al procesar el archivo Excel: {str(e)}")
        return False

def actualizar_registro(licitacion, empresa, esquema, ubicacion):
    df = pd.read_csv(REGISTRO_CSV)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Evitar duplicados
    df = df[~((df['Esquema'] == esquema))]
    nueva_fila = pd.DataFrame({
        "Licitacion": [licitacion],
        "Empresa": [empresa],
        "Esquema": [esquema],
        "Ubicacion": [ubicacion],
        "Cargado": [now],
        "UltimaActualizacion": [now]
    })
    df = pd.concat([df, nueva_fila], ignore_index=True)
    df.to_csv(REGISTRO_CSV, index=False)

def mostrar_tablas_columnas(esquema):
    try:
        inspector = inspect(engine)
        tablas = inspector.get_table_names(schema=esquema)
        if tablas:
            for tabla in tablas:
                st.write(f"**Tabla:** {tabla}")
                columnas = inspector.get_columns(tabla, schema=esquema)
                if columnas:
                    df_col = pd.DataFrame(columnas)[['name', 'type']]
                    st.table(df_col)
                else:
                    st.write("No hay columnas para esta tabla.")
        else:
            st.info("Este esquema no tiene tablas.")
    except Exception as e:
        st.error(f"Error consultando esquema: {e}")

def obtener_esquemas_postgres():
    """Obtiene todos los esquemas de PostgreSQL excepto los del sistema"""
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN 
                ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 
                'pg_toast_temp_1', 'public')
            """)
            result = conn.execute(query)
            esquemas = [row[0] for row in result]
            return esquemas
    except Exception as e:
        st.error(f"Error consultando esquemas de PostgreSQL: {e}")
        return []

def eliminar_esquema(esquema):
    try:
        with engine.connect() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{esquema}" CASCADE;'))
        
        # Actualizar CSV
        df = pd.read_csv(REGISTRO_CSV)
        df = df[df['Esquema'] != esquema]
        df.to_csv(REGISTRO_CSV, index=False)
        return True
    except Exception as e:
        st.error(f"Error eliminando esquema: {e}")
        return False

def obtener_info_esquema(esquema):
    """Obtiene información del esquema desde el CSV si existe"""
    df = pd.read_csv(REGISTRO_CSV)
    info = df[df['Esquema'] == esquema]
    if not info.empty:
        return info.iloc[0].to_dict()
    else:
        # Si no existe en el CSV, devolvemos información básica
        return {
            "Licitacion": "Desconocido",
            "Empresa": "Desconocido",
            "Esquema": esquema,
            "Ubicacion": "Desconocido",
            "Cargado": "Desconocido"
        }

def sincronizar_csv_con_postgres():
    """Sincroniza el CSV con los esquemas reales en PostgreSQL"""
    try:
        # Obtener esquemas de PostgreSQL
        esquemas_postgres = obtener_esquemas_postgres()
        
        # Leer CSV actual
        df = pd.read_csv(REGISTRO_CSV)
        
        # Añadir columna UltimaActualizacion si no existe
        if "UltimaActualizacion" not in df.columns:
            df["UltimaActualizacion"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
        esquemas_csv = df['Esquema'].tolist() if not df.empty else []
        
        # Esquemas en Postgres pero no en CSV
        esquemas_faltantes = [e for e in esquemas_postgres if e not in esquemas_csv]
        
        # Esquemas en CSV pero no en Postgres
        esquemas_sobrantes = [e for e in esquemas_csv if e not in esquemas_postgres]
        
        # Añadir esquemas faltantes al CSV
        for esquema in esquemas_faltantes:
            nueva_fila = pd.DataFrame({
                "Licitacion": ["Desconocido - Detectado automáticamente"],
                "Empresa": ["Desconocido - Detectado automáticamente"],
                "Esquema": [esquema],
                "Ubicacion": ["Desconocido"],
                "Cargado": ["Detectado: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                "UltimaActualizacion": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
            })
            df = pd.concat([df, nueva_fila], ignore_index=True)
        
        # Actualizar la hora de la última verificación para todos los registros
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df["UltimaActualizacion"] = now
        
        # Marcar esquemas que ya no existen en PostgreSQL
        for idx, row in df.iterrows():
            if row['Esquema'] in esquemas_sobrantes:
                df.at[idx, 'Ubicacion'] = f"[NO EXISTE EN DB] {df.at[idx, 'Ubicacion']}"
        
        # Guardar cambios
        df.to_csv(REGISTRO_CSV, index=False)
        
        return {
            "esquemas_postgres": esquemas_postgres,
            "esquemas_faltantes": esquemas_faltantes,
            "esquemas_sobrantes": esquemas_sobrantes,
            "ultima_actualizacion": now
        }
    except Exception as e:
        st.error(f"Error sincronizando CSV con PostgreSQL: {e}")
        return {"error": str(e)}

def pagina_cargar_archivo():
    st.header("Cargar archivo Excel")
    
    # Iniciar actualización automática
    iniciar_actualizacion_automatica()
    
    licitacion = st.text_input("Licitación")
    empresa = st.text_input("Empresa Adjudicada")

    if licitacion and empresa:
        esquema = f"{licitacion.strip().replace(' ', '_').lower()}_{empresa.strip().replace(' ', '_').lower()}"
        st.text(f"Nombre del esquema: {esquema} (no editable)")

        archivo = st.file_uploader("Selecciona archivo Excel", type=["xls", "xlsx"])

        if archivo is not None:
            # Añadir pestañas para análisis y carga
            tab1, tab2 = st.tabs(["📊 Análisis del archivo", "💾 Cargar a la base de datos"])
            
            with tab1:
                st.subheader("Análisis del archivo Excel")
                st.info("""
                Antes de cargar el archivo, es recomendable verificar su estructura.
                El archivo debe contener las hojas 'ejecucion_general', 'ejecucion_por_zonas', 'orden_de_compra' y 'llamado'.
                """)
                # Inicializar estado de análisis si no existe
                if 'archivo_analizado' not in st.session_state:
                    st.session_state.archivo_analizado = False
                
                # Botón para analizar (solo se muestra si aún no se ha analizado)
                if not st.session_state.archivo_analizado:
                    if st.button("Analizar archivo"):
                        analizar_excel(archivo)
                else:
                    # Si ya se analizó, mostrar resultados directamente
                    analizar_excel(archivo)
                    
                    # Opción para reiniciar análisis
                    if st.button("Reiniciar análisis"):
                        st.session_state.archivo_analizado = False
                        st.session_state.hojas_excel = []
                        st.session_state.hoja_seleccionada = None
                        st.session_state.muestra_datos = None
                        st.session_state.columnas_hoja = []
                        st.rerun()
            
            with tab2:
                st.subheader("Cargar archivo a la base de datos")
                with st.expander("ℹ️ Formato de archivo esperado"):
                    st.info("""
                    El archivo Excel debe contener las siguientes hojas:
                    - ejecucion_general
                    - ejecucion_por_zonas
                    - orden_de_compra
                    - llamado
                    
                    Asegúrate de que los nombres de las hojas están escritos correctamente.
                    
                    Si la hoja 'llamado' no existe, la aplicación puede generarla automáticamente
                    a partir de los datos en 'ejecucion_general'.
                    """)
                
                if st.button("Cargar archivo a PostgreSQL"):
                    with st.spinner("Cargando archivo y creando tablas..."):
                        try:
                            result = cargar_archivo(esquema, archivo)
                            if result:
                                # Solo guardamos registro si la carga fue exitosa
                                ubicacion = archivo.name
                                actualizar_registro(licitacion, empresa, esquema, ubicacion)
                                st.success("✅ Archivo cargado y tablas creadas correctamente.")
                                st.balloons()
                        except Exception as e:
                            st.error(f"Error al cargar archivo: {str(e)}")
                            st.info("Consulta el formato esperado del archivo expandiendo la sección 'Formato de archivo esperado'.")
    else:
        st.info("Por favor, completa Licitación y Empresa Adjudicada para generar el esquema.")

def pagina_ver_cargas():
    st.header("Listado de archivos cargados")
    
    # Iniciar actualización automática
    actualizado = iniciar_actualizacion_automatica()
    
    # Mostrar información sobre la última actualización
    if 'ultima_actualizacion' in st.session_state:
        tiempo_restante = INTERVALO_ACTUALIZACION - ((datetime.now() - st.session_state.ultima_actualizacion).total_seconds() / 60)
        if tiempo_restante < 0:
            tiempo_restante = 0
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.info(f"🕒 Última actualización: {st.session_state.ultima_actualizacion.strftime('%H:%M:%S')} | "
                   f"Próxima en: {int(tiempo_restante)} minutos")
        
        if actualizado:
            st.success("✅ Base de datos sincronizada automáticamente")
    
    # Botón para sincronización manual
    if st.button("🔄 Sincronizar ahora con PostgreSQL"):
        with st.spinner("Sincronizando con PostgreSQL..."):
            result = sincronizar_csv_con_postgres()
            if "error" not in result:
                st.session_state.ultima_actualizacion = datetime.now()
                if result["esquemas_faltantes"] or result["esquemas_sobrantes"]:
                    st.success("Base de datos sincronizada correctamente")
                    if result["esquemas_faltantes"]:
                        st.info(f"Se añadieron {len(result['esquemas_faltantes'])} esquemas detectados en PostgreSQL")
                    if result["esquemas_sobrantes"]:
                        st.warning(f"Se marcaron {len(result['esquemas_sobrantes'])} esquemas que ya no existen en PostgreSQL")
                else:
                    st.success("El registro ya estaba sincronizado con PostgreSQL")
    
    # Leer el CSV después de posible sincronización
    df = pd.read_csv(REGISTRO_CSV)
    if df.empty:
        st.info("No hay archivos cargados aún.")
        return

    # Estilizar el dataframe
    def highlight_missing(val):
        if '[NO EXISTE EN DB]' in str(val):
            return 'background-color: #ffcccc'
        return ''
    
    # Mostrar todas las columnas incluyendo UltimaActualizacion
    st.dataframe(df.style.applymap(highlight_missing, subset=['Ubicacion']))

    esquema_seleccionado = st.selectbox("Selecciona esquema para ver detalles", options=df['Esquema'].unique())

    if esquema_seleccionado:
        mostrar_tablas_columnas(esquema_seleccionado)

def pagina_eliminar_esquemas():
    st.header("Eliminar Esquemas")
    
    # Iniciar actualización automática
    iniciar_actualizacion_automatica()
    
    # Sincronizar con PostgreSQL antes de mostrar
    with st.spinner("Verificando esquemas en PostgreSQL..."):
        sincronizar_csv_con_postgres()
    
    # Obtener esquemas directamente de PostgreSQL
    esquemas_postgres = obtener_esquemas_postgres()
    
    if not esquemas_postgres:
        st.info("No hay esquemas disponibles para eliminar en la base de datos.")
        return
    
    # Inicializar variables de estado si no existen
    if 'confirmar_eliminacion' not in st.session_state:
        st.session_state.confirmar_eliminacion = False
    if 'esquema_a_eliminar' not in st.session_state:
        st.session_state.esquema_a_eliminar = None
    
    # Leer el CSV para información adicional
    df = pd.read_csv(REGISTRO_CSV)
    
    # Crear tabla con estado de esquemas
    datos_esquemas = []
    for esquema in esquemas_postgres:
        info = obtener_info_esquema(esquema)
        datos_esquemas.append({
            "Esquema": esquema,
            "Licitación": info["Licitacion"],
            "Empresa": info["Empresa"],
            "Cargado": info["Cargado"],
            "Estado": "En PostgreSQL y CSV" if esquema in df['Esquema'].values else "Solo en PostgreSQL"
        })
    
    # Mostrar tabla de esquemas
    st.subheader("Esquemas en PostgreSQL")
    
    # Mostrar datos como DataFrame
    df_esquemas = pd.DataFrame(datos_esquemas)
    
    # Función para destacar filas según estado
    def highlight_estado(val):
        if val == "Solo en PostgreSQL":
            return 'background-color: #ffffcc'
        return ''
    
    st.dataframe(df_esquemas.style.applymap(highlight_estado, subset=['Estado']))
    
    # Selector de esquema a eliminar
    esquema_seleccionado = st.selectbox(
        "Selecciona el esquema que deseas eliminar:",
        options=esquemas_postgres
    )
    
    # Mostrar información del esquema seleccionado
    if esquema_seleccionado:
        st.subheader("Detalles del esquema seleccionado")
        info_esquema = obtener_info_esquema(esquema_seleccionado)
        
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**Licitación:** {info_esquema['Licitacion']}")
            st.write(f"**Empresa:** {info_esquema['Empresa']}")
        with col2:
            st.write(f"**Archivo:** {info_esquema['Ubicacion']}")
            st.write(f"**Cargado:** {info_esquema['Cargado']}")
        
        # Mostrar estructura del esquema
        with st.expander("Ver estructura del esquema en la base de datos"):
            mostrar_tablas_columnas(esquema_seleccionado)
        
        # Primer botón de eliminación
        if not st.session_state.confirmar_eliminacion:
            if st.button("Eliminar este esquema"):
                st.session_state.confirmar_eliminacion = True
                st.session_state.esquema_a_eliminar = esquema_seleccionado
                st.rerun()  # Cambiado de experimental_rerun() a rerun()
        
        # Confirmación doble de eliminación
        if st.session_state.confirmar_eliminacion and st.session_state.esquema_a_eliminar == esquema_seleccionado:
            st.warning(f"⚠️ ¿Estás seguro que deseas eliminar el esquema '{esquema_seleccionado}'?")
            st.warning("⚠️ Esta acción eliminará permanentemente todas las tablas y datos asociados.")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("❌ Cancelar", key="btn_cancelar"):
                    st.session_state.confirmar_eliminacion = False
                    st.session_state.esquema_a_eliminar = None
                    st.rerun()  # Cambiado de experimental_rerun() a rerun()
            
            with col2:
                if st.button("✅ Confirmar eliminación", key="btn_confirmar"):
                    if eliminar_esquema(esquema_seleccionado):
                        st.success(f"✅ El esquema '{esquema_seleccionado}' ha sido eliminado correctamente")
                        st.session_state.confirmar_eliminacion = False
                        st.session_state.esquema_a_eliminar = None
                        # Esperar un momento para que el usuario vea el mensaje
                        st.balloons()
                        # Recargar la página para actualizar la lista
                        st.rerun()  # Cambiado de experimental_rerun() a rerun()

def main():
    st.set_page_config(
        page_title="Gestión de Esquemas PostgreSQL",
        page_icon="📊",
        layout="wide"
    )
    
    # Verificar conexión con PostgreSQL al iniciar
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        st.sidebar.success("✅ Conectado a PostgreSQL")
        
        # Mostrar estado de actualización automática
        if 'ultima_actualizacion' in st.session_state:
            tiempo_restante = INTERVALO_ACTUALIZACION - ((datetime.now() - st.session_state.ultima_actualizacion).total_seconds() / 60)
            if tiempo_restante < 0:
                tiempo_restante = 0
            st.sidebar.info(f"🔄 Próxima actualización auto: {int(tiempo_restante)} min")
    except Exception as e:
        st.sidebar.error(f"❌ Error de conexión: {e}")
        st.error("No se pudo conectar a la base de datos PostgreSQL. Verifique la configuración y que el servidor esté en funcionamiento.")
        return
    
    # Título principal
    st.title("Gestión de Archivos Excel a Base de Datos")
    
    # Crear un menú de navegación más atractivo
    menu = st.sidebar.radio(
        "Menú de Navegación", 
        ["Cargar archivo", "Ver cargas", "Eliminar esquemas"],
        format_func=lambda x: {
            "Cargar archivo": "📥 Cargar archivo", 
            "Ver cargas": "📋 Ver cargas",
            "Eliminar esquemas": "🗑️ Eliminar esquemas"
        }[x]
    )
    
    # Mostrar la página seleccionada
    if menu == "Cargar archivo":
        pagina_cargar_archivo()
    elif menu == "Ver cargas":
        pagina_ver_cargas()
    elif menu == "Eliminar esquemas":
        pagina_eliminar_esquemas()

if __name__ == "__main__":
    main()