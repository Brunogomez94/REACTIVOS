import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import os
import io
import json
import hashlib
import openpyxl
from sqlalchemy import create_engine, text

# Configuración de conexión a PostgreSQL
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "Dggies12345"

# Intervalo de actualización automática (en minutos)
INTERVALO_ACTUALIZACION = 10

# Crear conexión a PostgreSQL
try:
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
except Exception as e:
    print(f"Error al crear conexión a PostgreSQL: {e}")

# Función para configurar la tabla de usuarios
def formatear_columnas_tabla(df, mapeo_columnas=None):
    """Formatea las columnas de un DataFrame para mostrar al usuario final"""
    mapeo_default = {
        'id': 'ID',
        'ruc': 'RUC',
        'razon_social': 'Razón Social',
        'direccion': 'Dirección',
        'correo_electronico': 'Correo Electrónico',
        'telefono': 'Teléfono',
        'contacto_nombre': 'Contacto',
        'activo': 'Estado',
        'fecha_registro': 'Fecha Registro',
        'fecha_actualizacion': 'Fecha Actualización',
        'fecha_creacion': 'Fecha Creación',
        'fecha_carga': 'Fecha Carga',
        'nombre_archivo': 'Nombre Archivo',
        'esquema': 'Esquema',
        'usuario': 'Usuario',
        'estado': 'Estado',
        'usuario_id': 'Usuario ID',
        'cedula': 'Cédula',
        'username': 'Usuario',
        'nombre_completo': 'Nombre Completo',
        'role': 'Rol',
        'ultimo_cambio_password': 'Último Cambio Password',
        'numero_orden': 'Número Orden',
        'fecha_emision': 'Fecha Emisión',
        'servicio_beneficiario': 'Servicio Beneficiario',
        'simese': 'SIMESE',
        'cantidad_items': 'Cantidad Items',
        'monto_total': 'Monto Total',
        'lote': 'Lote',
        'item': 'Item',
        'codigo_insumo': 'Código Insumo',
        'codigo_servicio': 'Código Servicio',
        'descripcion': 'Descripción',
        'cantidad': 'Cantidad',
        'unidad_medida': 'Unidad Medida',
        'precio_unitario': 'Precio Unitario',
        'observaciones': 'Observaciones'
    }
    
    if mapeo_columnas:
        mapeo_default.update(mapeo_columnas)
    
    df_formateado = df.copy()
    nuevos_nombres = {}
    for col in df_formateado.columns:
        col_lower = col.lower()
        if col_lower in mapeo_default:
            nuevos_nombres[col] = mapeo_default[col_lower]
        else:
            nuevos_nombres[col] = col.replace('_', ' ').title()
    
    df_formateado = df_formateado.rename(columns=nuevos_nombres)
    return df_formateado

def configurar_tabla_usuarios():
    """Crea la tabla de usuarios si no existe"""
    try:
        print("Intentando configurar tabla de usuarios...")
        with engine.connect() as conn:
            print("Conexión establecida, creando tabla...")
            
            # Crear esquema si no existe
            conn.execute(text("""
                CREATE SCHEMA IF NOT EXISTS reactivos_py;
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS reactivos_py.usuarios (
                    id SERIAL PRIMARY KEY,
                    cedula VARCHAR(20) UNIQUE NOT NULL,  -- Agregar la columna cédula
                    username VARCHAR(50) UNIQUE NOT NULL,
                    password VARCHAR(200) NOT NULL,
                    nombre_completo VARCHAR(100) NOT NULL,
                    role VARCHAR(20) NOT NULL DEFAULT 'user',
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    ultimo_cambio_password TIMESTAMP
                );
            """))
            
            # Verificar si existe el usuario admin
            query = text("SELECT COUNT(*) FROM reactivos_py.usuarios WHERE username = 'admin'")
            result = conn.execute(query)
            count = result.scalar()
            
            if count == 0:
                # Crear usuario admin por defecto
                password_hash = hashlib.sha256("admin".encode()).hexdigest()
                
                query = text("""
                    INSERT INTO reactivos_py.usuarios (cedula, username, password, nombre_completo, role, ultimo_cambio_password)
                    VALUES ('0000000', 'admin', :password, 'Administrador del Sistema', 'admin', CURRENT_TIMESTAMP)
                """)
                
                conn.execute(query, {'password': password_hash})
                print("Usuario admin creado")
                
            return True
    except Exception as e:
        print(f"Error configurando tabla de usuarios: {e}")
        return False

# Función para configurar la tabla de archivos cargados
def configurar_tabla_cargas():
    """Crea la tabla para registrar las cargas de archivos CSV"""
    try:
        with engine.connect() as conn:
            # Crear esquema si no existe
            conn.execute(text("""
                CREATE SCHEMA IF NOT EXISTS reactivos_py;
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS reactivos_py.archivos_cargados (
                    id SERIAL PRIMARY KEY,
                    nombre_archivo VARCHAR(255) NOT NULL,
                    esquema VARCHAR(100) NOT NULL,
                    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    usuario_id INTEGER NOT NULL,
                    contenido_original TEXT,
                    ubicacion_fisica VARCHAR(500),
                    estado VARCHAR(50) DEFAULT 'Activo',
                    FOREIGN KEY (usuario_id) REFERENCES reactivos_py.usuarios(id)
                );
            """))
            return True
    except Exception as e:
        print(f"Error configurando tabla de archivos: {e}")
        return False

# Crear tabla para almacenar órdenes de compra
def configurar_tabla_ordenes_compra():
    """Crea la tabla de órdenes de compra si no existe"""
    try:
        with engine.connect() as conn:
            # Crear esquema si no existe
            conn.execute(text("""
                CREATE SCHEMA IF NOT EXISTS reactivos_py;
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS reactivos_py.ordenes_compra (
                    id SERIAL PRIMARY KEY,
                    numero_orden VARCHAR(50) UNIQUE NOT NULL,
                    fecha_emision TIMESTAMP NOT NULL,
                    esquema VARCHAR(100) NOT NULL,
                    servicio_beneficiario VARCHAR(200),
                    simese VARCHAR(50),
                    usuario_id INTEGER NOT NULL,
                    estado VARCHAR(50) NOT NULL DEFAULT 'Emitida',
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (usuario_id) REFERENCES reactivos_py.usuarios(id)
                );
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS reactivos_py.items_orden_compra (
                    id SERIAL PRIMARY KEY,
                    orden_compra_id INTEGER NOT NULL,
                    lote VARCHAR(50),
                    item VARCHAR(50),
                    codigo_insumo VARCHAR(100),
                    codigo_servicio VARCHAR(100),
                    descripcion TEXT,
                    cantidad NUMERIC(15, 2) NOT NULL,
                    unidad_medida VARCHAR(50),
                    precio_unitario NUMERIC(15, 2),
                    monto_total NUMERIC(15, 2),
                    observaciones TEXT,
                    FOREIGN KEY (orden_compra_id) REFERENCES reactivos_py.ordenes_compra(id) ON DELETE CASCADE
                );
            """))
            
            return True
    except Exception as e:
        print(f"Error configurando tablas de órdenes de compra: {e}")
        return False

def configurar_tabla_proveedores():
    """Crea la tabla de proveedores si no existe"""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE SCHEMA IF NOT EXISTS reactivos_py;
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS reactivos_py.proveedores (
                    id SERIAL PRIMARY KEY,
                    ruc VARCHAR(50) UNIQUE NOT NULL,
                    razon_social VARCHAR(200) NOT NULL,
                    direccion TEXT,
                    correo_electronico VARCHAR(100),
                    telefono VARCHAR(50),
                    contacto_nombre VARCHAR(100),
                    observaciones TEXT,
                    activo BOOLEAN DEFAULT TRUE,
                    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            
            return True
    except Exception as e:
        print(f"Error configurando tabla de proveedores: {e}")
        return False

def configurar_tabla_auditoria():
   """Crea la tabla de auditoría para registrar todas las actividades del sistema"""
   try:
       with engine.connect() as conn:
           conn.execute(text("""
               CREATE SCHEMA IF NOT EXISTS reactivos_py;
           """))
           
           conn.execute(text("""
               CREATE TABLE IF NOT EXISTS reactivos_py.auditoria (
                   id SERIAL PRIMARY KEY,
                   usuario_id INTEGER NOT NULL,
                   usuario_nombre VARCHAR(100) NOT NULL,
                   accion VARCHAR(100) NOT NULL,
                   modulo VARCHAR(50) NOT NULL,
                   descripcion TEXT NOT NULL,
                   detalles JSONB,
                   ip_address VARCHAR(45),
                   user_agent TEXT,
                   fecha_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                   esquema_afectado VARCHAR(100),
                   registro_afectado_id INTEGER,
                   valores_anteriores JSONB,
                   valores_nuevos JSONB,
                   FOREIGN KEY (usuario_id) REFERENCES reactivos_py.usuarios(id)
               );
           """))
           
           conn.execute(text("""
               CREATE INDEX IF NOT EXISTS idx_auditoria_usuario ON reactivos_py.auditoria(usuario_id);
               CREATE INDEX IF NOT EXISTS idx_auditoria_fecha ON reactivos_py.auditoria(fecha_hora DESC);
               CREATE INDEX IF NOT EXISTS idx_auditoria_modulo ON reactivos_py.auditoria(modulo);
               CREATE INDEX IF NOT EXISTS idx_auditoria_accion ON reactivos_py.auditoria(accion);
           """))
           
           return True
   except Exception as e:
       print(f"Error configurando tabla de auditoría: {e}")
       return False

def registrar_actividad(accion, modulo, descripcion, detalles=None, esquema_afectado=None, 
                      registro_afectado_id=None, valores_anteriores=None, valores_nuevos=None):
   """Registra una actividad en el sistema de auditoría"""
   try:
       if 'user_id' not in st.session_state or 'user_name' not in st.session_state:
           return False
           
       with engine.connect() as conn:
           query = text("""
               INSERT INTO reactivos_py.auditoria 
               (usuario_id, usuario_nombre, accion, modulo, descripcion, detalles, 
                esquema_afectado, registro_afectado_id, valores_anteriores, valores_nuevos)
               VALUES (:usuario_id, :usuario_nombre, :accion, :modulo, :descripcion, :detalles,
                       :esquema_afectado, :registro_afectado_id, :valores_anteriores, :valores_nuevos)
           """)
           
           conn.execute(query, {
               'usuario_id': st.session_state.user_id,
               'usuario_nombre': st.session_state.user_name,
               'accion': accion,
               'modulo': modulo,
               'descripcion': descripcion,
               'detalles': json.dumps(detalles) if detalles else None,
               'esquema_afectado': esquema_afectado,
               'registro_afectado_id': registro_afectado_id,
               'valores_anteriores': json.dumps(valores_anteriores) if valores_anteriores else None,
               'valores_nuevos': json.dumps(valores_nuevos) if valores_nuevos else None
           })
           
           return True
   except Exception as e:
       print(f"Error registrando actividad en auditoría: {e}")
       return False

def obtener_historial_actividades(limite=100, usuario_id=None, modulo=None, accion=None, fecha_desde=None, fecha_hasta=None):
    """Obtiene el historial de actividades con filtros opcionales"""
    try:
        with engine.connect() as conn:
            query_base = """
                SELECT a.id, a.usuario_nombre, a.accion, a.modulo, a.descripcion, 
                       a.fecha_hora, a.esquema_afectado, a.detalles,
                       a.valores_anteriores, a.valores_nuevos
                FROM reactivos_py.auditoria a
                WHERE 1=1
            """
            params = {}
            
            if usuario_id:
                query_base += " AND a.usuario_id = :usuario_id"
                params['usuario_id'] = usuario_id
            
            if modulo:
                query_base += " AND a.modulo = :modulo"
                params['modulo'] = modulo
            
            if accion:
                query_base += " AND a.accion = :accion"
                params['accion'] = accion
            
            if fecha_desde:
                query_base += " AND a.fecha_hora >= :fecha_desde"
                params['fecha_desde'] = fecha_desde
            
            if fecha_hasta:
                query_base += " AND a.fecha_hora <= :fecha_hasta"
                params['fecha_hasta'] = fecha_hasta
            
            query_base += " ORDER BY a.fecha_hora DESC LIMIT :limite"
            params['limite'] = limite
            
            query = text(query_base)
            result = conn.execute(query, params)
            
            actividades = []
            for row in result:
                actividades.append({
                    'id': row[0],
                    'usuario': row[1],
                    'accion': row[2],
                    'modulo': row[3],
                    'descripcion': row[4],
                    'fecha_hora': row[5],
                    'esquema_afectado': row[6],
                    'detalles': json.loads(row[7]) if row[7] else None,
                    'valores_anteriores': json.loads(row[8]) if row[8] else None,
                    'valores_nuevos': json.loads(row[9]) if row[9] else None
                })
            
            return actividades
    except Exception as e:
        st.error(f"Error obteniendo historial de actividades: {e}")
        return []

def numero_a_letras(numero):
    """Convierte un número a su representación en letras (simplificada)"""
    millones = int(numero / 1000000)
    miles = int((numero % 1000000) / 1000)
    unidades = int(numero % 1000)
    
    texto = ""
    
    if millones > 0:
        if millones == 1:
            texto += "UN MILLÓN "
        else:
            texto += f"{millones} MILLONES "
    
    if miles > 0:
        texto += f"{miles} MIL "
    
    if unidades > 0 or (millones == 0 and miles == 0):
        texto += f"{unidades} "
    
    return texto.strip()

def iniciar_actualizacion_automatica():
    """Configura la actualización automática de datos"""
    # Verificar si ya se ha iniciado la actualización
    if 'ultima_actualizacion' not in st.session_state:
        st.session_state.ultima_actualizacion = datetime.now()
    else:
        # Verificar si es hora de actualizar
        tiempo_transcurrido = (datetime.now() - st.session_state.ultima_actualizacion).total_seconds() / 60
        if tiempo_transcurrido >= INTERVALO_ACTUALIZACION:
            st.session_state.ultima_actualizacion = datetime.now()
            st.info("Actualizando datos...")
            # Aquí puedes poner el código para refrescar los datos
            time.sleep(1)
            st.rerun()

def obtener_esquemas_postgres():
    """Obtiene la lista de esquemas existentes en PostgreSQL, excluyendo esquemas del sistema"""
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'public', 'reactivos_py', 'pg_toast')
                AND schema_name NOT LIKE 'pg_temp_%'
                AND schema_name NOT LIKE 'pg_toast_temp_%'
                ORDER BY schema_name
            """)
            
            result = conn.execute(query)
            
            esquemas = [row[0] for row in result]
            return esquemas
    except Exception as e:
        st.error(f"Error obteniendo esquemas: {e}")
        return []

def cargar_archivo_a_postgres(archivo, nombre_archivo, esquema, empresa_prefijo=None, datos_formulario=None):
    """Carga un archivo Excel o CSV directamente a PostgreSQL"""
    try:
        # Primero, crear el esquema explícitamente
        with engine.connect() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{esquema}"'))
            conn.commit()
        
        # Resto del procesamiento del archivo...
        # [código existente para procesar Excel/CSV]
        
        # Al crear la tabla LLAMADO, combinar datos del formulario con datos del Excel
        if datos_formulario and empresa_prefijo:
            tabla_llamado = f"{empresa_prefijo}_llamado"
            
            # Crear DataFrame con datos del formulario
            llamado_df = pd.DataFrame({
                'NOMBRE_LICITACION': [datos_formulario['nombre_licitacion']],
                'I_D': [datos_formulario['id_licitacion']],
                'NOMBRE_DEL_LLAMADO': [datos_formulario['nombre_llamado']],
                'EMPRESA_ADJUDICADA': [datos_formulario['empresa_adjudicada']],
                'RUC': [datos_formulario['ruc']],
                'FECHA_FIRMA_CONTRATO': [datos_formulario['fecha_firma']],
                'NUMERO_CONTRATO': [datos_formulario['numero_contrato']],
                'VIGENCIA_CONTRATO': [datos_formulario['vigencia_contrato']]
            })
            
            # Guardar tabla llamado combinada
            llamado_df.to_sql(tabla_llamado, con=engine, schema=esquema, if_exists='replace', index=False)
        
        # Resto del código...
        
        return True, f"Archivo cargado correctamente con ID: {archivo_id}"
    except Exception as e:
        return False, f"Error al cargar archivo: {e}"

def obtener_archivos_cargados():
    """Obtiene la lista de archivos cargados con su estado actual"""
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT ac.id, ac.nombre_archivo, ac.esquema, ac.fecha_carga, 
                       u.username as usuario, ac.estado
                FROM archivos_cargados ac
                JOIN usuarios u ON ac.usuario_id = u.id
                ORDER BY ac.fecha_carga DESC
            """)
            
            result = conn.execute(query)
            
            archivos = []
            for row in result:
                archivos.append({
                    'id': row[0],
                    'nombre_archivo': row[1],
                    'esquema': row[2],
                    'fecha_carga': row[3],
                    'usuario': row[4],
                    'estado': row[5]
                })
            
            return archivos
    except Exception as e:
        st.error(f"Error obteniendo archivos cargados: {e}")
        return []

def eliminar_esquema_postgres(esquema):
    """Elimina un esquema de PostgreSQL y actualiza la tabla de cargas"""
    try:
        with engine.connect() as conn:
            # Iniciar transacción
            trans = conn.begin()
            try:
                # Actualizar estado en la tabla de archivos cargados
                query_update = text("""
                    UPDATE archivos_cargados
                    SET estado = 'Eliminado'
                    WHERE esquema = :esquema
                """)
                
                conn.execute(query_update, {'esquema': esquema})
                
                # Eliminar el esquema
                query = text(f'DROP SCHEMA IF EXISTS "{esquema}" CASCADE')
                conn.execute(query)
                
                # Confirmar transacción
                trans.commit()
                
                return True, f"Esquema '{esquema}' eliminado correctamente."
            except Exception as e:
                # Revertir transacción en caso de error
                trans.rollback()
                raise e
    except Exception as e:
        return False, f"Error al eliminar esquema: {e}"

def pagina_login():
    """Página de inicio de sesión"""
    st.title("Sistema de Gestión de Licitaciones")
    st.subheader("Iniciar sesión")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        cedula = st.text_input("Cédula de Identidad:")  # Cambiar a cédula
        password = st.text_input("Contraseña:", type="password")
        
        if st.button("Ingresar"):
            if not cedula or not password:  # Cambiar validación
                st.error("Por favor, complete todos los campos.")
            else:
                # Verificar credenciales
                password_hash = hashlib.sha256(password.encode()).hexdigest()
                
                try:
                    with engine.connect() as conn:
                        # Modificar la consulta para usar cédula
                        query = text("""
                            SELECT id, username, role, nombre_completo, ultimo_cambio_password 
                            FROM reactivos_py.usuarios 
                            WHERE cedula = :cedula AND password = :password
                        """)
                        
                        result = conn.execute(query, {
                            'cedula': cedula,  # Usar cédula
                            'password': password_hash
                        })
                        
                        user = result.fetchone()
                        
                        if user:
                            # Autenticación exitosa
                            st.session_state.logged_in = True
                            st.session_state.user_id = user[0]
                            st.session_state.username = user[1]
                            st.session_state.user_role = user[2]
                            st.session_state.user_name = user[3]
                            
                            # Verificar si se requiere cambio de contraseña
                            if user[4] is None:  # ultimo_cambio_password es NULL
                                st.session_state.requiere_cambio_password = True
                                st.warning("Se requiere cambiar su contraseña. Será redirigido para hacerlo.")
                                time.sleep(2)
                            else:
                                st.session_state.requiere_cambio_password = False
                            
                            st.success("Inicio de sesión exitoso!")
                            registrar_actividad(
                                accion="LOGIN",
                                modulo="USUARIOS", 
                                descripcion=f"Usuario {cedula} inició sesión exitosamente")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("Cédula o contraseña incorrectos.")  # Cambiar mensaje
                except Exception as e:
                    st.error(f"Error al verificar credenciales: {e}")    
    with col2:
        st.image("https://via.placeholder.com/300x200?text=Logo+Sistema", width=300)

def main():
    # (Código existente hasta la verificación de login)
    
    # Verificar si el usuario está autenticado
    if not st.session_state.logged_in:
        pagina_login()
        return
    
    # AGREGAR ESTE BLOQUE - Verificar si el usuario requiere cambio de contraseña
    if 'requiere_cambio_password' in st.session_state and st.session_state.requiere_cambio_password:
        pagina_cambiar_password()
        return

def obtener_proveedores():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT razon_social, ruc 
                FROM reactivos_py.proveedores 
                WHERE activo = TRUE 
                ORDER BY razon_social
            """)
            result = conn.execute(query)
            return [{'nombre': row[0], 'ruc': row[1]} for row in result]
    except:
        return []
        
        # Campos en el orden solicitado
        col1, col2 = st.columns(2)
        
        with col1:
            id_licitacion = st.text_input("I.D.:")
            modalidad = st.text_input("Modalidad:", placeholder="Ej: CD, LP, LC, CO, LPN, CVE, LPI, LCO, MCN...")
            numero_anio = st.text_input("N° / Año de Modalidad:")
        
        with col2:
            nombre_llamado = st.text_input("Nombre del llamado:")
            
            # Autocompletado de proveedores
            proveedores_existentes = obtener_proveedores()
            
            if proveedores_existentes:
                empresa_options = [f"{p['nombre']} - {p['ruc']}" for p in proveedores_existentes]
                
                empresa_seleccionada = st.selectbox(
                    "Empresa Adjudicada:",
                    options=["Seleccionar..."] + empresa_options + ["+ Nuevo Proveedor"]
                )
                
                if empresa_seleccionada == "+ Nuevo Proveedor":
                    st.info("💡 Vaya a 'Gestión de Proveedores' para registrar una nueva empresa")
                    empresa_adjudicada = st.text_input("Nombre de la empresa:", disabled=True)
                    ruc = st.text_input("RUC:", disabled=True)
                elif empresa_seleccionada != "Seleccionar...":
                    # Extraer datos del proveedor seleccionado
                    empresa_adjudicada = empresa_seleccionada.split(" - ")[0]
                    ruc_autocompletado = empresa_seleccionada.split(" - ")[-1]
                    ruc = st.text_input("RUC:", value=ruc_autocompletado, disabled=True)
                else:
                    empresa_adjudicada = ""
                    ruc = st.text_input("RUC:", disabled=True)
            else:
                st.warning("⚠️ No hay proveedores registrados. Registre primero en 'Gestión de Proveedores'")
                empresa_adjudicada = st.text_input("Empresa Adjudicada:", disabled=True)
                ruc = st.text_input("RUC:", disabled=True)
        
        # Segunda fila de campos
        col3, col4 = st.columns(2)
        
        with col3:
            vigencia_contrato = st.text_input("Vigencia del Contrato:")
        
        with col4:
            fecha_firma = st.date_input("Fecha de la firma del contrato:")
        
        # Otros campos adicionales
        col5, col6 = st.columns(2)
        
        with col5:
            numero_contrato = st.text_input("Número de contrato/año:")
        
        with col6:
            # Generar sugerencia de nombre de esquema
            if modalidad and numero_anio:
                esquema_sugerido = f"{modalidad.strip().lower()}-{numero_anio.strip()}"
                esquema_personalizado = st.text_input("Nombre del esquema:", value=esquema_sugerido)
            else:
                esquema_personalizado = st.text_input("Nombre del esquema:")
        
        st.divider()
        
        # Campo para subir archivo
        archivo = st.file_uploader("Seleccionar archivo:", type=["csv", "xlsx", "xls"])
        
        # SECCIÓN DE ANÁLISIS DE DATOS
        if archivo is not None:
            st.subheader("📊 Análisis del Archivo")
            
            # Checkbox para activar análisis
            mostrar_analisis = st.checkbox("🔍 Mostrar análisis detallado del archivo")
            
            if mostrar_analisis:
                try:
                    # Determinar el tipo de archivo
                    extension = archivo.name.split('.')[-1].lower()
                    
                    if extension in ['xlsx', 'xls']:
                        # Análisis para archivos Excel
                        xls = pd.ExcelFile(archivo)
                        
                        st.write(f"**📁 Archivo:** {archivo.name}")
                        st.write(f"**📄 Tipo:** Excel ({extension.upper()})")
                        st.write(f"**📋 Hojas encontradas:** {len(xls.sheet_names)}")
                        
                        # Mostrar hojas disponibles
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write("**Hojas en el archivo:**")
                            for i, sheet in enumerate(xls.sheet_names, 1):
                                st.write(f"{i}. {sheet}")
                        
                        with col2:
                            # Verificar hojas requeridas
                            required_sheets = ["ejecucion_general", "ejecucion_por_zonas", "orden_de_compra", "llamado"]
                            missing_sheets = []
                            found_sheets = []
                            
                            for req_sheet in required_sheets:
                                found = any(req_sheet.lower() == sheet.lower() for sheet in xls.sheet_names)
                                if found:
                                    found_sheets.append(req_sheet)
                                else:
                                    missing_sheets.append(req_sheet)
                            
                            st.write("**Estado de hojas requeridas:**")
                            for sheet in found_sheets:
                                st.write(f"✅ {sheet}")
                            for sheet in missing_sheets:
                                st.write(f"❌ {sheet}")
                        
                        # Análisis detallado de cada hoja
                        hoja_analisis = st.selectbox(
                            "Seleccionar hoja para análisis detallado:",
                            options=xls.sheet_names
                        )
                        
                        if hoja_analisis:
                            # Leer una muestra de la hoja seleccionada
                            df_sample = pd.read_excel(xls, sheet_name=hoja_analisis, nrows=10)
                            
                            st.write(f"**📊 Análisis de la hoja '{hoja_analisis}':**")
                            
                            # Información básica
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Total Columnas", len(df_sample.columns))
                            with col2:
                                # Leer toda la hoja para contar filas (puede ser lento para archivos grandes)
                                try:
                                    df_full = pd.read_excel(xls, sheet_name=hoja_analisis)
                                    st.metric("Total Filas", len(df_full))
                                except:
                                    st.metric("Total Filas", "Error al contar")
                            with col3:
                                # Contar columnas vacías
                                empty_cols = df_sample.isnull().all().sum()
                                st.metric("Columnas Vacías", empty_cols)
                            
                            # Mostrar nombres de columnas
                            st.write("**Columnas encontradas:**")
                            cols_text = ", ".join(df_sample.columns.tolist())
                            st.text_area("Lista de columnas:", value=cols_text, height=100, disabled=True)
                            
                            # Mostrar muestra de datos
                            st.write("**Muestra de datos (primeras 10 filas):**")
                            st.dataframe(df_sample)
                            
                            # Verificar tipos de datos
                            st.write("**Tipos de datos por columna:**")
                            tipos_df = pd.DataFrame({
                                'Columna': df_sample.columns,
                                'Tipo': df_sample.dtypes.values,
                                'Valores Nulos': df_sample.isnull().sum().values,
                                'Valores Únicos': df_sample.nunique().values
                            })
                            st.dataframe(tipos_df)
                    
                    elif extension == 'csv':
                        # Análisis para archivos CSV (código similar al anterior)
                        contenido = archivo.getvalue().decode('utf-8')
                        df_sample = pd.read_csv(io.StringIO(contenido), nrows=10)
                        
                        st.write(f"**📁 Archivo:** {archivo.name}")
                        st.write(f"**📄 Tipo:** CSV")
                        
                        # Información básica
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Columnas", len(df_sample.columns))
                        with col2:
                            try:
                                df_full = pd.read_csv(io.StringIO(contenido))
                                st.metric("Total Filas", len(df_full))
                            except:
                                st.metric("Total Filas", "Error al contar")
                        with col3:
                            empty_cols = df_sample.isnull().all().sum()
                            st.metric("Columnas Vacías", empty_cols)
                        
                        st.write("**Columnas encontradas:**")
                        cols_text = ", ".join(df_sample.columns.tolist())
                        st.text_area("Lista de columnas:", value=cols_text, height=100, disabled=True)
                        
                        st.write("**Muestra de datos (primeras 10 filas):**")
                        st.dataframe(df_sample)
                    
                    # Mensaje de estado
                    if extension in ['xlsx', 'xls'] and 'missing_sheets' in locals() and missing_sheets:
                        st.warning(f"⚠️ Faltan las siguientes hojas requeridas: {', '.join(missing_sheets)}")
                        st.info("El sistema puede intentar generar automáticamente algunas hojas faltantes.")
                    else:
                        st.success("✅ El archivo parece tener la estructura correcta para ser procesado.")
                
                except Exception as e:
                    st.error(f"Error al analizar el archivo: {str(e)}")
            
            # Separador visual
            st.divider()
        
        # BOTONES DEL FORMULARIO - SIEMPRE VISIBLES
        col1, col2 = st.columns(2)
        
        with col1:
            # Botón para procesar - siempre visible
            submit = st.form_submit_button("🚀 Procesar y Cargar Archivo", type="primary", disabled=(archivo is None))
        
        with col2:
            # Botón para limpiar - siempre visible
            limpiar = st.form_submit_button("🗑️ Limpiar Formulario", type="secondary")
        
        # Procesar según el botón presionado
        if limpiar:
            # Limpiar estado del formulario
            st.rerun()
        
        elif submit:
            if not archivo:
                st.error("Por favor, seleccione un archivo Excel o CSV.")
            elif not id_licitacion or not modalidad or not numero_anio or not nombre_llamado or not empresa_adjudicada or not vigencia_contrato or not numero_contrato:
                st.error("Por favor, complete todos los campos obligatorios.")
            else:
                # Usar el esquema personalizado
                esquema = esquema_personalizado
                empresa_para_tablas = empresa_adjudicada.strip().upper().replace(" ", "_")
                
                # Crear diccionario con datos del formulario
                datos_formulario = {
                    'id_licitacion': id_licitacion,
                    'modalidad': modalidad,
                    'numero_anio': numero_anio,
                    'nombre_llamado': nombre_llamado,
                    'empresa_adjudicada': empresa_adjudicada,
                    'ruc': ruc,
                    'fecha_firma': fecha_firma,
                    'numero_contrato': numero_contrato,
                    'vigencia_contrato': vigencia_contrato
                }
                
                # Procesar el archivo con el prefijo de empresa en las tablas
                with st.spinner("Procesando archivo y creando tablas..."):
                    success, message = cargar_archivo_a_postgres(
                        archivo,
                        archivo.name,
                        esquema,
                        empresa_para_tablas,  # Pasar la empresa como parámetro adicional
                        datos_formulario  # Pasar datos del formulario
                    )
                
                if success:
                    st.success(f"Archivo cargado correctamente en el esquema '{esquema}' con tablas de empresa '{empresa_para_tablas}'")
                    st.balloons()
                else:
                    st.error(message)

def pagina_cargar_archivo():
    """Página para cargar un nuevo archivo"""
    st.header("Cargar Archivo")
    
    # Formulario para subir archivo
    with st.form("upload_form"):
        st.subheader("📋 Información de la Licitación")
        
        # Campos en el orden solicitado
        col1, col2 = st.columns(2)
        
        with col1:
            id_licitacion = st.text_input("I.D.:")
            modalidad = st.text_input("Modalidad:", placeholder="Ej: CD, LP, LC, CO, LPN, CVE, LPI, LCO, MCN...")
            numero_anio = st.text_input("N° / Año de Modalidad:")
        
        with col2:
            nombre_llamado = st.text_input("Nombre del llamado:")
            
            # Autocompletado de proveedores
            proveedores_existentes = obtener_proveedores()
            
            if proveedores_existentes:
                empresa_options = [f"{p['nombre']} - {p['ruc']}" for p in proveedores_existentes]
                
                empresa_seleccionada = st.selectbox(
                    "Empresa Adjudicada:",
                    options=["Seleccionar..."] + empresa_options + ["+ Nuevo Proveedor"]
                )
                
                if empresa_seleccionada == "+ Nuevo Proveedor":
                    st.info("💡 Vaya a 'Gestión de Proveedores' para registrar una nueva empresa")
                    empresa_adjudicada = st.text_input("Nombre de la empresa:", disabled=True)
                    ruc = st.text_input("RUC:", disabled=True)
                elif empresa_seleccionada != "Seleccionar...":
                    # Extraer datos del proveedor seleccionado
                    empresa_adjudicada = empresa_seleccionada.split(" - ")[0]
                    ruc_autocompletado = empresa_seleccionada.split(" - ")[-1]
                    ruc = st.text_input("RUC:", value=ruc_autocompletado, disabled=True)
                else:
                    empresa_adjudicada = ""
                    ruc = st.text_input("RUC:", disabled=True)
            else:
                st.warning("⚠️ No hay proveedores registrados. Registre primero en 'Gestión de Proveedores'")
                empresa_adjudicada = st.text_input("Empresa Adjudicada:", disabled=True)
                ruc = st.text_input("RUC:", disabled=True)
        
        # Segunda fila de campos
        col3, col4 = st.columns(2)
        
        with col3:
            vigencia_contrato = st.text_input("Vigencia del Contrato:")
        
        with col4:
            fecha_firma = st.date_input("Fecha de la firma del contrato:")
        
        # Otros campos adicionales
        col5, col6 = st.columns(2)
        
        with col5:
            numero_contrato = st.text_input("Número de contrato/año:")
        
        with col6:
            # Generar sugerencia de nombre de esquema
            if modalidad and numero_anio:
                esquema_sugerido = f"{modalidad.strip().lower()}-{numero_anio.strip()}"
                esquema_personalizado = st.text_input("Nombre del esquema:", value=esquema_sugerido)
            else:
                esquema_personalizado = st.text_input("Nombre del esquema:")
        
        st.divider()
        
        # Campo para subir archivo
        archivo = st.file_uploader("Seleccionar archivo:", type=["csv", "xlsx", "xls"])
        
        # SECCIÓN DE ANÁLISIS DE DATOS
        if archivo is not None:
            st.subheader("📊 Análisis del Archivo")
            
            # Checkbox para activar análisis
            mostrar_analisis = st.checkbox("🔍 Mostrar análisis detallado del archivo")
            
            if mostrar_analisis:
                try:
                    # Determinar el tipo de archivo
                    extension = archivo.name.split('.')[-1].lower()
                    
                    if extension in ['xlsx', 'xls']:
                        # Análisis para archivos Excel
                        xls = pd.ExcelFile(archivo)
                        
                        st.write(f"**📁 Archivo:** {archivo.name}")
                        st.write(f"**📄 Tipo:** Excel ({extension.upper()})")
                        st.write(f"**📋 Hojas encontradas:** {len(xls.sheet_names)}")
                        
                        # Mostrar hojas disponibles
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write("**Hojas en el archivo:**")
                            for i, sheet in enumerate(xls.sheet_names, 1):
                                st.write(f"{i}. {sheet}")
                        
                        with col2:
                            # Verificar hojas requeridas
                            required_sheets = ["ejecucion_general", "ejecucion_por_zonas", "orden_de_compra", "llamado"]
                            missing_sheets = []
                            found_sheets = []
                            
                            for req_sheet in required_sheets:
                                found = any(req_sheet.lower() == sheet.lower() for sheet in xls.sheet_names)
                                if found:
                                    found_sheets.append(req_sheet)
                                else:
                                    missing_sheets.append(req_sheet)
                            
                            st.write("**Estado de hojas requeridas:**")
                            for sheet in found_sheets:
                                st.write(f"✅ {sheet}")
                            for sheet in missing_sheets:
                                st.write(f"❌ {sheet}")
                        
                        # Análisis detallado de cada hoja
                        hoja_analisis = st.selectbox(
                            "Seleccionar hoja para análisis detallado:",
                            options=xls.sheet_names
                        )
                        
                        if hoja_analisis:
                            # Leer una muestra de la hoja seleccionada
                            df_sample = pd.read_excel(xls, sheet_name=hoja_analisis, nrows=10)
                            
                            st.write(f"**📊 Análisis de la hoja '{hoja_analisis}':**")
                            
                            # Información básica
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Total Columnas", len(df_sample.columns))
                            with col2:
                                # Leer toda la hoja para contar filas (puede ser lento para archivos grandes)
                                try:
                                    df_full = pd.read_excel(xls, sheet_name=hoja_analisis)
                                    st.metric("Total Filas", len(df_full))
                                except:
                                    st.metric("Total Filas", "Error al contar")
                            with col3:
                                # Contar columnas vacías
                                empty_cols = df_sample.isnull().all().sum()
                                st.metric("Columnas Vacías", empty_cols)
                            
                            # Mostrar nombres de columnas
                            st.write("**Columnas encontradas:**")
                            cols_text = ", ".join(df_sample.columns.tolist())
                            st.text_area("Lista de columnas:", value=cols_text, height=100, disabled=True)
                            
                            # Mostrar muestra de datos
                            st.write("**Muestra de datos (primeras 10 filas):**")
                            st.dataframe(df_sample)
                            
                            # Verificar tipos de datos
                            st.write("**Tipos de datos por columna:**")
                            tipos_df = pd.DataFrame({
                                'Columna': df_sample.columns,
                                'Tipo': df_sample.dtypes.values,
                                'Valores Nulos': df_sample.isnull().sum().values,
                                'Valores Únicos': df_sample.nunique().values
                            })
                            st.dataframe(tipos_df)
                    
                    elif extension == 'csv':
                        # Análisis para archivos CSV
                        contenido = archivo.getvalue().decode('utf-8')
                        df_sample = pd.read_csv(io.StringIO(contenido), nrows=10)
                        
                        st.write(f"**📁 Archivo:** {archivo.name}")
                        st.write(f"**📄 Tipo:** CSV")
                        
                        # Información básica
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Columnas", len(df_sample.columns))
                        with col2:
                            try:
                                df_full = pd.read_csv(io.StringIO(contenido))
                                st.metric("Total Filas", len(df_full))
                            except:
                                st.metric("Total Filas", "Error al contar")
                        with col3:
                            empty_cols = df_sample.isnull().all().sum()
                            st.metric("Columnas Vacías", empty_cols)
                        
                        st.write("**Columnas encontradas:**")
                        cols_text = ", ".join(df_sample.columns.tolist())
                        st.text_area("Lista de columnas:", value=cols_text, height=100, disabled=True)
                        
                        st.write("**Muestra de datos (primeras 10 filas):**")
                        st.dataframe(df_sample)
                    
                    # Mensaje de estado
                    if extension in ['xlsx', 'xls'] and 'missing_sheets' in locals() and missing_sheets:
                        st.warning(f"⚠️ Faltan las siguientes hojas requeridas: {', '.join(missing_sheets)}")
                        st.info("El sistema puede intentar generar automáticamente algunas hojas faltantes.")
                    else:
                        st.success("✅ El archivo parece tener la estructura correcta para ser procesado.")
                
                except Exception as e:
                    st.error(f"Error al analizar el archivo: {str(e)}")
            
            # Separador visual
            st.divider()
        
        # BOTONES DEL FORMULARIO - SIEMPRE VISIBLES
        col1, col2 = st.columns(2)
        
        with col1:
            # Botón para procesar - siempre visible
            submit = st.form_submit_button("🚀 Procesar y Cargar Archivo", type="primary", disabled=(archivo is None))
        
        with col2:
            # Botón para limpiar - siempre visible
            limpiar = st.form_submit_button("🗑️ Limpiar Formulario", type="secondary")
        
        # Procesar según el botón presionado
        if limpiar:
            # Limpiar estado del formulario
            st.rerun()
        
        elif submit:
            if not archivo:
                st.error("Por favor, seleccione un archivo Excel o CSV.")
            elif not id_licitacion or not modalidad or not numero_anio or not nombre_llamado or not empresa_adjudicada or not vigencia_contrato or not numero_contrato:
                st.error("Por favor, complete todos los campos obligatorios.")
            else:
                # Usar el esquema personalizado
                esquema = esquema_personalizado
                empresa_para_tablas = empresa_adjudicada.strip().upper().replace(" ", "_")
                
                # Crear diccionario con datos del formulario
                datos_formulario = {
                    'id_licitacion': id_licitacion,
                    'modalidad': modalidad,
                    'numero_anio': numero_anio,
                    'nombre_llamado': nombre_llamado,
                    'empresa_adjudicada': empresa_adjudicada,
                    'ruc': ruc,
                    'fecha_firma': fecha_firma,
                    'numero_contrato': numero_contrato,
                    'vigencia_contrato': vigencia_contrato
                }
                
                # Procesar el archivo con el prefijo de empresa en las tablas
                with st.spinner("Procesando archivo y creando tablas..."):
                    success, message = cargar_archivo_a_postgres(
                        archivo,
                        archivo.name,
                        esquema,
                        empresa_para_tablas,  # Pasar la empresa como parámetro adicional
                        datos_formulario  # Pasar datos del formulario
                    )
                
                if success:
                    st.success(f"Archivo cargado correctamente en el esquema '{esquema}' con tablas de empresa '{empresa_para_tablas}'")
                    st.balloons()
                    registrar_actividad(
                        accion="CREATE",
                        modulo="LICITACIONES",
                        descripcion=f"Archivo cargado: {archivo.name} en esquema {esquema}",
                        esquema_afectado=esquema
                    )
                else:
                    st.error(message)

def pagina_ver_cargas():
    """Página para ver las cargas realizadas"""
    st.header("Archivos Cargados")
    
    col1, col2 = st.columns([1, 3])
    
    with col1:
        # Botón para actualizar manualmente
        if st.button("🔄 Actualizar ahora"):
            st.rerun()
    
    with col2:
        # Opción para actualización automática
        auto_refresh = st.checkbox("Actualizar automáticamente cada 1 minuto", value=False)
    
    # Mostrar tiempo hasta próxima actualización si está activado
    if auto_refresh:
        # Inicializar contador si no existe
        if 'last_refresh_time' not in st.session_state:
            st.session_state.last_refresh_time = time.time()
        
        # Calcular tiempo transcurrido
        current_time = time.time()
        elapsed = current_time - st.session_state.last_refresh_time
        remaining = max(60 - elapsed, 0)
        
        # Mostrar barra de progreso para tiempo restante
        st.progress(elapsed / 60)
        st.caption(f"Próxima actualización en {int(remaining)} segundos")
        
        # Refrescar si ha pasado 1 minuto
        if elapsed >= 60:
            st.session_state.last_refresh_time = current_time
            st.rerun()
    
    # Obtener archivos actualizados
    archivos = obtener_archivos_cargados()
    
    if archivos:
        # Convertir a DataFrame para mejor visualización
        df_archivos = pd.DataFrame(archivos)
        
        # Dar formato a las fechas
        if 'fecha_carga' in df_archivos.columns:
            df_archivos['fecha_carga'] = pd.to_datetime(df_archivos['fecha_carga']).dt.strftime('%Y-%m-%d %H:%M')
        
        # Formatear columnas para el usuario final
        df_archivos_formateado = formatear_columnas_tabla(df_archivos)
        
        # Colorear estado
        def colorear_estado(estado):
            if estado == 'Activo':
                return 'background-color: #d4edda; color: #155724'
            elif estado == 'Eliminado':
                return 'background-color: #f8d7da; color: #721c24'
            else:
                return ''
        
        # Aplicar estilo condicional
        df_styled = df_archivos_formateado.style.applymap(colorear_estado, subset=['Estado'])
        
        # Mostrar DataFrame
        st.dataframe(df_styled, use_container_width=True)
        
        # Ofrecer descarga del contenido original si está activo
        archivos_activos = [a for a in archivos if a['estado'] == 'Activo']
        if archivos_activos:
            archivo_id = st.selectbox(
                "Seleccionar archivo para descargar contenido original:",
                options=[f"{a['nombre_archivo']} ({a['esquema']})" for a in archivos_activos],
                index=None
            )
            
            if archivo_id:
                archivo_seleccionado = next((a for a in archivos_activos if f"{a['nombre_archivo']} ({a['esquema']})" == archivo_id), None)
                
                if archivo_seleccionado:
                    # Obtener contenido original
                    with engine.connect() as conn:
                        query = text("""
                            SELECT contenido_original
                            FROM reactivos_py.archivos_cargados
                            WHERE id = :id
                        """)
                        
                        result = conn.execute(query, {'id': archivo_seleccionado['id']})
                        contenido = result.scalar()
                        
                        if contenido:
                            st.download_button(
                                label="Descargar archivo original",
                                data=contenido.encode('utf-8'),
                                file_name=archivo_seleccionado['nombre_archivo'],
                                mime="text/csv"
                            )
        else:
            st.info("No hay archivos activos para descargar.")
    else:
        st.info("No hay archivos cargados para mostrar.")

def pagina_eliminar_esquemas():
    """Página para eliminar esquemas (licitaciones)"""
    st.header("Eliminar Licitaciones")
    
    st.warning("⚠️ Advertencia: Esta operación eliminará permanentemente todos los datos asociados a la licitación seleccionada.")
    
    # Obtener esquemas existentes
    esquemas = obtener_esquemas_postgres()
    
    if esquemas:
        esquema_a_eliminar = st.selectbox(
            "Seleccionar licitación a eliminar:",
            options=esquemas
        )
        
        if st.button("Eliminar Licitación", type="primary", use_container_width=True):
            # Pedir confirmación
            if st.checkbox("Confirmo que deseo eliminar esta licitación permanentemente"):
                success, message = eliminar_esquema_postgres(esquema_a_eliminar)
                
                if success:
                    st.success(message)
                    time.sleep(2)
                    st.rerun()
                else:
                    st.error(message)
            else:
                st.warning("Debe confirmar la eliminación")
    else:
        st.info("No hay licitaciones para eliminar.")

def pagina_gestionar_proveedores():
    """Página para gestionar proveedores"""
    st.header("Gestión de Proveedores")
    if 'last_page' not in st.session_state:
        st.session_state.last_page = None
    # Pestañas para diferentes funciones
    if st.session_state.user_role == 'admin':
        tab1, tab2, tab3 = st.tabs(["Lista de Proveedores", "Nuevo Proveedor", "Importar CSV"])
    else:
        tab1, tab2, tab3 = st.tabs(["Lista de Proveedores", "Nuevo Proveedor", "Importar CSV"])
    
    with tab1:
        st.subheader("Proveedores Registrados")
        
        # Filtros
        col1, col2, col3 = st.columns(3)
        with col1:
            filtro_ruc = st.text_input("🔍 Filtrar por RUC:", placeholder="Ej: 80026564")
        with col2:
            filtro_nombre = st.text_input("🔍 Filtrar por Razón Social:", placeholder="Ej: CHACO")
        with col3:
            mostrar_inactivos = st.checkbox("Mostrar inactivos", value=False)
        
        # Obtener proveedores con filtros
        try:
            with engine.connect() as conn:
                query_base = """
                    SELECT id, ruc, razon_social, direccion, correo_electronico, 
                           telefono, contacto_nombre, activo, fecha_registro, fecha_actualizacion
                    FROM reactivos_py.proveedores
                    WHERE 1=1
                """
                params = {}
                
                if filtro_ruc:
                    query_base += " AND ruc ILIKE :ruc"
                    params['ruc'] = f"%{filtro_ruc}%"
                
                if filtro_nombre:
                    query_base += " AND razon_social ILIKE :nombre"
                    params['nombre'] = f"%{filtro_nombre}%"
                
                if not mostrar_inactivos:
                    query_base += " AND activo = TRUE"
                
                query_base += " ORDER BY razon_social"
                
                query = text(query_base)
                result = conn.execute(query, params)
                
                proveedores = []
                for row in result:
                    proveedores.append({
                        'id': row[0],
                        'ruc': row[1],
                        'razon_social': row[2],
                        'direccion': row[3] or 'No especificada',
                        'correo_electronico': row[4] or 'No especificado',
                        'telefono': row[5] or 'No especificado',
                        'contacto_nombre': row[6] or 'No especificado',
                        'activo': '✅ Activo' if row[7] else '❌ Inactivo',
                        'fecha_registro': row[8],
                        'fecha_actualizacion': row[9]
                    })
                
                if proveedores:
                    # Convertir a DataFrame para mejor visualización
                    df_proveedores = pd.DataFrame(proveedores)
                    
                    # Dar formato a las fechas
                    if 'fecha_registro' in df_proveedores.columns:
                        df_proveedores['fecha_registro'] = pd.to_datetime(df_proveedores['fecha_registro']).dt.strftime('%Y-%m-%d')
                    if 'fecha_actualizacion' in df_proveedores.columns:
                        df_proveedores['fecha_actualizacion'] = pd.to_datetime(df_proveedores['fecha_actualizacion']).dt.strftime('%Y-%m-%d %H:%M')
                    
                    # Mostrar proveedores
                    df_proveedores_formateado = formatear_columnas_tabla(df_proveedores)
                    st.dataframe(df_proveedores_formateado, use_container_width=True)
                    
                    # NUEVA SECCIÓN: Editar Proveedor
                    st.divider()
                    st.subheader("✏️ Editar Datos de Proveedor")
                    
                    # Selector de proveedor para editar
                    proveedor_options = [f"{p['ruc']} - {p['razon_social']}" for p in proveedores]
                    proveedor_seleccionado = st.selectbox(
                        "Seleccionar proveedor para editar:",
                        options=["Seleccionar..."] + proveedor_options
                    )
                    
                    if proveedor_seleccionado != "Seleccionar...":
                        # Encontrar el proveedor seleccionado
                        ruc_seleccionado = proveedor_seleccionado.split(" - ")[0]
                        proveedor = next((p for p in proveedores if p['ruc'] == ruc_seleccionado), None)
                        
                        if proveedor:
                            with st.form("editar_proveedor_form"):
                                st.write(f"**Editando:** {proveedor['razon_social']} ({proveedor['ruc']})")
                                
                                col1, col2 = st.columns(2)
                                with col1:
                                    nuevo_ruc = st.text_input("RUC:", value=proveedor['ruc'])
                                    nueva_razon = st.text_input("Razón Social:", value=proveedor['razon_social'])
                                    nueva_direccion = st.text_area("Dirección:", value=proveedor['direccion'] if proveedor['direccion'] != 'No especificada' else '')
                                
                                with col2:
                                    nuevo_correo = st.text_input("Correo Electrónico:", value=proveedor['correo_electronico'] if proveedor['correo_electronico'] != 'No especificado' else '')
                                    nuevo_telefono = st.text_input("Teléfono:", value=proveedor['telefono'] if proveedor['telefono'] != 'No especificado' else '')
                                    nuevo_contacto = st.text_input("Nombre de Contacto:", value=proveedor['contacto_nombre'] if proveedor['contacto_nombre'] != 'No especificado' else '')
                                
                                # Estado activo/inactivo
                                activo_actual = proveedor['activo'] == '✅ Activo'
                                activo = st.checkbox("Proveedor activo", value=activo_actual)
                                
                                observaciones = st.text_area("Observaciones:")
                                
                                # Botones de acción
                                col_btn1, col_btn2, col_btn3 = st.columns(3)
                                with col_btn1:
                                    actualizar = st.form_submit_button("💾 Actualizar Proveedor", type="primary")
                                with col_btn2:
                                    cambiar_estado = st.form_submit_button("🔄 Cambiar Estado", type="secondary")
                                with col_btn3:
                                    eliminar = st.form_submit_button("🗑️ Eliminar Proveedor", type="secondary", help="Esta acción eliminará permanentemente el proveedor")
                                
                                # Procesar eliminación
                                # Procesar eliminación
                                if eliminar:
                                    st.warning("⚠️ **CONFIRMACIÓN REQUERIDA**")
                                    confirmar_eliminacion = st.checkbox(
                                        f"Confirmo que deseo eliminar permanentemente a '{proveedor['razon_social']}' (RUC: {proveedor['ruc']})",
                                        key=f"confirm_delete_{proveedor['id']}"
                                    )
                                    
                                    if confirmar_eliminacion:
                                        # Usar un botón fuera del formulario para la confirmación final
                                        col_confirm, _ = st.columns([1, 2])
                                        with col_confirm:
                                            if st.button("✅ SÍ, ELIMINAR DEFINITIVAMENTE", type="primary", key=f"confirm_button_{proveedor['id']}"):
                                                try:
                                                    with st.spinner("Eliminando proveedor... Por favor espere"):
                                                        st.warning("⏳ OPERACIÓN EN PROGRESO - NO INTERRUMPA")
                                                        
                                                        with engine.connect() as conn:
                                                            # Proceder con la eliminación sin verificar si está en uso
                                                            query_delete = text("DELETE FROM reactivos_py.proveedores WHERE id = :id")
                                                            conn.execute(query_delete, {'id': proveedor['id']})
                                                            conn.commit()  # Hacer commit explícito
                                                            
                                                            # Registrar actividad de eliminación
                                                            registrar_actividad(
                                                                accion="DELETE",
                                                                modulo="PROVEEDORES",
                                                                descripcion=f"Proveedor eliminado: {proveedor['razon_social']} (RUC: {proveedor['ruc']})",
                                                                valores_anteriores={
                                                                    'id': proveedor['id'],
                                                                    'ruc': proveedor['ruc'],
                                                                    'razon_social': proveedor['razon_social'],
                                                                    'direccion': proveedor['direccion'],
                                                                    'correo_electronico': proveedor['correo_electronico']
                                                                }
                                                            )
                                                        
                                                        st.success(f"✅ Proveedor '{proveedor['razon_social']}' eliminado correctamente")
                                                        st.balloons()
                                                        
                                                        # Guardar el estado para mantener la página
                                                        if 'last_page' not in st.session_state:
                                                            st.session_state.last_page = None
                                                        st.session_state.last_page = "gestionar_proveedores"
                                                        
                                                        # Detener la ejecución un momento para que el usuario vea el mensaje
                                                        time.sleep(5)
                                                        st.rerun()
                                                    
                                                except Exception as e:
                                                    st.error(f"Error al eliminar proveedor: {e}")
                                    else:
                                        st.info("👆 Active la confirmación y presione el botón rojo para proceder")
                                
                                # Procesar actualización
                                if actualizar:
                                    try:
                                        with engine.connect() as conn:
                                            # Verificar si el RUC ya existe en otro proveedor
                                            if nuevo_ruc != proveedor['ruc']:
                                                query_check = text("""
                                                    SELECT COUNT(*) FROM reactivos_py.proveedores
                                                    WHERE ruc = :ruc AND id != :id
                                                """)
                                                result_check = conn.execute(query_check, {
                                                    'ruc': nuevo_ruc,
                                                    'id': proveedor['id']
                                                })
                                                count = result_check.scalar()
                                                
                                                if count > 0:
                                                    st.error(f"El RUC '{nuevo_ruc}' ya está registrado para otro proveedor.")
                                                    return
                                            
                                            # Actualizar proveedor
                                            query = text("""
                                                UPDATE reactivos_py.proveedores
                                                SET ruc = :ruc, razon_social = :razon_social, direccion = :direccion,
                                                    correo_electronico = :correo, telefono = :telefono,
                                                    contacto_nombre = :contacto, observaciones = :observaciones,
                                                    activo = :activo
                                                WHERE id = :id
                                            """)
                                            
                                            conn.execute(query, {
                                                'ruc': nuevo_ruc,
                                                'razon_social': nueva_razon,
                                                'direccion': nueva_direccion or None,
                                                'correo': nuevo_correo or None,
                                                'telefono': nuevo_telefono or None,
                                                'contacto': nuevo_contacto or None,
                                                'observaciones': observaciones,
                                                'activo': activo,
                                                'id': proveedor['id']
                                            })
                                            
                                            # Hacer commit explícito
                                            conn.commit()
                                            
                                            st.success(f"✅ Proveedor {nueva_razon} actualizado correctamente")
                                            time.sleep(5)  # Esperar más tiempo
                                            st.rerun()
                                    except Exception as e:
                                        st.error(f"Error al actualizar proveedor: {e}")
                                
                                # Procesar cambio de estado
                                if cambiar_estado:
                                    try:
                                        nuevo_estado = not activo_actual
                                        estado_texto = "activo" if nuevo_estado else "inactivo"
                                        
                                        with st.spinner(f"Cambiando estado a {estado_texto}... Por favor espere"):
                                            with engine.connect() as conn:
                                                query = text("""
                                                    UPDATE reactivos_py.proveedores
                                                    SET activo = :activo
                                                    WHERE id = :id
                                                """)
                                                
                                                conn.execute(query, {
                                                    'activo': nuevo_estado,
                                                    'id': proveedor['id']
                                                })
                                                
                                                # Hacer commit explícito
                                                conn.commit()
                                                
                                                st.success(f"✅ Proveedor {proveedor['razon_social']} marcado como {estado_texto}")
                                                # Esperar más tiempo para que la BD procese el cambio
                                                time.sleep(5)
                                                st.rerun()
                                    except Exception as e:
                                        st.error(f"Error al cambiar estado del proveedor: {e}")
                else:
                    st.info("No hay proveedores registrados que coincidan con los filtros.")
        except Exception as e:
            st.error(f"Error al obtener proveedores: {e}")
    
    with tab2:
        st.subheader("Registrar Nuevo Proveedor")
        
        with st.form("nuevo_proveedor_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                ruc_proveedor = st.text_input("RUC: *", placeholder="Ej: 80026564-5")
                razon_social = st.text_input("Razón Social: *", placeholder="Ej: EMPRESA S.A.")
                direccion = st.text_area("Dirección:", placeholder="Dirección completa")
            
            with col2:
                correo_electronico = st.text_input("Correo Electrónico:", placeholder="contacto@empresa.com")
                telefono = st.text_input("Teléfono:", placeholder="021-123456")
                contacto_nombre = st.text_input("Nombre de Contacto:", placeholder="Juan Pérez")
            
            observaciones = st.text_area("Observaciones:", placeholder="Información adicional...")
            
            submit = st.form_submit_button("📝 Registrar Proveedor", type="primary")
            
            if submit:
                if not ruc_proveedor or not razon_social:
                    st.error("Por favor, complete los campos obligatorios (RUC y Razón Social).")
                else:
                    try:
                        # Verificar si el RUC ya existe
                        with engine.connect() as conn:
                            query = text("SELECT COUNT(*) FROM reactivos_py.proveedores WHERE ruc = :ruc")
                            result = conn.execute(query, {'ruc': ruc_proveedor})
                            count = result.scalar()
                            
                            if count > 0:
                                st.error(f"El RUC '{ruc_proveedor}' ya está registrado.")
                            else:
                                # Registrar nuevo proveedor
                                query = text("""
                                    INSERT INTO reactivos_py.proveedores 
                                    (ruc, razon_social, direccion, correo_electronico, telefono, contacto_nombre, observaciones)
                                    VALUES (:ruc, :razon_social, :direccion, :correo, :telefono, :contacto, :observaciones)
                                """)
                                
                                conn.execute(query, {
                                    'ruc': ruc_proveedor,
                                    'razon_social': razon_social,
                                    'direccion': direccion,
                                    'correo': correo_electronico,
                                    'telefono': telefono,
                                    'contacto': contacto_nombre,
                                    'observaciones': observaciones
                                })
                                
                                # Hacer commit explícito
                                conn.commit()
                                
                                st.success(f"Proveedor '{razon_social}' registrado exitosamente")
                                time.sleep(5)  # Esperar más tiempo
                                st.rerun()
                    except Exception as e:
                        st.error(f"Error al registrar proveedor: {e}")
    
    with tab3:
        st.subheader("Importar Proveedores desde CSV")
        
        st.info("""
        📋 **Formato esperado del CSV:**
        - Columna 1: RUC
        - Columna 2: Razón Social
        - Columna 3: Dirección (opcional)
        - Columna 4: Correo Electrónico (opcional)
        """)
        
        archivo_csv = st.file_uploader("Seleccionar archivo CSV:", type=["csv"])
        
        if archivo_csv is not None:
            st.write(f"**Archivo seleccionado:** {archivo_csv.name}")
            st.write(f"**Tamaño:** {archivo_csv.size} bytes")
            
            # Botón para analizar archivo
            if st.button("🔍 Analizar Archivo"):
                try:
                    # Función mejorada para leer CSV completo
                    def leer_csv_robusto(archivo):
                        """Lee un archivo CSV de forma robusta con múltiples intentos"""
                        delimitadores = [',', ';', '\t', '|']
                        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
                        
                        mejor_df = None
                        mejor_config = None
                        max_filas = 0
                        
                        # Probar todas las combinaciones y quedarse con la que lee más filas
                        for encoding in encodings:
                            for delimiter in delimitadores:
                                try:
                                    archivo.seek(0)
                                    
                                    # Leer TODO el archivo (sin límite de filas)
                                    df_temp = pd.read_csv(
                                        archivo, 
                                        delimiter=delimiter,
                                        encoding=encoding,
                                        quotechar='"',
                                        skipinitialspace=True,
                                        on_bad_lines='skip',
                                        engine='python'
                                    )
                                    
                                    # Verificar que el DataFrame tenga contenido válido
                                    if len(df_temp) > max_filas and len(df_temp.columns) >= 2:
                                        # Verificar que no todas las celdas estén vacías
                                        celdas_no_vacias = df_temp.notna().sum().sum()
                                        if celdas_no_vacias > 0:
                                            mejor_df = df_temp.copy()
                                            mejor_config = (delimiter, encoding)
                                            max_filas = len(df_temp)
                                    
                                except Exception:
                                    continue
                        
                        return mejor_df, mejor_config
                    
                    # Intentar leer el archivo completo
                    st.write("**🔄 Analizando archivo completo...**")
                    
                    with st.spinner("Procesando archivo CSV..."):
                        df, config = leer_csv_robusto(archivo_csv)
                    
                    if df is None:
                        st.error("❌ No se pudo leer el archivo con ningún formato estándar.")
                        return
                    
                    delimiter_usado, encoding_usado = config
                    st.success(f"✅ Archivo leído correctamente (Delimitador: '{delimiter_usado}', Encoding: {encoding_usado})")
                    
                    # Limpiar DataFrame
                    df_original = df.copy()
                    df = df.dropna(how='all')
                    df = df.fillna('')
                    
                    # Información detallada del archivo
                    st.write("### 📊 Información del Archivo")
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("📝 Filas Totales", len(df_original))
                    with col2:
                        st.metric("✅ Filas Válidas", len(df))
                    with col3:
                        st.metric("📋 Columnas", len(df.columns))
                    with col4:
                        celdas_con_datos = df.astype(str).ne('').sum().sum()
                        st.metric("📊 Celdas con Datos", celdas_con_datos)
                    
                    # Guardar DataFrame procesado en session_state
                    st.session_state.df_importar = df
                    st.session_state.delimiter_usado = delimiter_usado
                    st.session_state.encoding_usado = encoding_usado
                    st.session_state.filas_originales = len(df_original)
                    st.session_state.celdas_con_datos = celdas_con_datos
                    
                    st.success(f"✅ Archivo analizado completamente: {len(df):,} filas listas para mapeo")
                    
                except Exception as e:
                    st.error(f"❌ Error al procesar el archivo: {e}")
            
            # Mostrar mapeo y importación si el DataFrame está disponible
            if 'df_importar' in st.session_state:
                df = st.session_state.df_importar
                
                st.divider()
                st.subheader("🗂️ Mapeo de Columnas")
                
                # Autodetección inteligente de columnas
                def detectar_columna_ruc(columnas):
                    for i, col in enumerate(columnas):
                        col_lower = str(col).lower()
                        if any(palabra in col_lower for palabra in ['ruc', 'rut', 'cuit', 'nit', 'documento']):
                            return i
                    return 0
                
                def detectar_columna_razon(columnas):
                    for i, col in enumerate(columnas):
                        col_lower = str(col).lower()
                        if any(palabra in col_lower for palabra in ['razon', 'nombre', 'empresa', 'social']):
                            return i
                    return 1 if len(columnas) > 1 else 0
                
                ruc_idx = detectar_columna_ruc(df.columns.tolist())
                razon_idx = detectar_columna_razon(df.columns.tolist())
                
                col1, col2 = st.columns(2)
                with col1:
                    col_ruc = st.selectbox("Columna RUC:", options=df.columns.tolist(), index=ruc_idx)
                    col_razon = st.selectbox("Columna Razón Social:", options=df.columns.tolist(), index=razon_idx)
                
                with col2:
                    opciones_direccion = ["No mapear"] + df.columns.tolist()
                    col_direccion = st.selectbox("Columna Dirección (opcional):", options=opciones_direccion)
                    
                    opciones_correo = ["No mapear"] + df.columns.tolist()
                    col_correo = st.selectbox("Columna Correo (opcional):", options=opciones_correo)
                
                # Análisis de datos antes de importar
                registros_procesables = df[(df[col_ruc].astype(str).str.strip().ne('')) & 
                                         (df[col_razon].astype(str).str.strip().ne(''))].shape[0]
                
                # Checkbox para confirmar importación
                confirmar_importacion = st.checkbox(
                    f"Confirmo la importación de {registros_procesables:,} registros procesables",
                    help="Esta acción insertará todos los registros válidos en la base de datos"
                )
                
                # Importación
                if confirmar_importacion:
                    if st.button("🚀 Importar Proveedores", type="primary"):
                        try:
                            insertados = 0
                            duplicados = 0
                            errores = 0
                            errores_detalle = []
                            
                            progress_bar = st.progress(0)
                            status_text = st.empty()
                            
                            total_filas = len(df)
                            
                            for index, row in df.iterrows():
                                try:
                                    progress = (index + 1) / total_filas
                                    progress_bar.progress(progress)
                                    status_text.text(f'Procesando fila {index + 1} de {total_filas}...')
                                    
                                    ruc = str(row[col_ruc]).strip()
                                    razon_social = str(row[col_razon]).strip()
                                    direccion = str(row[col_direccion]).strip() if col_direccion != "No mapear" and pd.notna(row[col_direccion]) else None
                                    correo = str(row[col_correo]).strip() if col_correo != "No mapear" and pd.notna(row[col_correo]) else None
                                    
                                    if not ruc or not razon_social or ruc.lower() == "nan" or razon_social.lower() == "nan":
                                        errores += 1
                                        errores_detalle.append(f"Fila {index + 1}: RUC o Razón Social vacíos")
                                        continue
                                    
                                    with engine.connect() as conn:
                                        trans = conn.begin()
                                        try:
                                            query = text("""
                                                INSERT INTO reactivos_py.proveedores (ruc, razon_social, direccion, correo_electronico)
                                                VALUES (:ruc, :razon_social, :direccion, :correo)
                                                ON CONFLICT (ruc) DO NOTHING
                                                RETURNING id
                                            """)
                                            
                                            result = conn.execute(query, {
                                                'ruc': ruc,
                                                'razon_social': razon_social,
                                                'direccion': direccion,
                                                'correo': correo
                                            })
                                            
                                            if result.rowcount > 0:
                                                trans.commit()
                                                insertados += 1
                                            else:
                                                trans.rollback()
                                                duplicados += 1
                                                errores_detalle.append(f"Fila {index + 1}: RUC {ruc} ya existe")
                                            
                                        except Exception as e:
                                            trans.rollback()
                                            errores += 1
                                            errores_detalle.append(f"Fila {index + 1}: Error - {str(e)}")
                                
                                except Exception as e:
                                    errores += 1
                                    errores_detalle.append(f"Fila {index + 1}: Error general - {str(e)}")
                            
                            progress_bar.empty()
                            status_text.empty()
                            
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("✅ Insertados", insertados)
                            with col2:
                                st.metric("⚠️ Duplicados", duplicados)
                            with col3:
                                st.metric("❌ Errores", errores)
                            
                            if insertados > 0:
                                st.success(f"✅ Importación completada: {insertados} nuevos registros")
                                registrar_actividad(
                                    accion="IMPORT",
                                    modulo="PROVEEDORES",
                                    descripcion=f"Importación CSV: {insertados} insertados, {duplicados} duplicados, {errores} errores"
                                )
                            
                            if 'df_importar' in st.session_state:
                                del st.session_state.df_importar
                                del st.session_state.delimiter_usado
                                del st.session_state.encoding_usado
                                
                        except Exception as e:
                            st.error(f"Error durante la importación: {e}")

def eliminar_proveedor_bulk():
    """Función para eliminar múltiples proveedores (admin only)"""
    if st.session_state.user_role != 'admin':
        st.error("❌ Solo administradores pueden realizar eliminaciones masivas")
        return
    
    st.subheader("🗑️ Eliminación Masiva de Proveedores")
    st.warning("⚠️ **FUNCIÓN ADMINISTRATIVA** - Use con extrema precaución")
    
    try:
        with engine.connect() as conn:
            # Diagnóstico: mostrar el número de proveedores inactivos
            diag_query = text("""
                SELECT COUNT(*) 
                FROM reactivos_py.proveedores 
                WHERE activo = FALSE
            """)
            diag_result = conn.execute(diag_query)
            count_inactivos = diag_result.scalar()
            
            if count_inactivos > 0:
                st.info(f"Diagnóstico: Se encontraron {count_inactivos} proveedores inactivos en la base de datos")
            else:
                st.warning("⚠️ No hay proveedores inactivos en la base de datos.")
                st.info("Para inactivar un proveedor, vaya a la pestaña 'Lista de Proveedores', seleccione un proveedor y haga clic en 'Cambiar Estado'.")
                return
            
            # Obtener proveedores inactivos
            query = text("""
                SELECT p.id, p.ruc, p.razon_social, p.fecha_registro
                FROM reactivos_py.proveedores p
                WHERE p.activo = FALSE
                ORDER BY p.fecha_registro DESC
            """)
            
            result = conn.execute(query)
            proveedores_eliminables = [
                {'id': row[0], 'ruc': row[1], 'razon_social': row[2], 'fecha_registro': row[3]}
                for row in result
            ]
            
            if proveedores_eliminables:
                st.success(f"📊 Se encontraron {len(proveedores_eliminables)} proveedores inactivos que pueden eliminarse:")
                
                # Mostrar lista
                df_eliminables = pd.DataFrame(proveedores_eliminables)
                df_eliminables['fecha_registro'] = pd.to_datetime(df_eliminables['fecha_registro']).dt.strftime('%Y-%m-%d')
                df_eliminables_formateado = formatear_columnas_tabla(df_eliminables)
                st.dataframe(df_eliminables_formateado, use_container_width=True)
                
                # Opción de eliminación masiva
                if st.checkbox("Confirmo que deseo eliminar TODOS los proveedores inactivos listados"):
                    if st.button("🗑️ ELIMINAR TODOS LOS INACTIVOS", type="primary"):
                        try:
                            eliminados = 0
                            for proveedor in proveedores_eliminables:
                                query_delete = text("DELETE FROM reactivos_py.proveedores WHERE id = :id")
                                conn.execute(query_delete, {'id': proveedor['id']})
                                eliminados += 1
                            
                            # Registrar actividad masiva
                            registrar_actividad(
                                accion="DELETE",
                                modulo="PROVEEDORES",
                                descripcion=f"Eliminación masiva: {eliminados} proveedores inactivos eliminados",
                                detalles={
                                    'cantidad_eliminados': eliminados,
                                    'tipo': 'eliminacion_masiva_inactivos'
                                }
                            )
                            
                            st.success(f"✅ {eliminados} proveedores eliminados correctamente")
                            st.balloons()
                            time.sleep(2)
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"Error en eliminación masiva: {e}")
            else:
                st.warning("⚠️ No se encontraron proveedores inactivos para eliminar.")
                
    except Exception as e:
        st.error(f"Error obteniendo proveedores eliminables: {e}")

def pagina_administrar_usuarios():
    """Página para administrar usuarios"""
    st.header("Administrar Usuarios")
    
    # Pestañas para diferentes funciones
    tab1, tab2 = st.tabs(["Lista de Usuarios", "Crear Usuario"])
    
    with tab1:
        st.subheader("Usuarios del Sistema")
        
        # Obtener usuarios
        try:
            with engine.connect() as conn:
                query = text("""
                    SELECT id, cedula, username, nombre_completo, role, fecha_creacion, ultimo_cambio_password
                    FROM reactivos_py.usuarios
                    ORDER BY username
                """)
                
                result = conn.execute(query)
                
                usuarios = []
                for row in result:
                    usuarios.append({
                        'id': row[0],
                        'cedula': row[1],
                        'username': row[2],
                        'nombre_completo': row[3],
                        'role': row[4],
                        'fecha_creacion': row[5],
                        'ultimo_cambio_password': row[6]
                    })
                
                if usuarios:
                    # Convertir a DataFrame para mejor visualización
                    df_usuarios = pd.DataFrame(usuarios)
                    
                    # Dar formato a las fechas
                    if 'fecha_creacion' in df_usuarios.columns:
                        df_usuarios['fecha_creacion'] = pd.to_datetime(df_usuarios['fecha_creacion']).dt.strftime('%Y-%m-%d %H:%M')
                    if 'ultimo_cambio_password' in df_usuarios.columns:
                        df_usuarios['ultimo_cambio_password'] = pd.to_datetime(df_usuarios['ultimo_cambio_password']).dt.strftime('%Y-%m-%d %H:%M')
                    
                    # Formatear columnas para el usuario final
                    df_usuarios_formateado = formatear_columnas_tabla(df_usuarios)
                    
                    # Mostrar usuarios
                    st.dataframe(df_usuarios_formateado, use_container_width=True)
                    
                    # Selector para editar usuario
                    usuario_a_editar = st.selectbox(
                        "Seleccionar usuario para editar:",
                        options=[u['username'] for u in usuarios]
                    )
                    
                    usuario = next((u for u in usuarios if u['username'] == usuario_a_editar), None)
                    
                    if usuario:
                        with st.form("editar_usuario_form"):
                            st.subheader(f"Editar Usuario: {usuario['username']}")
                            
                            # Campos para editar
                            cedula = st.text_input("Cédula de Identidad:", value=usuario['cedula'])
                            nombre = st.text_input("Nombre completo:", value=usuario['nombre_completo'])
                            rol = st.selectbox(
                                "Rol:",
                                options=["admin", "user"],
                                index=0 if usuario['role'] == "admin" else 1
                            )
                            reset_password = st.checkbox("Resetear contraseña")
                            new_password = st.text_input("Nueva contraseña:", type="password") if reset_password else None
                            
                            # Botón para actualizar
                            submit = st.form_submit_button("Actualizar Usuario")
                            
                            if submit:
                                try:
                                    with engine.connect() as conn:
                                        # Iniciar transacción
                                        trans = conn.begin()
                                        try:
                                            # Verificar si la cédula ya existe en otro usuario
                                            if cedula != usuario['cedula']:
                                                query_check = text("""
                                                    SELECT COUNT(*) FROM reactivos_py.usuarios
                                                    WHERE cedula = :cedula AND id != :id
                                                """)
                                                result_check = conn.execute(query_check, {
                                                    'cedula': cedula,
                                                    'id': usuario['id']
                                                })
                                                count = result_check.scalar()
                                                
                                                if count > 0:
                                                    st.error(f"La cédula '{cedula}' ya está asignada a otro usuario.")
                                                    trans.rollback()
                                                    return
                                            
                                            # Actualizar usuario
                                            if reset_password and new_password:
                                                # Actualizar con nueva contraseña
                                                password_hash = hashlib.sha256(new_password.encode()).hexdigest()
                                                
                                                query = text("""
                                                    UPDATE reactivos_py.usuarios
                                                    SET cedula = :cedula, nombre_completo = :nombre, role = :rol, 
                                                        password = :password, ultimo_cambio_password = CURRENT_TIMESTAMP
                                                    WHERE id = :id
                                                """)
                                                
                                                conn.execute(query, {
                                                    'cedula': cedula,
                                                    'nombre': nombre,
                                                    'rol': rol,
                                                    'password': password_hash,
                                                    'id': usuario['id']
                                                })
                                            else:
                                                # Actualizar sin cambiar contraseña
                                                query = text("""
                                                    UPDATE reactivos_py.usuarios
                                                    SET cedula = :cedula, nombre_completo = :nombre, role = :rol
                                                    WHERE id = :id
                                                """)
                                                
                                                conn.execute(query, {
                                                    'cedula': cedula,
                                                    'nombre': nombre,
                                                    'rol': rol,
                                                    'id': usuario['id']
                                                })
                                            
                                            # Confirmar transacción
                                            trans.commit()
                                            
                                            st.success(f"Usuario {usuario['username']} actualizado correctamente")
                                            time.sleep(1)
                                            st.rerun()
                                            
                                        except Exception as e:
                                            # Revertir transacción en caso de error
                                            trans.rollback()
                                            raise e
                                            
                                except Exception as e:
                                    st.error(f"Error al actualizar usuario: {e}")
                else:
                    st.info("No hay usuarios para mostrar.")
        except Exception as e:
            st.error(f"Error al obtener usuarios: {e}")
    
    with tab2:
        st.subheader("Crear Nuevo Usuario")
        
        with st.form("nuevo_usuario_form"):
            cedula = st.text_input("Cédula de Identidad:")
            username = st.text_input("Nombre de usuario:")
            password = st.text_input("Contraseña:", type="password")
            nombre = st.text_input("Nombre completo:")
            rol = st.selectbox(
                "Rol:",
                options=["user", "admin"]
            )
            requiere_cambio = st.checkbox("Requerir cambio de contraseña en próximo inicio de sesión", value=True)
            
            submit = st.form_submit_button("Crear Usuario")
            
            if submit:
                if not cedula or not username or not password or not nombre:
                    st.error("Por favor, complete todos los campos.")
                else:
                    try:
                        # Verificar si el usuario o cédula ya existen
                        with engine.connect() as conn:
                            query = text("""
                                SELECT 
                                    (SELECT COUNT(*) FROM reactivos_py.usuarios WHERE username = :username) as count_username,
                                    (SELECT COUNT(*) FROM reactivos_py.usuarios WHERE cedula = :cedula) as count_cedula
                            """)
                            result = conn.execute(query, {'username': username, 'cedula': cedula})
                            counts = result.fetchone()
                            
                            if counts[0] > 0:
                                st.error(f"El nombre de usuario '{username}' ya existe.")
                            elif counts[1] > 0:
                                st.error(f"La cédula '{cedula}' ya está registrada.")
                            else:
                                # Crear nuevo usuario
                                password_hash = hashlib.sha256(password.encode()).hexdigest()
                                
                                query = text("""
                                    INSERT INTO reactivos_py.usuarios 
                                    (cedula, username, password, nombre_completo, role, ultimo_cambio_password)
                                    VALUES (:cedula, :username, :password, :nombre, :rol, 
                                           CASE WHEN :requiere_cambio THEN NULL ELSE CURRENT_TIMESTAMP END)
                                """)
                                
                                conn.execute(query, {
                                    'cedula': cedula,
                                    'username': username,
                                    'password': password_hash,
                                    'nombre': nombre,
                                    'rol': rol,
                                    'requiere_cambio': requiere_cambio
                                })
                                
                                st.success(f"Usuario '{username}' creado exitosamente")
                                time.sleep(1)
                                st.rerun()
                    except Exception as e:
                        st.error(f"Error al crear usuario: {e}")

def pagina_historial_actividades():
    """Página para mostrar el historial de actividades del sistema"""
    st.header("📋 Historial de Actividades del Sistema")
    
    # Filtros
    with st.expander("🔍 Filtros de Búsqueda", expanded=True):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            try:
                with engine.connect() as conn:
                    query = text("SELECT id, username, nombre_completo FROM reactivos_py.usuarios ORDER BY username")
                    result = conn.execute(query)
                    usuarios = [{'id': row[0], 'username': row[1], 'nombre': row[2]} for row in result]
                    
                usuario_filtro = st.selectbox(
                    "Filtrar por Usuario:",
                    options=[None] + [u['id'] for u in usuarios],
                    format_func=lambda x: "Todos los usuarios" if x is None else f"{next(u['username'] for u in usuarios if u['id'] == x)} ({next(u['nombre'] for u in usuarios if u['id'] == x)})"
                )
            except:
                usuario_filtro = None
        
        with col2:
            modulos = ["PROVEEDORES", "ORDENES_COMPRA", "USUARIOS", "LICITACIONES", "ARCHIVOS", "LOGIN"]
            modulo_filtro = st.selectbox(
                "Filtrar por Módulo:",
                options=[None] + modulos,
                format_func=lambda x: "Todos los módulos" if x is None else x
            )
        
        with col3:
            acciones = ["CREATE", "UPDATE", "DELETE", "LOGIN", "LOGOUT", "IMPORT", "EXPORT"]
            accion_filtro = st.selectbox(
                "Filtrar por Acción:",
                options=[None] + acciones,
                format_func=lambda x: "Todas las acciones" if x is None else x
            )
        
        col4, col5, col6 = st.columns(3)
        with col4:
            fecha_desde = st.date_input("Fecha desde:", value=None)
        with col5:
            fecha_hasta = st.date_input("Fecha hasta:", value=None)
        with col6:
            limite = st.number_input("Máximo registros:", min_value=10, max_value=1000, value=100, step=10)
    
    if st.button("🔍 Aplicar Filtros"):
        st.rerun()
    
    actividades = obtener_historial_actividades(
        limite=limite,
        usuario_id=usuario_filtro,
        modulo=modulo_filtro,
        accion=accion_filtro,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta
    )
    
    if actividades:
        st.subheader(f"📊 Mostrando {len(actividades)} actividades")
        
        df_actividades = pd.DataFrame(actividades)
        df_actividades['fecha_hora'] = pd.to_datetime(df_actividades['fecha_hora']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Formatear columnas usando la función de formateo
        df_actividades_formateado = formatear_columnas_tabla(df_actividades, {
            'fecha_hora': 'Fecha/Hora',
            'usuario': 'Usuario',
            'modulo': 'Módulo',
            'accion': 'Acción',
            'descripcion': 'Descripción',
            'esquema_afectado': 'Esquema Afectado'
        })
        
        # Seleccionar solo las columnas que queremos mostrar
        columnas_mostrar = ['Fecha/Hora', 'Usuario', 'Módulo', 'Acción', 'Descripción', 'Esquema Afectado']
        df_display = df_actividades_formateado[columnas_mostrar]
        
        def colorear_accion(val):
            if val == 'CREATE':
                return 'background-color: #d4edda; color: #155724'
            elif val == 'UPDATE':
                return 'background-color: #fff3cd; color: #856404'
            elif val == 'DELETE':
                return 'background-color: #f8d7da; color: #721c24'
            elif val == 'LOGIN':
                return 'background-color: #d1ecf1; color: #0c5460'
            else:
                return ''
        
        df_styled = df_display.style.applymap(colorear_accion, subset=['Acción'])
        st.dataframe(df_styled, use_container_width=True)
        
        st.subheader("🔍 Detalles de Actividad")
        actividad_seleccionada = st.selectbox(
            "Seleccionar actividad para ver detalles:",
            options=range(len(actividades)),
            format_func=lambda x: f"{actividades[x]['fecha_hora'].strftime('%Y-%m-%d %H:%M')} - {actividades[x]['usuario']} - {actividades[x]['descripcion']}"
        )
        
        if actividad_seleccionada is not None:
            actividad = actividades[actividad_seleccionada]
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Información General:**")
                st.write(f"• **Usuario:** {actividad['usuario']}")
                st.write(f"• **Fecha/Hora:** {actividad['fecha_hora'].strftime('%Y-%m-%d %H:%M:%S')}")
                st.write(f"• **Módulo:** {actividad['modulo']}")
                st.write(f"• **Acción:** {actividad['accion']}")
                st.write(f"• **Descripción:** {actividad['descripcion']}")
                if actividad['esquema_afectado']:
                    st.write(f"• **Esquema Afectado:** {actividad['esquema_afectado']}")
            
            with col2:
                if actividad['detalles']:
                    st.write("**Detalles Adicionales:**")
                    st.json(actividad['detalles'])
                
                if actividad['valores_anteriores'] or actividad['valores_nuevos']:
                    st.write("**Cambios Realizados:**")
                    
                    if actividad['valores_anteriores']:
                        st.write("*Valores Anteriores:*")
                        st.json(actividad['valores_anteriores'])
                    
                    if actividad['valores_nuevos']:
                        st.write("*Valores Nuevos:*")
                        st.json(actividad['valores_nuevos'])
        
        st.subheader("📤 Exportar Historial")
        if st.button("Descargar Historial como CSV"):
            csv = df_display.to_csv(index=False)
            st.download_button(
                label="📥 Descargar CSV",
                data=csv.encode('utf-8'),
                file_name=f"historial_actividades_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    else:
        st.info("No se encontraron actividades con los filtros aplicados.")
    
    # Estadísticas del sistema
    with st.expander("📊 Estadísticas del Sistema"):
        try:
            with engine.connect() as conn:
                # Actividades por usuario
                query = text("""
                    SELECT usuario_nombre, COUNT(*) as total
                    FROM reactivos_py.auditoria
                    GROUP BY usuario_nombre
                    ORDER BY total DESC
                    LIMIT 10
                """)
                result = conn.execute(query)
                
                st.write("**Top 10 Usuarios más Activos:**")
                for row in result:
                    st.write(f"• {row[0]}: {row[1]} actividades")
                
                # Actividades por módulo
                query = text("""
                    SELECT modulo, COUNT(*) as total
                    FROM reactivos_py.auditoria
                    GROUP BY modulo
                    ORDER BY total DESC
                """)
                result = conn.execute(query)
                
                st.write("**Actividades por Módulo:**")
                for row in result:
                    st.write(f"• {row[0]}: {row[1]} actividades")
                    
        except Exception as e:
            st.error(f"Error obteniendo estadísticas: {e}")

def pagina_cambiar_password():
    """Página para cambiar contraseña del usuario actual"""
    st.header("Cambiar Contraseña")
    
    # Verificar si el usuario debe cambiar su contraseña
    with engine.connect() as conn:
        query = text("""
            SELECT ultimo_cambio_password 
            FROM usuarios 
            WHERE id = :user_id
        """)
        
        result = conn.execute(query, {'user_id': st.session_state.user_id})
        ultimo_cambio = result.scalar()
        
        if ultimo_cambio is None:
            st.warning("⚠️ Se requiere cambiar su contraseña. Por favor, establezca una nueva contraseña para continuar.")
    
    with st.form("cambiar_password_form"):
        password_actual = st.text_input("Contraseña actual:", type="password")
        password_nueva = st.text_input("Nueva contraseña:", type="password")
        password_confirmar = st.text_input("Confirmar nueva contraseña:", type="password")
        
        # Agregar reglas de validación para contraseñas
        if password_nueva:
            col1, col2 = st.columns(2)
            with col1:
                if len(password_nueva) >= 8:
                    st.success("✅ Mínimo 8 caracteres")
                else:
                    st.error("❌ Mínimo 8 caracteres")
            
            with col2:
                if any(c.isdigit() for c in password_nueva):
                    st.success("✅ Al menos un número")
                else:
                    st.error("❌ Al menos un número")
        
        submit = st.form_submit_button("Cambiar Contraseña")
        
        if submit:
            if not password_actual or not password_nueva or not password_confirmar:
                st.error("Por favor, complete todos los campos.")
            elif password_nueva != password_confirmar:
                st.error("Las contraseñas no coinciden.")
            elif len(password_nueva) < 8:
                st.error("La contraseña debe tener al menos 8 caracteres.")
            elif not any(c.isdigit() for c in password_nueva):
                st.error("La contraseña debe contener al menos un número.")
            else:
                try:
                    # Verificar contraseña actual
                    password_hash_actual = hashlib.sha256(password_actual.encode()).hexdigest()
                    
                    with engine.connect() as conn:
                        query = text("""
                            SELECT COUNT(*) 
                            FROM usuarios 
                            WHERE id = :user_id AND password = :password
                        """)
                        
                        result = conn.execute(query, {
                            'user_id': st.session_state.user_id,
                            'password': password_hash_actual
                        })
                        
                        count = result.scalar()
                        
                        if count == 0:
                            st.error("La contraseña actual es incorrecta.")
                        else:
                            # Actualizar contraseña
                            password_hash_nueva = hashlib.sha256(password_nueva.encode()).hexdigest()
                            
                            query = text("""
                                UPDATE usuarios
                                SET password = :password, ultimo_cambio_password = CURRENT_TIMESTAMP
                                WHERE id = :user_id
                            """)
                            
                            conn.execute(query, {
                                'password': password_hash_nueva,
                                'user_id': st.session_state.user_id
                            })
                            
                            st.success("Contraseña cambiada exitosamente.")
                            
                            # Si se requería cambio de contraseña, actualizar el estado de la sesión
                            if 'requiere_cambio_password' in st.session_state and st.session_state.requiere_cambio_password:
                                st.session_state.requiere_cambio_password = False
                                st.info("Ya puede acceder a todas las funcionalidades del sistema.")
                                time.sleep(2)
                                st.rerun()
                except Exception as e:
                    st.error(f"Error al cambiar contraseña: {e}")

def obtener_datos_items(esquema, servicio=None):
    """Obtiene los datos de los items disponibles para generar órdenes de compra"""
    try:
        with engine.connect() as conn:
            # Consulta para obtener items disponibles
            query = text(f"""
                SELECT 
                    z."LOTE",
                    z."ITEM",
                    z."CODIGO DE REACTIVOS / INSUMOS",
                    z."CODIGO PARA SERVICIO BENEFICIARIO",
                    z."SERVICIO BENEFICIARIO",
                    z."DESCRIPCION DEL PRODUCTO // MARCA // PROCEDENCIA",
                    z."UNIDAD DE MEDIDA",
                    z."PRECIO UNITARIO",
                    z."REDISTRIBUCION (CANTIDAD MAXIMA)",
                    z."CANTIDAD EMITIDA",
                    z."SALDO A EMITIR"
                FROM "{esquema}"."ejecucion_por_zonas" z
                WHERE z."SALDO A EMITIR" > 0
            """)
            
            # Si se especifica un servicio, filtrar por ese servicio
            if servicio:
                query = text(f"""
                    SELECT 
                        z."LOTE",
                        z."ITEM",
                        z."CODIGO DE REACTIVOS / INSUMOS",
                        z."CODIGO PARA SERVICIO BENEFICIARIO",
                        z."SERVICIO BENEFICIARIO",
                        z."DESCRIPCION DEL PRODUCTO // MARCA // PROCEDENCIA",
                        z."UNIDAD DE MEDIDA",
                        z."PRECIO UNITARIO",
                        z."REDISTRIBUCION (CANTIDAD MAXIMA)",
                        z."CANTIDAD EMITIDA",
                        z."SALDO A EMITIR"
                    FROM "{esquema}"."ejecucion_por_zonas" z
                    WHERE z."SALDO A EMITIR" > 0
                    AND z."SERVICIO BENEFICIARIO" = :servicio
                """)
                result = conn.execute(query, {'servicio': servicio})
            else:
                result = conn.execute(query)
                
            items = []
            for row in result:
                items.append({
                    'lote': row[0],
                    'item': row[1],
                    'codigo_insumo': row[2],
                    'codigo_servicio': row[3],
                    'servicio': row[4],
                    'descripcion': row[5],
                    'unidad_medida': row[6],
                    'precio_unitario': row[7],
                    'cantidad_maxima': row[8],
                    'cantidad_emitida': row[9],
                    'saldo_emitir': row[10]
                })
            
            return items
    except Exception as e:
        st.error(f"Error obteniendo datos de items: {e}")
        return []

def obtener_servicios_beneficiarios(esquema):
    """Obtiene la lista de servicios beneficiarios para un esquema"""
    try:
        with engine.connect() as conn:
            query = text(f"""
                SELECT DISTINCT "SERVICIO BENEFICIARIO"
                FROM "{esquema}"."ejecucion_por_zonas"
                WHERE "SERVICIO BENEFICIARIO" IS NOT NULL
                ORDER BY "SERVICIO BENEFICIARIO"
            """)
            result = conn.execute(query)
            
            servicios = [row[0] for row in result]
            return servicios
    except Exception as e:
        st.error(f"Error obteniendo servicios beneficiarios: {e}")
        return []

def obtener_proximo_numero_oc(esquema):
    """Genera un número para la próxima orden de compra"""
    try:
        year = datetime.now().year
        month = datetime.now().month
        
        with engine.connect() as conn:
            # Consultar el número más alto actual
            query = text("""
                SELECT MAX(CAST(SUBSTRING(numero_orden FROM '^\\d+') AS INTEGER))
                FROM ordenes_compra 
                WHERE esquema = :esquema 
                AND EXTRACT(YEAR FROM fecha_emision) = :year
                AND EXTRACT(MONTH FROM fecha_emision) = :month
            """)
            
            result = conn.execute(query, {'esquema': esquema, 'year': year, 'month': month})
            max_num = result.scalar()
            
            if max_num is None:
                next_num = 1
            else:
                next_num = max_num + 1
            
            # Obtener datos del llamado para el número de OC
            query_llamado = text(f"""
                SELECT "NUMERO DE LLAMADO", "AÑO DEL LLAMADO"
                FROM "{esquema}"."llamado"
                LIMIT 1
            """)
            
            result_llamado = conn.execute(query_llamado)
            llamado_data = result_llamado.fetchone()
            
            if llamado_data:
                num_llamado = llamado_data[0]
                anho_llamado = llamado_data[1]
                
                # Formato: NNN/YYYY-LL/MM (donde NNN es correlativo, YYYY año actual, LL es número de llamado, MM es mes)
                numero_oc = f"{next_num:03d}/{year}-{num_llamado}/{month:02d}"
                return numero_oc
            else:
                # Formato alternativo si no hay datos de llamado
                numero_oc = f"{next_num:03d}/{year}-{month:02d}"
                return numero_oc
            
    except Exception as e:
        st.error(f"Error generando número de orden de compra: {e}")
        return f"{datetime.now().strftime('%Y%m%d%H%M%S')}"

def crear_orden_compra(esquema, numero_orden, fecha_emision, servicio_beneficiario, simese, items):
    """
    Crea una nueva orden de compra con sus items
    
    Args:
        esquema (str): Esquema de la licitación
        numero_orden (str): Número de la orden de compra
        fecha_emision (datetime): Fecha de emisión
        servicio_beneficiario (str): Servicio beneficiario
        simese (str): Número de SIMESE
        items (list): Lista de items para la orden de compra
    
    Returns:
        tuple: (success, message, orden_id)
    """
    try:
        with engine.connect() as conn:
            # Iniciar transacción
            trans = conn.begin()
            try:
                # Insertar cabecera de orden de compra
                query = text("""
                    INSERT INTO ordenes_compra 
                    (numero_orden, fecha_emision, esquema, servicio_beneficiario, simese, usuario_id, estado)
                    VALUES (:numero_orden, :fecha_emision, :esquema, :servicio_beneficiario, :simese, :usuario_id, 'Emitida')
                    RETURNING id
                """)
                
                result = conn.execute(query, {
                    'numero_orden': numero_orden,
                    'fecha_emision': fecha_emision,
                    'esquema': esquema,
                    'servicio_beneficiario': servicio_beneficiario,
                    'simese': simese,
                    'usuario_id': st.session_state.user_id
                })
                
                orden_id = result.scalar()
                
                # Insertar items de la orden
                for item in items:
                    monto_total = item['cantidad'] * item['precio_unitario']
                    
                    query_item = text("""
                        INSERT INTO items_orden_compra
                        (orden_compra_id, lote, item, codigo_insumo, codigo_servicio, 
                         descripcion, cantidad, unidad_medida, precio_unitario, monto_total, observaciones)
                        VALUES
                        (:orden_id, :lote, :item, :codigo_insumo, :codigo_servicio,
                         :descripcion, :cantidad, :unidad_medida, :precio_unitario, :monto_total, :observaciones)
                    """)
                    
                    conn.execute(query_item, {
                        'orden_id': orden_id,
                        'lote': item['lote'],
                        'item': item['item'],
                        'codigo_insumo': item['codigo_insumo'],
                        'codigo_servicio': item['codigo_servicio'],
                        'descripcion': item['descripcion'],
                        'cantidad': item['cantidad'],
                        'unidad_medida': item['unidad_medida'],
                        'precio_unitario': item['precio_unitario'],
                        'monto_total': monto_total,
                        'observaciones': item.get('observaciones', '')
                    })
                    
                    # Actualizar cantidad emitida en la tabla de ejecución por zonas
                    query_update = text(f"""
                        UPDATE "{esquema}"."ejecucion_por_zonas"
                        SET "CANTIDAD EMITIDA" = "CANTIDAD EMITIDA" + :cantidad,
                            "SALDO A EMITIR" = "REDISTRIBUCION (CANTIDAD MAXIMA)" - ("CANTIDAD EMITIDA" + :cantidad),
                            "PORCENTAJE EMITIDO POR SERVICIO SANITARIO" = 
                                (("CANTIDAD EMITIDA" + :cantidad) / "REDISTRIBUCION (CANTIDAD MAXIMA)") * 100
                        WHERE "LOTE" = :lote 
                        AND "ITEM" = :item
                        AND "SERVICIO BENEFICIARIO" = :servicio
                    """)
                    
                    conn.execute(query_update, {
                        'cantidad': item['cantidad'],
                        'lote': item['lote'],
                        'item': item['item'],
                        'servicio': servicio_beneficiario
                    })
                    
                    # También actualizar la tabla de ejecución general
                    query_update_general = text(f"""
                        UPDATE "{esquema}"."ejecucion_general"
                        SET "CANTIDAD EMITIDA" = "CANTIDAD EMITIDA" + :cantidad,
                            "SALDO A EMITIR" = "REDISTRIBUCION (CANTIDAD MAXIMA)" - ("CANTIDAD EMITIDA" + :cantidad),
                            "PORCENTAJE EMITIDO" = 
                                (("CANTIDAD EMITIDA" + :cantidad) / "REDISTRIBUCION (CANTIDAD MAXIMA)") * 100
                        WHERE "LOTE" = :lote 
                        AND "ITEM" = :item
                    """)
                    
                    conn.execute(query_update_general, {
                        'cantidad': item['cantidad'],
                        'lote': item['lote'],
                        'item': item['item']
                    })
                
                # Actualizar también la tabla orden_de_compra del esquema
                for item in items:
                    query_insert_oc = text(f"""
                        INSERT INTO "{esquema}"."orden_de_compra"
                        ("SIMESE (PEDIDO)", "N° ORDEN DE COMPRA", "FECHA DE EMISION",
                        "CODIGO DE REACTIVOS / INSUMOS + CODIGO DE SERVICIO BENEFICIARIO",
                        "CODIGO DE REACTIVOS / INSUMOS", "SERVICIO BENEFICIARIO",
                        "LOTE", "ITEM", "CANTIDAD SOLICITADA", "UNIDAD DE MEDIDA",
                        "DESCRIPCION DEL PRODUCTO // MARCA // PROCEDENCIA", "PRECIO UNITARIO",
                        "MONTO EMITIDO", "Observaciones")
                        VALUES
                        (:simese, :numero_orden, :fecha_emision,
                        :codigo_completo, :codigo_insumo, :servicio,
                        :lote, :item, :cantidad, :unidad_medida,
                        :descripcion, :precio_unitario,
                        :monto_total, :observaciones)
                    """)
                    
                    codigo_completo = f"{item['codigo_insumo']}{item['codigo_servicio']}" if item['codigo_servicio'] else item['codigo_insumo']
                    
                    conn.execute(query_insert_oc, {
                        'simese': simese,
                        'numero_orden': numero_orden,
                        'fecha_emision': fecha_emision,
                        'codigo_completo': codigo_completo,
                        'codigo_insumo': item['codigo_insumo'],
                        'servicio': servicio_beneficiario,
                        'lote': item['lote'],
                        'item': item['item'],
                        'cantidad': item['cantidad'],
                        'unidad_medida': item['unidad_medida'],
                        'descripcion': item['descripcion'],
                        'precio_unitario': item['precio_unitario'],
                        'monto_total': item['cantidad'] * item['precio_unitario'],
                        'observaciones': item.get('observaciones', '')
                    })
                
                # Confirmar transacción
                trans.commit()
                registrar_actividad(
                    accion="CREATE",
                    modulo="ORDENES_COMPRA",
                    descripcion=f"Orden de compra {numero_orden} creada",
                    esquema_afectado=esquema
                )
                return True, "Orden de compra creada exitosamente", orden_id
                
            except Exception as e:
                # Revertir transacción en caso de error
                trans.rollback()
                raise e
                
    except Exception as e:
        return False, f"Error al crear orden de compra: {e}", None

def obtener_ordenes_compra(esquema=None):
    """Obtiene las órdenes de compra existentes, filtradas por esquema si se especifica"""
    try:
        with engine.connect() as conn:
            # Consulta base
            query_base = """
                SELECT oc.id, oc.numero_orden, oc.fecha_emision, oc.esquema, 
                       oc.servicio_beneficiario, oc.simese, oc.estado, 
                       u.username as usuario, oc.fecha_creacion,
                       COUNT(ioc.id) as cantidad_items,
                       SUM(ioc.monto_total) as monto_total
                FROM reactivos_py.ordenes_compra oc
                JOIN reactivos_py.usuarios u ON oc.usuario_id = u.id
                LEFT JOIN reactivos_py.items_orden_compra ioc ON oc.id = ioc.orden_compra_id
            """
            
            # Agregar filtro por esquema si es necesario
            if esquema:
                query_base += " WHERE oc.esquema = :esquema "
                params = {'esquema': esquema}
            else:
                params = {}
            
            # Agrupar y ordenar
            query_base += """
                GROUP BY oc.id, oc.numero_orden, oc.fecha_emision, oc.esquema, 
                         oc.servicio_beneficiario, oc.simese, oc.estado, 
                         u.username, oc.fecha_creacion
                ORDER BY oc.fecha_creacion DESC
            """
            
            query = text(query_base)
            result = conn.execute(query, params)
            
            ordenes = []
            for row in result:
                ordenes.append({
                    'id': row[0],
                    'numero_orden': row[1],
                    'fecha_emision': row[2],
                    'esquema': row[3],
                    'servicio_beneficiario': row[4],
                    'simese': row[5],
                    'estado': row[6],
                    'usuario': row[7],
                    'fecha_creacion': row[8],
                    'cantidad_items': row[9],
                    'monto_total': row[10]
                })
            
            return ordenes
    except Exception as e:
        st.error(f"Error obteniendo órdenes de compra: {e}")
        return []

def obtener_detalles_orden_compra(orden_id):
    """Obtiene los detalles completos de una orden de compra"""
    try:
        with engine.connect() as conn:
            # Obtener cabecera
            query_cabecera = text("""
                SELECT oc.id, oc.numero_orden, oc.fecha_emision, oc.esquema, 
                       oc.servicio_beneficiario, oc.simese, oc.estado, 
                       u.username as usuario, oc.fecha_creacion,
                       u.nombre_completo as usuario_nombre
                FROM ordenes_compra oc
                JOIN usuarios u ON oc.usuario_id = u.id
                WHERE oc.id = :orden_id
            """)
            
            result = conn.execute(query_cabecera, {'orden_id': orden_id})
            cabecera = result.fetchone()
            
            if not cabecera:
                return None
            
            # Obtener items
            query_items = text("""
                SELECT id, lote, item, codigo_insumo, codigo_servicio, 
                       descripcion, cantidad, unidad_medida, precio_unitario, 
                       monto_total, observaciones
                FROM items_orden_compra
                WHERE orden_compra_id = :orden_id
                ORDER BY lote, item
            """)
            
            result_items = conn.execute(query_items, {'orden_id': orden_id})
            
            items = []
            for row in result_items:
                items.append({
                    'id': row[0],
                    'lote': row[1],
                    'item': row[2],
                    'codigo_insumo': row[3],
                    'codigo_servicio': row[4],
                    'descripcion': row[5],
                    'cantidad': row[6],
                    'unidad_medida': row[7],
                    'precio_unitario': row[8],
                    'monto_total': row[9],
                    'observaciones': row[10]
                })
            
            # Obtener datos de la licitación desde el esquema
            query_licitacion = text(f"""
                SELECT "NUMERO DE LLAMADO", "AÑO DEL LLAMADO", "NOMBRE DEL LLAMADO", 
                       "EMPRESA ADJUDICADA", "FECHA DE FIRMA DEL CONTRATO", 
                       "N° de Contrato / Año", "Vigencia del Contrato"
                FROM "{cabecera[3]}"."llamado"
                LIMIT 1
            """)
            
            result_licitacion = conn.execute(query_licitacion)
            licitacion = result_licitacion.fetchone()
            
            # Armar respuesta completa
            orden = {
                'id': cabecera[0],
                'numero_orden': cabecera[1],
                'fecha_emision': cabecera[2],
                'esquema': cabecera[3],
                'servicio_beneficiario': cabecera[4],
                'simese': cabecera[5],
                'estado': cabecera[6],
                'usuario': cabecera[7],
                'fecha_creacion': cabecera[8],
                'usuario_nombre': cabecera[9],
                'items': items,
                'monto_total': sum(item['monto_total'] for item in items),
                'cantidad_items': len(items)
            }
            
            # Agregar datos de licitación si están disponibles
            if licitacion:
                orden['licitacion'] = {
                    'numero_llamado': licitacion[0],
                    'anio_llamado': licitacion[1],
                    'nombre_llamado': licitacion[2],
                    'empresa_adjudicada': licitacion[3],
                    'fecha_contrato': licitacion[4],
                    'numero_contrato': licitacion[5],
                    'vigencia_contrato': licitacion[6]
                }
            
            return orden
    except Exception as e:
        st.error(f"Error obteniendo detalles de orden de compra: {e}")
        return None

def cambiar_estado_orden_compra(orden_id, nuevo_estado):
    """Cambia el estado de una orden de compra"""
    try:
        with engine.connect() as conn:
            query = text("""
                UPDATE ordenes_compra
                SET estado = :estado
                WHERE id = :orden_id
                RETURNING numero_orden
            """)
            
            result = conn.execute(query, {'estado': nuevo_estado, 'orden_id': orden_id})
            numero_orden = result.scalar()
            
            if numero_orden:
                return True, f"Estado de orden {numero_orden} cambiado a '{nuevo_estado}'"
            else:
                return False, "Orden de compra no encontrada"
    except Exception as e:
        return False, f"Error al cambiar estado: {e}"

def generar_pdf_orden_compra(orden_id):
    """
    Genera un PDF para la orden de compra
    
    Esta función es un placeholder. En la implementación real, deberías usar 
    una biblioteca como reportlab, weasyprint o pdfkit para generar el PDF.
    
    Returns:
        bytes: Contenido del PDF
    """
    try:
        orden = obtener_detalles_orden_compra(orden_id)
        if not orden:
            return None, "Orden no encontrada"
            
        # Placeholder: En una implementación real, aquí generarías el PDF
        # Por ahora, solo devolvemos un mensaje indicando que esta función debe implementarse
        return None, "La generación de PDF debe implementarse utilizando una biblioteca como reportlab o weasyprint"
    except Exception as e:
        return None, f"Error generando PDF: {e}"

def pagina_ordenes_compra():
    """Página principal de gestión de órdenes de compra"""
    st.header("Gestión de Órdenes de Compra")
    
    # Obtener esquemas existentes
    esquemas = obtener_esquemas_postgres()

    # Pestañas para diferentes funciones
    tab1, tab2 = st.tabs(["Lista de Órdenes", "Emitir Nueva Orden"])
    
    # [Código existente para tab1]...
    
    with tab2:
        st.subheader("Emitir Nueva Orden de Compra")
        
        # Selector de esquema (licitación)
        esquema_seleccionado = st.selectbox(
            "Seleccionar Licitación:",
            options=esquemas
        )
        
        if esquema_seleccionado:
            # Obtener información de la licitación
            with engine.connect() as conn:
                try:
                    query = text(f"""
                        SELECT "NUMERO DE LLAMADO", "AÑO DEL LLAMADO", "NOMBRE DEL LLAMADO", 
                               "EMPRESA ADJUDICADA"
                        FROM "{esquema_seleccionado}"."llamado"
                        LIMIT 1
                    """)
                    result = conn.execute(query)
                    licitacion = result.fetchone()
                    
                    if licitacion:
                        st.write(f"**Licitación:** {licitacion[0]}/{licitacion[1]} - {licitacion[2]}")
                        st.write(f"**Empresa:** {licitacion[3]}")
                except Exception as e:
                    st.error(f"Error obteniendo datos de licitación: {e}")
            
            # Obtener lista de servicios beneficiarios
            servicios = obtener_servicios_beneficiarios(esquema_seleccionado)
            
            if servicios:
                servicio_seleccionado = st.selectbox(
                    "Seleccionar Servicio Beneficiario:",
                    options=servicios
                )
                
                # Formulario para los datos de la orden de compra
                with st.form("nueva_orden_form"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Generar número de orden sugerido
                        numero_sugerido = obtener_proximo_numero_oc(esquema_seleccionado)
                        numero_orden = st.text_input("Número de Orden:", value=numero_sugerido)
                    
                    with col2:
                        fecha_emision = st.date_input(
                            "Fecha de Emisión:",
                            value=datetime.now()
                        )
                    
                    simese = st.text_input("Número de SIMESE (Pedido):")
                    
                    st.subheader("Selección de Items")
                    
                    # Obtener items disponibles para el servicio seleccionado
                    items_disponibles = obtener_datos_items(esquema_seleccionado, servicio_seleccionado)
                    
                    if not items_disponibles:
                        st.warning(f"No hay items disponibles para el servicio '{servicio_seleccionado}'")
                        submit_disabled = True
                    else:
                        # Mostrar items disponibles en una tabla más completa
                        df_items = pd.DataFrame(items_disponibles)
                        
                        # Agregar columnas de ejecución
                        if 'saldo_emitir' in df_items.columns and 'cantidad_maxima' in df_items.columns:
                            df_items['porcentaje_ejecucion_servicio'] = (1 - (df_items['saldo_emitir'] / df_items['cantidad_maxima'])) * 100
                            df_items['porcentaje_ejecucion_servicio'] = df_items['porcentaje_ejecucion_servicio'].round(2)
                        
                        # Formatear para mejor visualización
                        df_display = df_items.copy()
                        if 'precio_unitario' in df_display.columns:
                            df_display['precio_unitario'] = df_display['precio_unitario'].apply(lambda x: f"₲ {x:,.0f}".replace(",", "."))
                        
                        st.dataframe(df_display)
                        
                        # Inicializar lista de items seleccionados si no existe
                        if 'items_seleccionados' not in st.session_state:
                            st.session_state.items_seleccionados = []
                        
                        # Selector para agregar un item
                        items_opciones = {f"{i['lote'] or '-'}-{i['item']} | {i['descripcion'][:40]}..." if len(i['descripcion']) > 40 else f"{i['lote'] or '-'}-{i['item']} | {i['descripcion']}" for i in items_disponibles}
                        item_seleccionado = st.selectbox(
                            "Seleccionar Item para agregar:",
                            options=list(items_opciones.keys())
                        )
                        
                        # Encontrar el ítem seleccionado
                        item_seleccionado_info = next((i for i in items_disponibles if 
                                                   f"{i['lote'] or '-'}-{i['item']} | {i['descripcion'][:40]}..." == item_seleccionado or
                                                   f"{i['lote'] or '-'}-{i['item']} | {i['descripcion']}" == item_seleccionado), None)
                        
                        if item_seleccionado_info:
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                cantidad = st.number_input(
                                    "Cantidad:",
                                    min_value=0.01,
                                    max_value=float(item_seleccionado_info['saldo_emitir']),
                                    value=min(1.0, float(item_seleccionado_info['saldo_emitir'])),
                                    step=0.01,
                                    format="%.2f"
                                )
                            
                            with col2:
                                # Determinar si se puede agregar cantidad complementaria (20%)
                                puede_complementario = True  # Lógica para determinar si puede tener complementario
                                if puede_complementario:
                                    usar_complementario = st.checkbox("Agregar cantidad complementaria (20%)")
                                    cantidad_complementaria = cantidad * 0.2 if usar_complementario else 0
                                else:
                                    usar_complementario = False
                                    cantidad_complementaria = 0
                            
                            with col3:
                                precio = st.number_input(
                                    "Precio Unitario:",
                                    min_value=0.01,
                                    value=float(item_seleccionado_info['precio_unitario']),
                                    step=0.01,
                                    format="%.2f",
                                    disabled=True
                                )
                            
                            # Calcular subtotal
                            subtotal = (cantidad + cantidad_complementaria) * precio
                            st.write(f"**Subtotal:** ₲ {subtotal:,.0f}".replace(",", "."))
                            
                            observaciones = st.text_area("Observaciones:", height=100)
                            
                            # Botón para agregar item a la lista
                            agregar_item = st.form_submit_button("Agregar Item")
                            
                            if agregar_item:
                                # Crear item para agregar
                                nuevo_item = {
                                    'lote': item_seleccionado_info['lote'],
                                    'item': item_seleccionado_info['item'],
                                    'codigo_insumo': item_seleccionado_info['codigo_insumo'],
                                    'codigo_servicio': item_seleccionado_info['codigo_servicio'],
                                    'descripcion': item_seleccionado_info['descripcion'],
                                    'cantidad': cantidad,
                                    'cantidad_complementaria': cantidad_complementaria,
                                    'cantidad_total': cantidad + cantidad_complementaria,
                                    'unidad_medida': item_seleccionado_info['unidad_medida'],
                                    'precio_unitario': float(item_seleccionado_info['precio_unitario']),
                                    'monto_total': subtotal,
                                    'observaciones': observaciones,
                                    'saldo_emitir': float(item_seleccionado_info['saldo_emitir']),
                                    'porcentaje_ejecucion_servicio': float(item_seleccionado_info.get('porcentaje_ejecucion_servicio', 0)),
                                    'porcentaje_ejecucion_global': float(item_seleccionado_info.get('porcentaje_ejecucion_global', 0))
                                }
                                
                                # Verificar que no se haya agregado ya
                                item_existe = any(
                                    i['lote'] == nuevo_item['lote'] and 
                                    i['item'] == nuevo_item['item'] 
                                    for i in st.session_state.items_seleccionados
                                )
                                
                                if item_existe:
                                    st.error(f"El item {nuevo_item['lote'] or '-'}-{nuevo_item['item']} ya fue agregado.")
                                else:
                                    st.session_state.items_seleccionados.append(nuevo_item)
                                    st.success(f"Item {nuevo_item['lote'] or '-'}-{nuevo_item['item']} agregado a la orden.")
                                    st.rerun()
                        
                        # Mostrar items seleccionados
                        if st.session_state.items_seleccionados:
                            st.subheader("Items Seleccionados")
                            
                            # Crear DataFrame para visualización
                            df_seleccionados = pd.DataFrame(st.session_state.items_seleccionados)
                            
                            # Formatear para mejor visualización
                            df_display = df_seleccionados.copy()
                            df_display['precio_unitario'] = df_display['precio_unitario'].apply(lambda x: f"₲ {x:,.0f}".replace(",", "."))
                            df_display['monto_total'] = df_display['monto_total'].apply(lambda x: f"₲ {x:,.0f}".replace(",", "."))
                            
                            # Mostrar DataFrame
                            st.dataframe(df_display)
                            
                            # Mostrar monto total
                            monto_total = sum(item['monto_total'] for item in st.session_state.items_seleccionados)
                            st.subheader(f"Monto Total: ₲ {monto_total:,.0f}".replace(",", "."))
                            
                            # Mostrar vista previa del PDF
                            with st.expander("Vista previa de la Orden de Compra"):
                                # Crear una vista previa del PDF como HTML
                                html_preview = f"""
                                <div style="border: 1px solid #ddd; padding: 20px; font-family: Arial, sans-serif;">
                                    <div style="text-align: center; margin-bottom: 20px;">
                                        <h3>GOBIERNO NACIONAL</h3>
                                        <h4>Ministerio de Salud Pública y Bienestar Social</h4>
                                        <h2>ORDEN DE COMPRA N° {numero_orden}</h2>
                                    </div>
                                    
                                    <div style="display: flex; justify-content: space-between; margin-bottom: 20px;">
                                        <div>
                                            <p><strong>Señores:</strong> {licitacion[3]}</p>
                                            <p><strong>Licitación:</strong> {licitacion[0]}/{licitacion[1]} - {licitacion[2]}</p>
                                        </div>
                                        <div>
                                            <p><strong>Fecha de Emisión:</strong> {fecha_emision.strftime('%d/%m/%Y')}</p>
                                            <p><strong>SIMESE:</strong> {simese}</p>
                                        </div>
                                    </div>
                                    
                                    <p><strong>Servicio Beneficiario:</strong> {servicio_seleccionado}</p>
                                    
                                    <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
                                        <thead>
                                            <tr style="background-color: #f2f2f2;">
                                                <th style="border: 1px solid #ddd; padding: 8px; text-align: center;">Lote</th>
                                                <th style="border: 1px solid #ddd; padding: 8px; text-align: center;">Item</th>
                                                <th style="border: 1px solid #ddd; padding: 8px; text-align: center;">Cantidad</th>
                                                <th style="border: 1px solid #ddd; padding: 8px; text-align: center;">Cantidad Comp.</th>
                                                <th style="border: 1px solid #ddd; padding: 8px; text-align: center;">Unidad</th>
                                                <th style="border: 1px solid #ddd; padding: 8px; text-align: center;">Descripción</th>
                                                <th style="border: 1px solid #ddd; padding: 8px; text-align: center;">Ejecución</th>
                                                <th style="border: 1px solid #ddd; padding: 8px; text-align: center;">Precio Unit.</th>
                                                <th style="border: 1px solid #ddd; padding: 8px; text-align: center;">Subtotal</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                """
                                
                                for item in st.session_state.items_seleccionados:
                                    lote_display = item['lote'] if item['lote'] else "-"
                                    cantidad_comp_display = f"{item['cantidad_complementaria']:.2f}" if item['cantidad_complementaria'] > 0 else "-"
                                    ejecucion_display = f"{item['porcentaje_ejecucion_servicio']:.1f}% / {item['porcentaje_ejecucion_global']:.1f}%"
                                    
                                    html_preview += f"""
                                        <tr>
                                            <td style="border: 1px solid #ddd; padding: 8px; text-align: center;">{lote_display}</td>
                                            <td style="border: 1px solid #ddd; padding: 8px; text-align: center;">{item['item']}</td>
                                            <td style="border: 1px solid #ddd; padding: 8px; text-align: right;">{item['cantidad']:.2f}</td>
                                            <td style="border: 1px solid #ddd; padding: 8px; text-align: right;">{cantidad_comp_display}</td>
                                            <td style="border: 1px solid #ddd; padding: 8px; text-align: center;">{item['unidad_medida']}</td>
                                            <td style="border: 1px solid #ddd; padding: 8px; text-align: left;">{item['descripcion']}</td>
                                            <td style="border: 1px solid #ddd; padding: 8px; text-align: center;">{ejecucion_display}</td>
                                            <td style="border: 1px solid #ddd; padding: 8px; text-align: right;">₲ {item['precio_unitario']:,.0f}</td>
                                            <td style="border: 1px solid #ddd; padding: 8px; text-align: right;">₲ {item['monto_total']:,.0f}</td>
                                        </tr>
                                    """.replace(",", ".")
                                
                                html_preview += f"""
                                        </tbody>
                                        <tfoot>
                                            <tr>
                                                <td colspan="8" style="border: 1px solid #ddd; padding: 8px; text-align: right;"><strong>TOTAL</strong></td>
                                                <td style="border: 1px solid #ddd; padding: 8px; text-align: right;"><strong>₲ {monto_total:,.0f}</strong></td>
                                            </tr>
                                        </tfoot>
                                    </table>
                                    
                                    <p><strong>Son Guaraníes:</strong> {numero_a_letras(monto_total)}</p>
                                    
                                    <div style="margin-top: 50px; display: flex; justify-content: space-between;">
                                        <div style="text-align: center; width: 30%;">
                                            <div style="border-top: 1px solid #000; padding-top: 10px;">
                                                Director Administrativo
                                            </div>
                                        </div>
                                        <div style="text-align: center; width: 30%;">
                                            <div style="border-top: 1px solid #000; padding-top: 10px;">
                                                Director General
                                            </div>
                                        </div>
                                    </div>
                                </div>
                                """
                                
                                st.components.v1.html(html_preview, height=600)
                            
                            # Botón para limpiar lista
                            if st.form_submit_button("Limpiar Lista"):
                                st.session_state.items_seleccionados = []
                                st.rerun()
                        
                        submit_disabled = len(st.session_state.items_seleccionados) == 0
                    
                    # Botón para emitir orden
                    submit = st.form_submit_button("Emitir Orden de Compra", disabled=submit_disabled)
                    
                    if submit and st.session_state.items_seleccionados:
                        # Validar datos
                        if not numero_orden:
                            st.error("Debe ingresar un número de orden.")
                        elif not simese:
                            st.error("Debe ingresar un número de SIMESE.")
                        else:
                            # Crear orden de compra
                            success, message, orden_id = crear_orden_compra(
                                esquema_seleccionado,
                                numero_orden,
                                fecha_emision,
                                servicio_seleccionado,
                                simese,
                                st.session_state.items_seleccionados
                            )
                            
                            if success:
                                st.success(message)
                                # Limpiar estado
                                st.session_state.items_seleccionados = []
                                # Mostrar botón para ver la orden
                                if st.button("Ver Orden Creada"):
                                    st.session_state.orden_seleccionada = orden_id
                                    st.session_state.menu = "ordenes_compra"
                                    st.rerun()
                            else:
                                st.error(message)
            else:
                st.warning(f"No hay servicios beneficiarios definidos para la licitación seleccionada.")
        else:
            st.info("Seleccione una licitación para emitir una orden de compra.")

def pagina_dashboard():
    """Página de resumen/dashboard"""
    st.header("Dashboard")
    
    # Mostrar información resumida
    col1, col2, col3 = st.columns(3)
    
    try:
        # Iniciar actualización automática
        iniciar_actualizacion_automatica()
        
        # Obtener cantidad de esquemas
        esquemas = obtener_esquemas_postgres()
        # Filtrar reactivos_py del conteo de esquemas
        esquemas = [e for e in esquemas if e != 'reactivos_py']
        
        with col1:
            st.metric(
                label="Total Licitaciones", 
                value=len(esquemas)
            )
        
        # En lugar de obtener órdenes de compra directamente (lo que causa error),
        # mostrar información alternativa o un mensaje
        with col2:
            try:
                # Intentar obtener órdenes de compra del esquema correcto
                with engine.connect() as conn:
                    query = text("""
                        SELECT COUNT(*) 
                        FROM reactivos_py.ordenes_compra
                    """)
                    result = conn.execute(query)
                    count = result.scalar() or 0
                    st.metric(
                        label="Órdenes de Compra", 
                        value=count
                    )
            except Exception:
                # Si hay error, solo mostrar 0
                st.metric(
                    label="Órdenes de Compra", 
                    value="0"
                )
        
        with col3:
            try:
                # Intentar obtener monto total de órdenes de compra
                with engine.connect() as conn:
                    query = text("""
                        SELECT SUM(monto_total) 
                        FROM reactivos_py.items_orden_compra
                    """)
                    result = conn.execute(query)
                    monto_total = result.scalar() or 0
                    st.metric(
                        label="Monto Total Emitido", 
                        value=f"₲ {monto_total:,.0f}".replace(",", ".")
                    )
            except Exception:
                # Si hay error, solo mostrar 0
                st.metric(
                    label="Monto Total Emitido", 
                    value="₲ 0"
                )
        
        # Mostrar gráficos
        st.subheader("Esquemas Disponibles")
        
        if esquemas:
            # Crear un gráfico simple de los esquemas
            datos_grafico = pd.DataFrame({
                'Esquema': esquemas,
                'Cantidad': [1] * len(esquemas)  # Solo para visualización
            })
            
            st.bar_chart(datos_grafico.set_index('Esquema'))
            
            # Lista de esquemas disponibles
            st.subheader("Licitaciones Disponibles")
            for i, esquema in enumerate(esquemas, 1):
                st.write(f"{i}. {esquema}")
            
            # Botón para ir a órdenes de compra
            if st.button("Gestionar Archivos"):
                # Cambiar a la página de cargas
                st.session_state.menu = "cargar_archivo"
                st.rerun()
        else:
            st.info("No hay licitaciones disponibles. Por favor, cargue un archivo primero.")
        
    except Exception as e:
        st.error(f"Error al cargar el dashboard: {e}")

def main():
    st.set_page_config(
        page_title="Gestión de Licitaciones",
        page_icon="📊",
        layout="wide"
    )
    
    # Configurar tablas si no existen
    configurar_tabla_usuarios()
    configurar_tabla_ordenes_compra()
    configurar_tabla_cargas()
    configurar_tabla_proveedores()
    configurar_tabla_auditoria()
    
    # Inicializar el estado de sesión si es necesario
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
    
    # Verificar si el usuario está autenticado
    if not st.session_state.logged_in:
        pagina_login()
        return
    
    # Verificar si el usuario requiere cambio de contraseña
    if 'requiere_cambio_password' in st.session_state and st.session_state.requiere_cambio_password:
        pagina_cambiar_password()
        return
    
    # Si llega aquí, el usuario está autenticado
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        st.sidebar.success(f"✅ Conectado a PostgreSQL | Usuario: {st.session_state.username}")
        
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
    st.title("Sistema de Gestión de Llamados Reactivos")
    
    # Opciones de menú según el rol (SIN historial_actividades por ahora)
    if st.session_state.user_role == 'admin':
        menu_options = {
            "dashboard": "📈 Dashboard",
            "cargar_archivo": "📥 Cargar Archivo", 
            "ver_cargas": "📋 Ver Cargas",
            "ordenes_compra": "📝 Órdenes de Compra",
            "gestionar_proveedores": "🏭 Gestión de Proveedores",
            "eliminar_esquemas": "🗑️ Eliminar Esquemas",
            "admin_usuarios": "👥 Administrar Usuarios",
            "cambiar_password": "🔑 Cambiar Contraseña",
            "logout": "🚪 Cerrar Sesión"
        }
    else:
        menu_options = {
            "dashboard": "📈 Dashboard",
            "cargar_archivo": "📥 Cargar Archivo", 
            "ver_cargas": "📋 Ver Cargas",
            "ordenes_compra": "📝 Órdenes de Compra",
            "gestionar_proveedores": "🏭 Gestión de Proveedores",
            "historial_actividades": "📋 Historial de Actividades",
            "cambiar_password": "🔑 Cambiar Contraseña",
            "logout": "🚪 Cerrar Sesión"
        }
    
    # Crear menú de navegación
    menu = st.sidebar.radio(
        "Menú de Navegación", 
        list(menu_options.keys()),
        format_func=lambda x: menu_options[x]
    )
    
    # Mostrar la página seleccionada (SIN elif historial_actividades)

    if menu == "dashboard":
        pagina_dashboard()
    elif menu == "cargar_archivo":
        pagina_cargar_archivo()
    elif menu == "ver_cargas":
        pagina_ver_cargas()
    elif menu == "ordenes_compra":
        pagina_ordenes_compra()
    elif menu == "gestionar_proveedores":
        pagina_gestionar_proveedores()
    elif menu == "eliminar_esquemas" and st.session_state.user_role == 'admin':
        pagina_eliminar_esquemas()
    elif menu == "admin_usuarios" and st.session_state.user_role == 'admin':
        pagina_administrar_usuarios()
    elif menu == "historial_actividades":
            pagina_historial_actividades()
    elif menu == "cambiar_password":
        pagina_cambiar_password()
    elif menu == "logout":
        # Cerrar sesión
        st.session_state.logged_in = False
        st.session_state.user_id = None
        st.session_state.user_role = None
        st.session_state.username = None
        st.success("Sesión cerrada correctamente. Redirigiendo...")
        time.sleep(1)
        st.rerun()

def obtener_ordenes_compra(esquema=None):
    """Obtiene las órdenes de compra existentes, filtradas por esquema si se especifica"""
    try:
        with engine.connect() as conn:
            # Consulta base
            query_base = """
                SELECT oc.id, oc.numero_orden, oc.fecha_emision, oc.esquema, 
                       oc.servicio_beneficiario, oc.simese, oc.estado, 
                       u.username as usuario, oc.fecha_creacion,
                       COUNT(ioc.id) as cantidad_items,
                       SUM(ioc.monto_total) as monto_total
                FROM ordenes_compra oc
                JOIN usuarios u ON oc.usuario_id = u.id
                LEFT JOIN items_orden_compra ioc ON oc.id = ioc.orden_compra_id
            """
            
            # Agregar filtro por esquema si es necesario
            if esquema:
                query_base += " WHERE oc.esquema = :esquema "
                params = {'esquema': esquema}
            else:
                params = {}
            
            # Agrupar y ordenar
            query_base += """
                GROUP BY oc.id, oc.numero_orden, oc.fecha_emision, oc.esquema, 
                         oc.servicio_beneficiario, oc.simese, oc.estado, 
                         u.username, oc.fecha_creacion
                ORDER BY oc.fecha_creacion DESC
            """
            
            query = text(query_base)
            result = conn.execute(query, params)
            
            ordenes = []
            for row in result:
                ordenes.append({
                    'id': row[0],
                    'numero_orden': row[1],
                    'fecha_emision': row[2],
                    'esquema': row[3],
                    'servicio_beneficiario': row[4],
                    'simese': row[5],
                    'estado': row[6],
                    'usuario': row[7],
                    'fecha_creacion': row[8],
                    'cantidad_items': row[9],
                    'monto_total': row[10]
                })
            
            return ordenes
    except Exception as e:
        st.error(f"Error obteniendo órdenes de compra: {e}")
        return []

def obtener_detalles_orden_compra(orden_id):
    """Obtiene los detalles completos de una orden de compra"""
    try:
        with engine.connect() as conn:
            # Obtener cabecera
            query_cabecera = text("""
                SELECT oc.id, oc.numero_orden, oc.fecha_emision, oc.esquema, 
                       oc.servicio_beneficiario, oc.simese, oc.estado, 
                       u.username as usuario, oc.fecha_creacion,
                       u.nombre_completo as usuario_nombre
                FROM ordenes_compra oc
                JOIN usuarios u ON oc.usuario_id = u.id
                WHERE oc.id = :orden_id
            """)
            
            result = conn.execute(query_cabecera, {'orden_id': orden_id})
            cabecera = result.fetchone()
            
            if not cabecera:
                return None
            
            # Obtener items
            query_items = text("""
                SELECT id, lote, item, codigo_insumo, codigo_servicio, 
                       descripcion, cantidad, unidad_medida, precio_unitario, 
                       monto_total, observaciones
                FROM items_orden_compra
                WHERE orden_compra_id = :orden_id
                ORDER BY lote, item
            """)
            
            result_items = conn.execute(query_items, {'orden_id': orden_id})
            
            items = []
            for row in result_items:
                items.append({
                    'id': row[0],
                    'lote': row[1],
                    'item': row[2],
                    'codigo_insumo': row[3],
                    'codigo_servicio': row[4],
                    'descripcion': row[5],
                    'cantidad': row[6],
                    'unidad_medida': row[7],
                    'precio_unitario': row[8],
                    'monto_total': row[9],
                    'observaciones': row[10]
                })
            
            # Obtener datos de la licitación desde el esquema
            query_licitacion = text(f"""
                SELECT "NUMERO DE LLAMADO", "AÑO DEL LLAMADO", "NOMBRE DEL LLAMADO", 
                       "EMPRESA ADJUDICADA", "FECHA DE FIRMA DEL CONTRATO", 
                       "N° de Contrato / Año", "Vigencia del Contrato"
                FROM "{cabecera[3]}"."llamado"
                LIMIT 1
            """)
            
            result_licitacion = conn.execute(query_licitacion)
            licitacion = result_licitacion.fetchone()
            
            # Armar respuesta completa
            orden = {
                'id': cabecera[0],
                'numero_orden': cabecera[1],
                'fecha_emision': cabecera[2],
                'esquema': cabecera[3],
                'servicio_beneficiario': cabecera[4],
                'simese': cabecera[5],
                'estado': cabecera[6],
                'usuario': cabecera[7],
                'fecha_creacion': cabecera[8],
                'usuario_nombre': cabecera[9],
                'items': items,
                'monto_total': sum(item['monto_total'] for item in items),
                'cantidad_items': len(items)
            }
            
            # Agregar datos de licitación si están disponibles
            if licitacion:
                orden['licitacion'] = {
                    'numero_llamado': licitacion[0],
                    'anio_llamado': licitacion[1],
                    'nombre_llamado': licitacion[2],
                    'empresa_adjudicada': licitacion[3],
                    'fecha_contrato': licitacion[4],
                    'numero_contrato': licitacion[5],
                    'vigencia_contrato': licitacion[6]
                }
            
            return orden
    except Exception as e:
        st.error(f"Error obteniendo detalles de orden de compra: {e}")
        return None

def cambiar_estado_orden_compra(orden_id, nuevo_estado):
    """Cambia el estado de una orden de compra"""
    try:
        with engine.connect() as conn:
            query = text("""
                UPDATE ordenes_compra
                SET estado = :estado
                WHERE id = :orden_id
                RETURNING numero_orden
            """)
            
            result = conn.execute(query, {'estado': nuevo_estado, 'orden_id': orden_id})
            numero_orden = result.scalar()
            
            if numero_orden:
                return True, f"Estado de orden {numero_orden} cambiado a '{nuevo_estado}'"
            else:
                return False, "Orden de compra no encontrada"
    except Exception as e:
        return False, f"Error al cambiar estado: {e}"

def generar_pdf_orden_compra(orden_id):
    """
    Genera un PDF para la orden de compra
    
    Esta función es un placeholder. En la implementación real, deberías usar 
    una biblioteca como reportlab, weasyprint o pdfkit para generar el PDF.
    
    Returns:
        bytes: Contenido del PDF
    """
    try:
        orden = obtener_detalles_orden_compra(orden_id)
        if not orden:
            return None, "Orden no encontrada"
            
        # Placeholder: En una implementación real, aquí generarías el PDF
        # Por ahora, solo devolvemos un mensaje indicando que esta función debe implementarse
        return None, "La generación de PDF debe implementarse utilizando una biblioteca como reportlab o weasyprint"
    except Exception as e:
        return None, f"Error generando PDF: {e}"

def pagina_ordenes_compra():
    """Página principal de gestión de órdenes de compra"""
    st.header("Gestión de Órdenes de Compra")
    
    # Pestañas para diferentes funciones
    tab1, tab2 = st.tabs(["Lista de Órdenes", "Emitir Nueva Orden"])
    
    with tab1:
        st.subheader("Órdenes de Compra Emitidas")
        
        # Opción para filtrar por esquema
        esquemas = obtener_esquemas_postgres()
        esquema_seleccionado = st.selectbox(
            "Filtrar por esquema:",
            options=["Todos"] + esquemas,
            index=0
        )
        
        # Obtener órdenes de compra
        if esquema_seleccionado == "Todos":
            ordenes = obtener_ordenes_compra()
        else:
            ordenes = obtener_ordenes_compra(esquema_seleccionado)
        
        if ordenes:
            # Convertir a DataFrame para mejor visualización
            df_ordenes = pd.DataFrame(ordenes)
            
            # Dar formato a las fechas
            if 'fecha_emision' in df_ordenes.columns:
                df_ordenes['fecha_emision'] = pd.to_datetime(df_ordenes['fecha_emision']).dt.strftime('%Y-%m-%d')
            if 'fecha_creacion' in df_ordenes.columns:
                df_ordenes['fecha_creacion'] = pd.to_datetime(df_ordenes['fecha_creacion']).dt.strftime('%Y-%m-%d %H:%M')
            
            # Dar formato al monto total
            if 'monto_total' in df_ordenes.columns:
                df_ordenes['monto_total'] = df_ordenes['monto_total'].apply(lambda x: f"₲ {x:,.0f}".replace(",", "."))
            
            # Mostrar órdenes
            st.dataframe(df_ordenes)
            
            # Selector para ver detalles de una orden
            ordenes_ids = {f"{o['numero_orden']} - {o['servicio_beneficiario']}": o['id'] for o in ordenes}
            selected_orden = st.selectbox(
                "Seleccionar orden para ver detalles:",
                options=list(ordenes_ids.keys())
            )
            
            if selected_orden:
                orden_id = ordenes_ids[selected_orden]
                orden = obtener_detalles_orden_compra(orden_id)
                
                if orden:
                    st.subheader(f"Detalles de Orden de Compra: {orden['numero_orden']}")
                    
                    # Mostrar datos de cabecera
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Fecha Emisión:** {orden['fecha_emision'].strftime('%Y-%m-%d')}")
                        st.write(f"**Servicio Beneficiario:** {orden['servicio_beneficiario']}")
                        st.write(f"**SIMESE:** {orden['simese']}")
                    with col2:
                        st.write(f"**Estado:** {orden['estado']}")
                        st.write(f"**Usuario:** {orden['usuario_nombre']} ({orden['usuario']})")
                        st.write(f"**Fecha Creación:** {orden['fecha_creacion'].strftime('%Y-%m-%d %H:%M')}")
                    
                    # Mostrar datos de licitación si están disponibles
                    if 'licitacion' in orden:
                        with st.expander("Datos de la Licitación"):
                            lic = orden['licitacion']
                            st.write(f"**Llamado:** {lic['numero_llamado']}/{lic['anio_llamado']}")
                            st.write(f"**Nombre:** {lic['nombre_llamado']}")
                            st.write(f"**Empresa:** {lic['empresa_adjudicada']}")
                            st.write(f"**Contrato:** {lic['numero_contrato']}")
                            if lic['fecha_contrato']:
                                st.write(f"**Fecha Contrato:** {lic['fecha_contrato'].strftime('%Y-%m-%d')}")
                            st.write(f"**Vigencia:** {lic['vigencia_contrato']}")
                    
                    # Mostrar items
                    st.subheader("Items de la Orden")
                    
                    if orden['items']:
                        # Crear DataFrame para visualización
                        df_items = pd.DataFrame(orden['items'])
                        
                        # Formatear montos
                        df_items['precio_unitario'] = df_items['precio_unitario'].apply(lambda x: f"₲ {x:,.0f}".replace(",", "."))
                        df_items['monto_total'] = df_items['monto_total'].apply(lambda x: f"₲ {x:,.0f}".replace(",", "."))
                        
                        # Mostrar DataFrame
                        st.dataframe(df_items)
                        
                        # Mostrar monto total
                        st.subheader(f"Monto Total: ₲ {orden['monto_total']:,.0f}".replace(",", "."))
                    else:
                        st.info("Esta orden no tiene items.")
                    
                    # Opciones para la orden
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        if orden['estado'] == 'Emitida':
                            if st.button("Marcar como Entregada"):
                                success, message = cambiar_estado_orden_compra(orden_id, "Entregada")
                                if success:
                                    st.success(message)
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error(message)
                                    
                    with col2:
                        if orden['estado'] in ['Emitida', 'Entregada']:
                            if st.button("Marcar como Anulada"):
                                success, message = cambiar_estado_orden_compra(orden_id, "Anulada")
                                if success:
                                    st.success(message)
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error(message)
                    
                    with col3:
                        if st.button("Generar PDF"):
                            # Placeholder para generación de PDF
                            st.info("La generación de PDF será implementada en una versión futura.")
                            
                            # En una implementación real, generarías el PDF y lo ofrecerías para descarga
                            # pdf_bytes, message = generar_pdf_orden_compra(orden_id)
                            # if pdf_bytes:
                            #     st.download_button(
                            #         label="Descargar PDF",
                            #         data=pdf_bytes,
                            #         file_name=f"OC_{orden['numero_orden']}.pdf",
                            #         mime="application/pdf"
                            #     )
                            # else:
                            #     st.error(message)
        else:
            st.info("No hay órdenes de compra para mostrar.")
    
    with tab2:
        st.subheader("Emitir Nueva Orden de Compra")
        
        # Selector de esquema (licitación)
        esquema_seleccionado = st.selectbox(
            "Seleccionar Licitación:",
            options=esquemas
        )
        
        if esquema_seleccionado:
            # Obtener información de la licitación
            with engine.connect() as conn:
                try:
                    query = text(f"""
                        SELECT "NUMERO DE LLAMADO", "AÑO DEL LLAMADO", "NOMBRE DEL LLAMADO", 
                               "EMPRESA ADJUDICADA"
                        FROM "{esquema_seleccionado}"."llamado"
                        LIMIT 1
                    """)
                    result = conn.execute(query)
                    licitacion = result.fetchone()
                    
                    if licitacion:
                        st.write(f"**Licitación:** {licitacion[0]}/{licitacion[1]} - {licitacion[2]}")
                        st.write(f"**Empresa:** {licitacion[3]}")
                except Exception as e:
                    st.error(f"Error obteniendo datos de licitación: {e}")
            
            # Obtener lista de servicios beneficiarios
            servicios = obtener_servicios_beneficiarios(esquema_seleccionado)
            
            if servicios:
                servicio_seleccionado = st.selectbox(
                    "Seleccionar Servicio Beneficiario:",
                    options=servicios
                )
                
                # Formulario para los datos de la orden de compra
                with st.form("nueva_orden_form"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Generar número de orden sugerido
                        numero_sugerido = obtener_proximo_numero_oc(esquema_seleccionado)
                        numero_orden = st.text_input("Número de Orden:", value=numero_sugerido)
                    
                    with col2:
                        fecha_emision = st.date_input(
                            "Fecha de Emisión:",
                            value=datetime.now()
                        )
                    
                    simese = st.text_input("Número de SIMESE (Pedido):")
                    
                    st.subheader("Selección de Items")
                    
                    # Obtener items disponibles para el servicio seleccionado
                    items_disponibles = obtener_datos_items(esquema_seleccionado, servicio_seleccionado)
                    
                    if not items_disponibles:
                        st.warning(f"No hay items disponibles para el servicio '{servicio_seleccionado}'")
                        submit_disabled = True
                    else:
                        # Inicializar lista de items seleccionados si no existe
                        if 'items_seleccionados' not in st.session_state:
                            st.session_state.items_seleccionados = []
                        
                        # Mostrar items disponibles
                        df_items = pd.DataFrame(items_disponibles)
                        
                        # Formatear para mejor visualización
                        df_display = df_items.copy()
                        df_display['precio_unitario'] = df_display['precio_unitario'].apply(lambda x: f"₲ {x:,.0f}".replace(",", "."))
                        
                        st.dataframe(df_display)
                        
                        # Selector para agregar un item
                        items_opciones = {f"{i['lote']}-{i['item']} | {i['descripcion']}": idx for idx, i in enumerate(items_disponibles)}
                        item_seleccionado = st.selectbox(
                            "Seleccionar Item para agregar:",
                            options=list(items_opciones.keys())
                        )
                        
                        idx_item = items_opciones[item_seleccionado]
                        item = items_disponibles[idx_item]
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            cantidad = st.number_input(
                                "Cantidad:",
                                min_value=0.01,
                                max_value=float(item['saldo_emitir']),
                                value=min(1.0, float(item['saldo_emitir'])),
                                step=0.01,
                                format="%.2f"
                            )
                        
                        with col2:
                            precio = st.number_input(
                                "Precio Unitario:",
                                min_value=0.01,
                                value=float(item['precio_unitario']),
                                step=0.01,
                                format="%.2f",
                                disabled=True
                            )
                        
                        observaciones = st.text_area("Observaciones:", height=100)
                        
                        # Botón para agregar item a la lista
                        agregar_item = st.form_submit_button("Agregar Item")
                        
                        if agregar_item:
                            # Crear item para agregar
                            nuevo_item = {
                                'lote': item['lote'],
                                'item': item['item'],
                                'codigo_insumo': item['codigo_insumo'],
                                'codigo_servicio': item['codigo_servicio'],
                                'descripcion': item['descripcion'],
                                'cantidad': cantidad,
                                'unidad_medida': item['unidad_medida'],
                                'precio_unitario': float(item['precio_unitario']),
                                'monto_total': cantidad * float(item['precio_unitario']),
                                'observaciones': observaciones,
                                'saldo_emitir': float(item['saldo_emitir'])
                            }
                            
                            # Verificar que no se haya agregado ya
                            item_existe = any(
                                i['lote'] == nuevo_item['lote'] and 
                                i['item'] == nuevo_item['item'] 
                                for i in st.session_state.items_seleccionados
                            )
                            
                            if item_existe:
                                st.error(f"El item {nuevo_item['lote']}-{nuevo_item['item']} ya fue agregado.")
                            else:
                                st.session_state.items_seleccionados.append(nuevo_item)
                                st.success(f"Item {nuevo_item['lote']}-{nuevo_item['item']} agregado a la orden.")
                                st.rerun()
                        
                        # Mostrar items seleccionados
                        if st.session_state.items_seleccionados:
                            st.subheader("Items Seleccionados")
                            
                            # Crear DataFrame para visualización
                            df_seleccionados = pd.DataFrame(st.session_state.items_seleccionados)
                            
                            # Formatear para mejor visualización
                            df_display = df_seleccionados.copy()
                            df_display['precio_unitario'] = df_display['precio_unitario'].apply(lambda x: f"₲ {x:,.0f}".replace(",", "."))
                            df_display['monto_total'] = df_display['monto_total'].apply(lambda x: f"₲ {x:,.0f}".replace(",", "."))
                            
                            # Mostrar DataFrame
                            st.dataframe(df_display)
                            
                            # Mostrar monto total
                            monto_total = sum(item['monto_total'] for item in st.session_state.items_seleccionados)
                            st.subheader(f"Monto Total: ₲ {monto_total:,.0f}".replace(",", "."))
                            
                            # Botón para limpiar lista
                            if st.form_submit_button("Limpiar Lista"):
                                st.session_state.items_seleccionados = []
                                st.rerun()
                        
                        submit_disabled = len(st.session_state.items_seleccionados) == 0
                    
                    # Botón para emitir orden
                    submit = st.form_submit_button("Emitir Orden de Compra", disabled=submit_disabled)
                    
                    if submit and st.session_state.items_seleccionados:
                        # Validar datos
                        if not numero_orden:
                            st.error("Debe ingresar un número de orden.")
                        elif not simese:
                            st.error("Debe ingresar un número de SIMESE.")
                        else:
                            # Crear orden de compra
                            success, message, orden_id = crear_orden_compra(
                                esquema_seleccionado,
                                numero_orden,
                                fecha_emision,
                                servicio_seleccionado,
                                simese,
                                st.session_state.items_seleccionados
                            )
                            
                            if success:
                                st.success(message)
                                # Limpiar estado
                                st.session_state.items_seleccionados = []
                                # Mostrar botón para ver la orden
                                if st.button("Ver Orden Creada"):
                                    st.session_state.orden_seleccionada = orden_id
                                    st.session_state.menu = "ordenes_compra"
                                    st.rerun()
                            else:
                                st.error(message)
            else:
                st.warning(f"No hay servicios beneficiarios definidos para la licitación seleccionada.")
        else:
            st.info("Seleccione una licitación para emitir una orden de compra.")

    with tab1:
        st.subheader("Órdenes de Compra Emitidas")
        
        # Opción para filtrar por esquema
        esquemas = obtener_esquemas_postgres()
        esquema_seleccionado = st.selectbox(
            "Filtrar por esquema:",
            options=["Todos"] + esquemas,
            index=0
        )
        
        # Obtener órdenes de compra
        if esquema_seleccionado == "Todos":
            ordenes = obtener_ordenes_compra()
        else:
            ordenes = obtener_ordenes_compra(esquema_seleccionado)
        
        if ordenes:
            # Convertir a DataFrame para mejor visualización
            df_ordenes = pd.DataFrame(ordenes)
            
            # Dar formato a las fechas
            if 'fecha_emision' in df_ordenes.columns:
                df_ordenes['fecha_emision'] = pd.to_datetime(df_ordenes['fecha_emision']).dt.strftime('%Y-%m-%d')
            if 'fecha_creacion' in df_ordenes.columns:
                df_ordenes['fecha_creacion'] = pd.to_datetime(df_ordenes['fecha_creacion']).dt.strftime('%Y-%m-%d %H:%M')
            
            # Dar formato al monto total
            if 'monto_total' in df_ordenes.columns:
                df_ordenes['monto_total'] = df_ordenes['monto_total'].apply(lambda x: f"₲ {x:,.0f}".replace(",", "."))
            
            # Mostrar órdenes
            st.dataframe(df_ordenes)
            
            # Selector para ver detalles de una orden
            ordenes_ids = {f"{o['numero_orden']} - {o['servicio_beneficiario']}": o['id'] for o in ordenes}
            selected_orden = st.selectbox(
                "Seleccionar orden para ver detalles:",
                options=list(ordenes_ids.keys())
            )
            
            if selected_orden:
                orden_id = ordenes_ids[selected_orden]
                orden = obtener_detalles_orden_compra(orden_id)
                
                if orden:
                    st.subheader(f"Detalles de Orden de Compra: {orden['numero_orden']}")
                    
                    # Mostrar datos de cabecera
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Fecha Emisión:** {orden['fecha_emision'].strftime('%Y-%m-%d')}")
                        st.write(f"**Servicio Beneficiario:** {orden['servicio_beneficiario']}")
                        st.write(f"**SIMESE:** {orden['simese']}")
                    with col2:
                        st.write(f"**Estado:** {orden['estado']}")
                        st.write(f"**Usuario:** {orden['usuario_nombre']} ({orden['usuario']})")
                        st.write(f"**Fecha Creación:** {orden['fecha_creacion'].strftime('%Y-%m-%d %H:%M')}")
                    
                    # Mostrar datos de licitación si están disponibles
                    if 'licitacion' in orden:
                        with st.expander("Datos de la Licitación"):
                            lic = orden['licitacion']
                            st.write(f"**Llamado:** {lic['numero_llamado']}/{lic['anio_llamado']}")
                            st.write(f"**Nombre:** {lic['nombre_llamado']}")
                            st.write(f"**Empresa:** {lic['empresa_adjudicada']}")
                            st.write(f"**Contrato:** {lic['numero_contrato']}")
                            if lic['fecha_contrato']:
                                st.write(f"**Fecha Contrato:** {lic['fecha_contrato'].strftime('%Y-%m-%d')}")
                            st.write(f"**Vigencia:** {lic['vigencia_contrato']}")
                    
                    # Mostrar items
                    st.subheader("Items de la Orden")
                    
                    if orden['items']:
                        # Crear DataFrame para visualización
                        df_items = pd.DataFrame(orden['items'])
                        
                        # Formatear montos
                        df_items['precio_unitario'] = df_items['precio_unitario'].apply(lambda x: f"₲ {x:,.0f}".replace(",", "."))
                        df_items['monto_total'] = df_items['monto_total'].apply(lambda x: f"₲ {x:,.0f}".replace(",", "."))
                        
                        # Mostrar DataFrame
                        st.dataframe(df_items)
                        
                        # Mostrar monto total
                        st.subheader(f"Monto Total: ₲ {orden['monto_total']:,.0f}".replace(",", "."))
                    else:
                        st.info("Esta orden no tiene items.")
                    
                    # Opciones para la orden
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        if orden['estado'] == 'Emitida':
                            if st.button("Marcar como Entregada"):
                                success, message = cambiar_estado_orden_compra(orden_id, "Entregada")
                                if success:
                                    st.success(message)
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error(message)
                                    
                    with col2:
                        if orden['estado'] in ['Emitida', 'Entregada']:
                            if st.button("Marcar como Anulada"):
                                success, message = cambiar_estado_orden_compra(orden_id, "Anulada")
                                if success:
                                    st.success(message)
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error(message)
                    
                    with col3:
                        if st.button("Generar PDF"):
                            # Placeholder para generación de PDF
                            st.info("La generación de PDF será implementada en una versión futura.")
        else:
            st.info("No hay órdenes de compra para mostrar.")

if __name__ == "__main__":
    main()