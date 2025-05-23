import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import os
import io
import hashlib
from sqlalchemy import create_engine, text

# Configuración de conexión a PostgreSQL
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "licitaciones_db"
DB_USER = "postgres"
DB_PASSWORD = "admin"

# Intervalo de actualización automática (en minutos)
INTERVALO_ACTUALIZACION = 15

# Crear conexión a PostgreSQL
try:
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
except Exception as e:
    print(f"Error al crear conexión a PostgreSQL: {e}")

# Función para configurar la tabla de usuarios
def configurar_tabla_usuarios():
    """Crea la tabla de usuarios si no existe"""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS usuarios (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(50) UNIQUE NOT NULL,
                    password VARCHAR(200) NOT NULL,
                    nombre_completo VARCHAR(100) NOT NULL,
                    role VARCHAR(20) NOT NULL DEFAULT 'user',
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            
            # Verificar si existe el usuario admin
            query = text("SELECT COUNT(*) FROM usuarios WHERE username = 'admin'")
            result = conn.execute(query)
            count = result.scalar()
            
            if count == 0:
                # Crear usuario admin por defecto
                password_hash = hashlib.sha256("admin".encode()).hexdigest()
                
                query = text("""
                    INSERT INTO usuarios (username, password, nombre_completo, role)
                    VALUES ('admin', :password, 'Administrador del Sistema', 'admin')
                """)
                
                conn.execute(query, {'password': password_hash})
                
            return True
    except Exception as e:
        print(f"Error configurando tabla de usuarios: {e}")
        return False

# Función para configurar la tabla de archivos cargados
def configurar_tabla_cargas():
    """Crea la tabla para registrar las cargas de archivos CSV"""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS archivos_cargados (
                    id SERIAL PRIMARY KEY,
                    nombre_archivo VARCHAR(255) NOT NULL,
                    esquema VARCHAR(100) NOT NULL,
                    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    usuario_id INTEGER NOT NULL,
                    contenido_original TEXT,
                    ubicacion_fisica VARCHAR(500),
                    estado VARCHAR(50) DEFAULT 'Activo',
                    FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
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
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ordenes_compra (
                    id SERIAL PRIMARY KEY,
                    numero_orden VARCHAR(50) UNIQUE NOT NULL,
                    fecha_emision TIMESTAMP NOT NULL,
                    esquema VARCHAR(100) NOT NULL,
                    servicio_beneficiario VARCHAR(200),
                    simese VARCHAR(50),
                    usuario_id INTEGER NOT NULL,
                    estado VARCHAR(50) NOT NULL DEFAULT 'Emitida',
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
                );
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS items_orden_compra (
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
                    FOREIGN KEY (orden_compra_id) REFERENCES ordenes_compra(id) ON DELETE CASCADE
                );
            """))
            
            return True
    except Exception as e:
        print(f"Error configurando tablas de órdenes de compra: {e}")
        return False

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
    """Obtiene la lista de esquemas existentes en PostgreSQL"""
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'public')
                ORDER BY schema_name
            """)
            
            result = conn.execute(query)
            
            esquemas = [row[0] for row in result]
            return esquemas
    except Exception as e:
        st.error(f"Error obteniendo esquemas: {e}")
        return []

def cargar_archivo_a_postgres(archivo_csv, nombre_archivo, esquema):
    """Carga un archivo CSV directamente a PostgreSQL"""
    try:
        # Leer el contenido del archivo
        contenido = archivo_csv.getvalue().decode('utf-8')
        
        # Crear tabla en PostgreSQL para registrar el archivo original
        with engine.connect() as conn:
            # Guardar registro del archivo en la tabla de cargas
            query = text("""
                INSERT INTO archivos_cargados 
                (nombre_archivo, esquema, usuario_id, contenido_original)
                VALUES (:nombre, :esquema, :usuario_id, :contenido)
                RETURNING id
            """)
            
            result = conn.execute(query, {
                'nombre': nombre_archivo,
                'esquema': esquema,
                'usuario_id': st.session_state.user_id,
                'contenido': contenido
            })
            
            archivo_id = result.scalar()
            
        # Procesar el CSV y cargar en las tablas correspondientes
        # (Aquí iría la lógica de procesamiento del CSV, que depende del formato específico)
        # Por ejemplo, crear las tablas necesarias en el esquema y cargar los datos
        
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
        username = st.text_input("Usuario:")
        password = st.text_input("Contraseña:", type="password")
        
        if st.button("Ingresar"):
            if not username or not password:
                st.error("Por favor, complete todos los campos.")
            else:
                # Verificar credenciales
                password_hash = hashlib.sha256(password.encode()).hexdigest()
                
                try:
                    with engine.connect() as conn:
                        query = text("""
                            SELECT id, username, role, nombre_completo 
                            FROM usuarios 
                            WHERE username = :username AND password = :password
                        """)
                        
                        result = conn.execute(query, {
                            'username': username, 
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
                            
                            st.success("Inicio de sesión exitoso!")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("Usuario o contraseña incorrectos.")
                except Exception as e:
                    st.error(f"Error al verificar credenciales: {e}")
    
    with col2:
        st.image("https://via.placeholder.com/300x200?text=Logo+Sistema", width=300)

def pagina_cargar_archivo():
    """Página para cargar un nuevo archivo CSV"""
    st.header("Cargar Archivo CSV")
    
    # Obtener esquemas existentes
    esquemas = obtener_esquemas_postgres()
    
    # Formulario para subir archivo
    with st.form("upload_form"):
        # Opción para crear nuevo esquema o usar uno existente
        opcion_esquema = st.radio(
            "Seleccione una opción:",
            ["Crear nueva licitación", "Agregar a licitación existente"]
        )
        
        if opcion_esquema == "Crear nueva licitación":
            nuevo_esquema = st.text_input("Nombre de la nueva licitación (esquema):")
        else:
            esquema_seleccionado = st.selectbox(
                "Seleccionar licitación existente:",
                options=esquemas
            )
        
        # Campo para subir archivo
        archivo_csv = st.file_uploader("Seleccionar archivo CSV:", type=["csv"])
        
        # Botón para procesar
        submit = st.form_submit_button("Procesar archivo")
        
        if submit:
            if not archivo_csv:
                st.error("Por favor, seleccione un archivo CSV.")
            elif opcion_esquema == "Crear nueva licitación" and not nuevo_esquema:
                st.error("Por favor, ingrese un nombre para la nueva licitación.")
            else:
                esquema = nuevo_esquema if opcion_esquema == "Crear nueva licitación" else esquema_seleccionado
                
                # Normalizar el nombre del esquema (quitar espacios, etc.)
                esquema = esquema.strip().lower().replace(" ", "_")
                
                # Procesar el archivo
                success, message = cargar_archivo_a_postgres(
                    archivo_csv,
                    archivo_csv.name,
                    esquema
                )
                
                if success:
                    st.success(message)
                else:
                    st.error(message)

def pagina_ver_cargas():
    """Página para ver las cargas realizadas"""
    st.header("Archivos Cargados")
    
    archivos = obtener_archivos_cargados()
    
    if archivos:
        # Convertir a DataFrame para mejor visualización
        df_archivos = pd.DataFrame(archivos)
        
        # Dar formato a las fechas
        if 'fecha_carga' in df_archivos.columns:
            df_archivos['fecha_carga'] = pd.to_datetime(df_archivos['fecha_carga']).dt.strftime('%Y-%m-%d %H:%M')
        
        # Colorear estado
        def colorear_estado(estado):
            if estado == 'Activo':
                return 'background-color: #d4edda; color: #155724'
            elif estado == 'Eliminado':
                return 'background-color: #f8d7da; color: #721c24'
            else:
                return ''
        
        # Aplicar estilo condicional
        df_styled = df_archivos.style.applymap(colorear_estado, subset=['estado'])
        
        # Mostrar DataFrame
        st.dataframe(df_styled)
        
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
                            FROM archivos_cargados
                            WHERE id = :id
                        """)
                        
                        result = conn.execute(query, {'id': archivo_seleccionado['id']})
                        contenido = result.scalar()
                        
                        if contenido:
                            st.download_button(
                                label="Descargar CSV original",
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
                    SELECT id, username, nombre_completo, role, fecha_creacion
                    FROM usuarios
                    ORDER BY username
                """)
                
                result = conn.execute(query)
                
                usuarios = []
                for row in result:
                    usuarios.append({
                        'id': row[0],
                        'username': row[1],
                        'nombre': row[2],
                        'rol': row[3],
                        'fecha_creacion': row[4]
                    })
                
                if usuarios:
                    # Convertir a DataFrame para mejor visualización
                    df_usuarios = pd.DataFrame(usuarios)
                    
                    # Dar formato a las fechas
                    if 'fecha_creacion' in df_usuarios.columns:
                        df_usuarios['fecha_creacion'] = pd.to_datetime(df_usuarios['fecha_creacion']).dt.strftime('%Y-%m-%d %H:%M')
                    
                    # Mostrar usuarios
                    st.dataframe(df_usuarios)
                    
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
                            nombre = st.text_input("Nombre completo:", value=usuario['nombre'])
                            rol = st.selectbox(
                                "Rol:",
                                options=["admin", "user"],
                                index=0 if usuario['rol'] == "admin" else 1
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
                                            # Actualizar usuario
                                            if reset_password and new_password:
                                                # Actualizar con nueva contraseña
                                                password_hash = hashlib.sha256(new_password.encode()).hexdigest()
                                                
                                                query = text("""
                                                    UPDATE usuarios
                                                    SET nombre_completo = :nombre, role = :rol, password = :password
                                                    WHERE id = :id
                                                """)
                                                
                                                conn.execute(query, {
                                                    'nombre': nombre,
                                                    'rol': rol,
                                                    'password': password_hash,
                                                    'id': usuario['id']
                                                })
                                            else:
                                                # Actualizar sin cambiar contraseña
                                                query = text("""
                                                    UPDATE usuarios
                                                    SET nombre_completo = :nombre, role = :rol
                                                    WHERE id = :id
                                                """)
                                                
                                                conn.execute(query, {
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
            username = st.text_input("Nombre de usuario:")
            password = st.text_input("Contraseña:", type="password")
            nombre = st.text_input("Nombre completo:")
            rol = st.selectbox(
                "Rol:",
                options=["user", "admin"]
            )
            
            submit = st.form_submit_button("Crear Usuario")
            
            if submit:
                if not username or not password or not nombre:
                    st.error("Por favor, complete todos los campos.")
                else:
                    try:
                        # Verificar si el usuario ya existe
                        with engine.connect() as conn:
                            query = text("SELECT COUNT(*) FROM usuarios WHERE username = :username")
                            result = conn.execute(query, {'username': username})
                            count = result.scalar()
                            
                            if count > 0:
                                st.error(f"El usuario '{username}' ya existe.")
                            else:
                                # Crear nuevo usuario
                                password_hash = hashlib.sha256(password.encode()).hexdigest()
                                
                                query = text("""
                                    INSERT INTO usuarios (username, password, nombre_completo, role)
                                    VALUES (:username, :password, :nombre, :rol)
                                """)
                                
                                conn.execute(query, {
                                    'username': username,
                                    'password': password_hash,
                                    'nombre': nombre,
                                    'rol': rol
                                })
                                
                                st.success(f"Usuario '{username}' creado exitosamente")
                                time.sleep(1)
                                st.rerun()
                    except Exception as e:
                        st.error(f"Error al crear usuario: {e}")

def pagina_cambiar_password():
    """Página para cambiar contraseña del usuario actual"""
    st.header("Cambiar Contraseña")
    
    with st.form("cambiar_password_form"):
        password_actual = st.text_input("Contraseña actual:", type="password")
        password_nueva = st.text_input("Nueva contraseña:", type="password")
        password_confirmar = st.text_input("Confirmar nueva contraseña:", type="password")
        
        submit = st.form_submit_button("Cambiar Contraseña")
        
        if submit:
            if not password_actual or not password_nueva or not password_confirmar:
                st.error("Por favor, complete todos los campos.")
            elif password_nueva != password_confirmar:
                st.error("Las contraseñas no coinciden.")
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
                                SET password = :password
                                WHERE id = :user_id
                            """)
                            
                            conn.execute(query, {
                                'password': password_hash_nueva,
                                'user_id': st.session_state.user_id
                            })
                            
                            st.success("Contraseña cambiada exitosamente.")
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
                next_num


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
            import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import os
import io
import hashlib
from sqlalchemy import create_engine, text

# Configuración de conexión a PostgreSQL
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "licitaciones_db"
DB_USER = "postgres"
DB_PASSWORD = "admin"

# Intervalo de actualización automática (en minutos)
INTERVALO_ACTUALIZACION = 15

# Crear conexión a PostgreSQL
try:
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
except Exception as e:
    print(f"Error al crear conexión a PostgreSQL: {e}")

# Función para configurar la tabla de usuarios
def configurar_tabla_usuarios():
    """Crea la tabla de usuarios si no existe"""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS usuarios (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(50) UNIQUE NOT NULL,
                    password VARCHAR(200) NOT NULL,
                    nombre_completo VARCHAR(100) NOT NULL,
                    role VARCHAR(20) NOT NULL DEFAULT 'user',
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            
            # Verificar si existe el usuario admin
            query = text("SELECT COUNT(*) FROM usuarios WHERE username = 'admin'")
            result = conn.execute(query)
            count = result.scalar()
            
            if count == 0:
                # Crear usuario admin por defecto
                password_hash = hashlib.sha256("admin".encode()).hexdigest()
                
                query = text("""
                    INSERT INTO usuarios (username, password, nombre_completo, role)
                    VALUES ('admin', :password, 'Administrador del Sistema', 'admin')
                """)
                
                conn.execute(query, {'password': password_hash})
                
            return True
    except Exception as e:
        print(f"Error configurando tabla de usuarios: {e}")
        return False

# Función para configurar la tabla de archivos cargados
def configurar_tabla_cargas():
    """Crea la tabla para registrar las cargas de archivos CSV"""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS archivos_cargados (
                    id SERIAL PRIMARY KEY,
                    nombre_archivo VARCHAR(255) NOT NULL,
                    esquema VARCHAR(100) NOT NULL,
                    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    usuario_id INTEGER NOT NULL,
                    contenido_original TEXT,
                    ubicacion_fisica VARCHAR(500),
                    estado VARCHAR(50) DEFAULT 'Activo',
                    FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
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
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ordenes_compra (
                    id SERIAL PRIMARY KEY,
                    numero_orden VARCHAR(50) UNIQUE NOT NULL,
                    fecha_emision TIMESTAMP NOT NULL,
                    esquema VARCHAR(100) NOT NULL,
                    servicio_beneficiario VARCHAR(200),
                    simese VARCHAR(50),
                    usuario_id INTEGER NOT NULL,
                    estado VARCHAR(50) NOT NULL DEFAULT 'Emitida',
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
                );
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS items_orden_compra (
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
                    FOREIGN KEY (orden_compra_id) REFERENCES ordenes_compra(id) ON DELETE CASCADE
                );
            """))
            
            return True
    except Exception as e:
        print(f"Error configurando tablas de órdenes de compra: {e}")
        return False

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
    """Obtiene la lista de esquemas existentes en PostgreSQL"""
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'public')
                ORDER BY schema_name
            """)
            
            result = conn.execute(query)
            
            esquemas = [row[0] for row in result]
            return esquemas
    except Exception as e:
        st.error(f"Error obteniendo esquemas: {e}")
        return []

def cargar_archivo_a_postgres(archivo_csv, nombre_archivo, esquema):
    """Carga un archivo CSV directamente a PostgreSQL"""
    try:
        # Leer el contenido del archivo
        contenido = archivo_csv.getvalue().decode('utf-8')
        
        # Crear tabla en PostgreSQL para registrar el archivo original
        with engine.connect() as conn:
            # Guardar registro del archivo en la tabla de cargas
            query = text("""
                INSERT INTO archivos_cargados 
                (nombre_archivo, esquema, usuario_id, contenido_original)
                VALUES (:nombre, :esquema, :usuario_id, :contenido)
                RETURNING id
            """)
            
            result = conn.execute(query, {
                'nombre': nombre_archivo,
                'esquema': esquema,
                'usuario_id': st.session_state.user_id,
                'contenido': contenido
            })
            
            archivo_id = result.scalar()
            
        # Procesar el CSV y cargar en las tablas correspondientes
        # (Aquí iría la lógica de procesamiento del CSV, que depende del formato específico)
        # Por ejemplo, crear las tablas necesarias en el esquema y cargar los datos
        
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
        username = st.text_input("Usuario:")
        password = st.text_input("Contraseña:", type="password")
        
        if st.button("Ingresar"):
            if not username or not password:
                st.error("Por favor, complete todos los campos.")
            else:
                # Verificar credenciales
                password_hash = hashlib.sha256(password.encode()).hexdigest()
                
                try:
                    with engine.connect() as conn:
                        query = text("""
                            SELECT id, username, role, nombre_completo 
                            FROM usuarios 
                            WHERE username = :username AND password = :password
                        """)
                        
                        result = conn.execute(query, {
                            'username': username, 
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
                            
                            st.success("Inicio de sesión exitoso!")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("Usuario o contraseña incorrectos.")
                except Exception as e:
                    st.error(f"Error al verificar credenciales: {e}")
    
    with col2:
        st.image("https://via.placeholder.com/300x200?text=Logo+Sistema", width=300)

def pagina_cargar_archivo():
    """Página para cargar un nuevo archivo CSV"""
    st.header("Cargar Archivo CSV")
    
    # Obtener esquemas existentes
    esquemas = obtener_esquemas_postgres()
    
    # Formulario para subir archivo
    with st.form("upload_form"):
        # Opción para crear nuevo esquema o usar uno existente
        opcion_esquema = st.radio(
            "Seleccione una opción:",
            ["Crear nueva licitación", "Agregar a licitación existente"]
        )
        
        if opcion_esquema == "Crear nueva licitación":
            nuevo_esquema = st.text_input("Nombre de la nueva licitación (esquema):")
        else:
            esquema_seleccionado = st.selectbox(
                "Seleccionar licitación existente:",
                options=esquemas
            )
        
        # Campo para subir archivo
        archivo_csv = st.file_uploader("Seleccionar archivo CSV:", type=["csv"])
        
        # Botón para procesar
        submit = st.form_submit_button("Procesar archivo")
        
        if submit:
            if not archivo_csv:
                st.error("Por favor, seleccione un archivo CSV.")
            elif opcion_esquema == "Crear nueva licitación" and not nuevo_esquema:
                st.error("Por favor, ingrese un nombre para la nueva licitación.")
            else:
                esquema = nuevo_esquema if opcion_esquema == "Crear nueva licitación" else esquema_seleccionado
                
                # Normalizar el nombre del esquema (quitar espacios, etc.)
                esquema = esquema.strip().lower().replace(" ", "_")
                
                # Procesar el archivo
                success, message = cargar_archivo_a_postgres(
                    archivo_csv,
                    archivo_csv.name,
                    esquema
                )
                
                if success:
                    st.success(message)
                else:
                    st.error(message)

def pagina_ver_cargas():
    """Página para ver las cargas realizadas"""
    st.header("Archivos Cargados")
    
    archivos = obtener_archivos_cargados()
    
    if archivos:
        # Convertir a DataFrame para mejor visualización
        df_archivos = pd.DataFrame(archivos)
        
        # Dar formato a las fechas
        if 'fecha_carga' in df_archivos.columns:
            df_archivos['fecha_carga'] = pd.to_datetime(df_archivos['fecha_carga']).dt.strftime('%Y-%m-%d %H:%M')
        
        # Colorear estado
        def colorear_estado(estado):
            if estado == 'Activo':
                return 'background-color: #d4edda; color: #155724'
            elif estado == 'Eliminado':
                return 'background-color: #f8d7da; color: #721c24'
            else:
                return ''
        
        # Aplicar estilo condicional
        df_styled = df_archivos.style.applymap(colorear_estado, subset=['estado'])
        
        # Mostrar DataFrame
        st.dataframe(df_styled)
        
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
                            FROM archivos_cargados
                            WHERE id = :id
                        """)
                        
                        result = conn.execute(query, {'id': archivo_seleccionado['id']})
                        contenido = result.scalar()
                        
                        if contenido:
                            st.download_button(
                                label="Descargar CSV original",
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
                    SELECT id, username, nombre_completo, role, fecha_creacion
                    FROM usuarios
                    ORDER BY username
                """)
                
                result = conn.execute(query)
                
                usuarios = []
                for row in result:
                    usuarios.append({
                        'id': row[0],
                        'username': row[1],
                        'nombre': row[2],
                        'rol': row[3],
                        'fecha_creacion': row[4]
                    })
                
                if usuarios:
                    # Convertir a DataFrame para mejor visualización
                    df_usuarios = pd.DataFrame(usuarios)
                    
                    # Dar formato a las fechas
                    if 'fecha_creacion' in df_usuarios.columns:
                        df_usuarios['fecha_creacion'] = pd.to_datetime(df_usuarios['fecha_creacion']).dt.strftime('%Y-%m-%d %H:%M')
                    
                    # Mostrar usuarios
                    st.dataframe(df_usuarios)
                    
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
                            nombre = st.text_input("Nombre completo:", value=usuario['nombre'])
                            rol = st.selectbox(
                                "Rol:",
                                options=["admin", "user"],
                                index=0 if usuario['rol'] == "admin" else 1
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
                                            # Actualizar usuario
                                            if reset_password and new_password:
                                                # Actualizar con nueva contraseña
                                                password_hash = hashlib.sha256(new_password.encode()).hexdigest()
                                                
                                                query = text("""
                                                    UPDATE usuarios
                                                    SET nombre_completo = :nombre, role = :rol, password = :password
                                                    WHERE id = :id
                                                """)
                                                
                                                conn.execute(query, {
                                                    'nombre': nombre,
                                                    'rol': rol,
                                                    'password': password_hash,
                                                    'id': usuario['id']
                                                })
                                            else:
                                                # Actualizar sin cambiar contraseña
                                                query = text("""
                                                    UPDATE usuarios
                                                    SET nombre_completo = :nombre, role = :rol
                                                    WHERE id = :id
                                                """)
                                                
                                                conn.execute(query, {
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
            username = st.text_input("Nombre de usuario:")
            password = st.text_input("Contraseña:", type="password")
            nombre = st.text_input("Nombre completo:")
            rol = st.selectbox(
                "Rol:",
                options=["user", "admin"]
            )
            
            submit = st.form_submit_button("Crear Usuario")
            
            if submit:
                if not username or not password or not nombre:
                    st.error("Por favor, complete todos los campos.")
                else:
                    try:
                        # Verificar si el usuario ya existe
                        with engine.connect() as conn:
                            query = text("SELECT COUNT(*) FROM usuarios WHERE username = :username")
                            result = conn.execute(query, {'username': username})
                            count = result.scalar()
                            
                            if count > 0:
                                st.error(f"El usuario '{username}' ya existe.")
                            else:
                                # Crear nuevo usuario
                                password_hash = hashlib.sha256(password.encode()).hexdigest()
                                
                                query = text("""
                                    INSERT INTO usuarios (username, password, nombre_completo, role)
                                    VALUES (:username, :password, :nombre, :rol)
                                """)
                                
                                conn.execute(query, {
                                    'username': username,
                                    'password': password_hash,
                                    'nombre': nombre,
                                    'rol': rol
                                })
                                
                                st.success(f"Usuario '{username}' creado exitosamente")
                                time.sleep(1)
                                st.rerun()
                    except Exception as e:
                        st.error(f"Error al crear usuario: {e}")

def pagina_cambiar_password():
    """Página para cambiar contraseña del usuario actual"""
    st.header("Cambiar Contraseña")
    
    with st.form("cambiar_password_form"):
        password_actual = st.text_input("Contraseña actual:", type="password")
        password_nueva = st.text_input("Nueva contraseña:", type="password")
        password_confirmar = st.text_input("Confirmar nueva contraseña:", type="password")
        
        submit = st.form_submit_button("Cambiar Contraseña")
        
        if submit:
            if not password_actual or not password_nueva or not password_confirmar:
                st.error("Por favor, complete todos los campos.")
            elif password_nueva != password_confirmar:
                st.error("Las contraseñas no coinciden.")
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
                                SET password = :password
                                WHERE id = :user_id
                            """)
                            
                            conn.execute(query, {
                                'password': password_hash_nueva,
                                'user_id': st.session_state.user_id
                            })
                            
                            st.success("Contraseña cambiada exitosamente.")
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
        
        with col1:
            st.metric(
                label="Total Licitaciones", 
                value=len(esquemas)
            )
        
        # Obtener cantidad de órdenes de compra
        ordenes = obtener_ordenes_compra()
        
        with col2:
            st.metric(
                label="Órdenes de Compra", 
                value=len(ordenes)
            )
        
        # Calcular monto total de órdenes
        monto_total = sum(orden['monto_total'] for orden in ordenes if orden['monto_total'])
        
        with col3:
            st.metric(
                label="Monto Total Emitido", 
                value=f"₲ {monto_total:,.0f}".replace(",", ".")
            )
        
        # Mostrar gráficos
        st.subheader("Órdenes de Compra por Esquema")
        
        if esquemas and ordenes:
            # Agrupar órdenes por esquema
            ordenes_por_esquema = {}
            for orden in ordenes:
                esquema = orden['esquema']
                if esquema not in ordenes_por_esquema:
                    ordenes_por_esquema[esquema] = 0
                ordenes_por_esquema[esquema] += 1
            
            # Crear gráfico de barras
            datos_grafico = pd.DataFrame({
                'Esquema': list(ordenes_por_esquema.keys()),
                'Cantidad': list(ordenes_por_esquema.values())
            })
            
            st.bar_chart(datos_grafico.set_index('Esquema'))
            
            # Mostrar últimas órdenes de compra
            st.subheader("Últimas Órdenes de Compra")
            
            # Mostrar las últimas 5 órdenes
            ultimas_ordenes = sorted(ordenes, key=lambda x: x['fecha_creacion'], reverse=True)[:5]
            
            # Crear DataFrame para visualización
            df_ultimas = pd.DataFrame(ultimas_ordenes)
            
            # Dar formato a las fechas
            if 'fecha_emision' in df_ultimas.columns:
                df_ultimas['fecha_emision'] = pd.to_datetime(df_ultimas['fecha_emision']).dt.strftime('%Y-%m-%d')
            if 'fecha_creacion' in df_ultimas.columns:
                df_ultimas['fecha_creacion'] = pd.to_datetime(df_ultimas['fecha_creacion']).dt.strftime('%Y-%m-%d %H:%M')
            
            # Dar formato al monto total
            if 'monto_total' in df_ultimas.columns:
                df_ultimas['monto_total'] = df_ultimas['monto_total'].apply(lambda x: f"₲ {x:,.0f}".replace(",", "."))
            
            # Mostrar tabla
            st.dataframe(df_ultimas)
            
            # Botón para ir a órdenes de compra
            if st.button("Ver todas las órdenes"):
                # Cambiar a la página de órdenes de compra
                st.session_state.menu = "ordenes_compra"
                st.rerun()
        else:
            st.info("No hay órdenes de compra para mostrar.")
        
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
    
    # Inicializar el estado de sesión si es necesario
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
    
    # Verificar si el usuario está autenticado
    if not st.session_state.logged_in:
        pagina_login()
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
    st.title("Sistema de Gestión de Licitaciones")
    
    # Opciones de menú según el rol
    if st.session_state.user_role == 'admin':
        menu_options = {
            "dashboard": "📈 Dashboard",
            "cargar_archivo": "📥 Cargar Archivo", 
            "ver_cargas": "📋 Ver Cargas",
            "ordenes_compra": "📝 Órdenes de Compra",
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
            "cambiar_password": "🔑 Cambiar Contraseña",
            "logout": "🚪 Cerrar Sesión"
        }
    
    # Crear menú de navegación
    menu = st.sidebar.radio(
        "Menú de Navegación", 
        list(menu_options.keys()),
        format_func=lambda x: menu_options[x]
    )
    
    # Mostrar la página seleccionada
    if menu == "dashboard":
        pagina_dashboard()
    elif menu == "cargar_archivo":
        pagina_cargar_archivo()
    elif menu == "ver_cargas":
        pagina_ver_cargas()
    elif menu == "ordenes_compra":
        pagina_ordenes_compra()
    elif menu == "eliminar_esquemas" and st.session_state.user_role == 'admin':
        pagina_eliminar_esquemas()
    elif menu == "admin_usuarios" and st.session_state.user_role == 'admin':
        pagina_administrar_usuarios()
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

if __name__ == "__main__":
    main()def pagina_ordenes_compra():
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
                                st.button("Ver Orden Creada")
                            else:
                                st.error(message)
            else:
                st.warning(f"No hay servicios beneficiarios definidos para la licitación seleccionada.")
        else:
            st.info("Seleccione una licitación para emitir una orden de compra.")
            import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import os
import io
import hashlib
from sqlalchemy import create_engine, text

# Configuración de conexión a PostgreSQL
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "licitaciones_db"
DB_USER = "postgres"
DB_PASSWORD = "admin"

# Intervalo de actualización automática (en minutos)
INTERVALO_ACTUALIZACION = 15

# Crear conexión a PostgreSQL
try:
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
except Exception as e:
    print(f"Error al crear conexión a PostgreSQL: {e}")

# Función para configurar la tabla de usuarios
def configurar_tabla_usuarios():
    """Crea la tabla de usuarios si no existe"""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS usuarios (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(50) UNIQUE NOT NULL,
                    password VARCHAR(200) NOT NULL,
                    nombre_completo VARCHAR(100) NOT NULL,
                    role VARCHAR(20) NOT NULL DEFAULT 'user',
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            
            # Verificar si existe el usuario admin
            query = text("SELECT COUNT(*) FROM usuarios WHERE username = 'admin'")
            result = conn.execute(query)
            count = result.scalar()
            
            if count == 0:
                # Crear usuario admin por defecto
                password_hash = hashlib.sha256("admin".encode()).hexdigest()
                
                query = text("""
                    INSERT INTO usuarios (username, password, nombre_completo, role)
                    VALUES ('admin', :password, 'Administrador del Sistema', 'admin')
                """)
                
                conn.execute(query, {'password': password_hash})
                
            return True
    except Exception as e:
        print(f"Error configurando tabla de usuarios: {e}")
        return False

# Función para configurar la tabla de archivos cargados
def configurar_tabla_cargas():
    """Crea la tabla para registrar las cargas de archivos CSV"""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS archivos_cargados (
                    id SERIAL PRIMARY KEY,
                    nombre_archivo VARCHAR(255) NOT NULL,
                    esquema VARCHAR(100) NOT NULL,
                    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    usuario_id INTEGER NOT NULL,
                    contenido_original TEXT,
                    ubicacion_fisica VARCHAR(500),
                    estado VARCHAR(50) DEFAULT 'Activo',
                    FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
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
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ordenes_compra (
                    id SERIAL PRIMARY KEY,
                    numero_orden VARCHAR(50) UNIQUE NOT NULL,
                    fecha_emision TIMESTAMP NOT NULL,
                    esquema VARCHAR(100) NOT NULL,
                    servicio_beneficiario VARCHAR(200),
                    simese VARCHAR(50),
                    usuario_id INTEGER NOT NULL,
                    estado VARCHAR(50) NOT NULL DEFAULT 'Emitida',
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
                );
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS items_orden_compra (
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
                    FOREIGN KEY (orden_compra_id) REFERENCES ordenes_compra(id) ON DELETE CASCADE
                );
            """))
            
            return True
    except Exception as e:
        print(f"Error configurando tablas de órdenes de compra: {e}")
        return False

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
    """Obtiene la lista de esquemas existentes en PostgreSQL"""
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'public')
                ORDER BY schema_name
            """)
            
            result = conn.execute(query)
            
            esquemas = [row[0] for row in result]
            return esquemas
    except Exception as e:
        st.error(f"Error obteniendo esquemas: {e}")
        return []

def cargar_archivo_a_postgres(archivo_csv, nombre_archivo, esquema):
    """Carga un archivo CSV directamente a PostgreSQL"""
    try:
        # Leer el contenido del archivo
        contenido = archivo_csv.getvalue().decode('utf-8')
        
        # Crear tabla en PostgreSQL para registrar el archivo original
        with engine.connect() as conn:
            # Guardar registro del archivo en la tabla de cargas
            query = text("""
                INSERT INTO archivos_cargados 
                (nombre_archivo, esquema, usuario_id, contenido_original)
                VALUES (:nombre, :esquema, :usuario_id, :contenido)
                RETURNING id
            """)
            
            result = conn.execute(query, {
                'nombre': nombre_archivo,
                'esquema': esquema,
                'usuario_id': st.session_state.user_id,
                'contenido': contenido
            })
            
            archivo_id = result.scalar()
            
        # Procesar el CSV y cargar en las tablas correspondientes
        # (Aquí iría la lógica de procesamiento del CSV, que depende del formato específico)
        # Por ejemplo, crear las tablas necesarias en el esquema y cargar los datos
        
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
        username = st.text_input("Usuario:")
        password = st.text_input("Contraseña:", type="password")
        
        if st.button("Ingresar"):
            if not username or not password:
                st.error("Por favor, complete todos los campos.")
            else:
                # Verificar credenciales
                password_hash = hashlib.sha256(password.encode()).hexdigest()
                
                try:
                    with engine.connect() as conn:
                        query = text("""
                            SELECT id, username, role, nombre_completo 
                            FROM usuarios 
                            WHERE username = :username AND password = :password
                        """)
                        
                        result = conn.execute(query, {
                            'username': username, 
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
                            
                            st.success("Inicio de sesión exitoso!")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("Usuario o contraseña incorrectos.")
                except Exception as e:
                    st.error(f"Error al verificar credenciales: {e}")
    
    with col2:
        st.image("https://via.placeholder.com/300x200?text=Logo+Sistema", width=300)

def pagina_cargar_archivo():
    """Página para cargar un nuevo archivo CSV"""
    st.header("Cargar Archivo CSV")
    
    # Obtener esquemas existentes
    esquemas = obtener_esquemas_postgres()
    
    # Formulario para subir archivo
    with st.form("upload_form"):
        # Opción para crear nuevo esquema o usar uno existente
        opcion_esquema = st.radio(
            "Seleccione una opción:",
            ["Crear nueva licitación", "Agregar a licitación existente"]
        )
        
        if opcion_esquema == "Crear nueva licitación":
            nuevo_esquema = st.text_input("Nombre de la nueva licitación (esquema):")
        else:
            esquema_seleccionado = st.selectbox(
                "Seleccionar licitación existente:",
                options=esquemas
            )
        
        # Campo para subir archivo
        archivo_csv = st.file_uploader("Seleccionar archivo CSV:", type=["csv"])
        
        # Botón para procesar
        submit = st.form_submit_button("Procesar archivo")
        
        if submit:
            if not archivo_csv:
                st.error("Por favor, seleccione un archivo CSV.")
            elif opcion_esquema == "Crear nueva licitación" and not nuevo_esquema:
                st.error("Por favor, ingrese un nombre para la nueva licitación.")
            else:
                esquema = nuevo_esquema if opcion_esquema == "Crear nueva licitación" else esquema_seleccionado
                
                # Normalizar el nombre del esquema (quitar espacios, etc.)
                esquema = esquema.strip().lower().replace(" ", "_")
                
                # Procesar el archivo
                success, message = cargar_archivo_a_postgres(
                    archivo_csv,
                    archivo_csv.name,
                    esquema
                )
                
                if success:
                    st.success(message)
                else:
                    st.error(message)

def pagina_ver_cargas():
    """Página para ver las cargas realizadas"""
    st.header("Archivos Cargados")
    
    archivos = obtener_archivos_cargados()
    
    if archivos:
        # Convertir a DataFrame para mejor visualización
        df_archivos = pd.DataFrame(archivos)
        
        # Dar formato a las fechas
        if 'fecha_carga' in df_archivos.columns:
            df_archivos['fecha_carga'] = pd.to_datetime(df_archivos['fecha_carga']).dt.strftime('%Y-%m-%d %H:%M')
        
        # Colorear estado
        def colorear_estado(estado):
            if estado == 'Activo':
                return 'background-color: #d4edda; color: #155724'
            elif estado == 'Eliminado':
                return 'background-color: #f8d7da; color: #721c24'
            else:
                return ''
        
        # Aplicar estilo condicional
        df_styled = df_archivos.style.applymap(colorear_estado, subset=['estado'])
        
        # Mostrar DataFrame
        st.dataframe(df_styled)
        
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
                            FROM archivos_cargados
                            WHERE id = :id
                        """)
                        
                        result = conn.execute(query, {'id': archivo_seleccionado['id']})
                        contenido = result.scalar()
                        
                        if contenido:
                            st.download_button(
                                label="Descargar CSV original",
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
                    SELECT id, username, nombre_completo, role, fecha_creacion
                    FROM usuarios
                    ORDER BY username
                """)
                
                result = conn.execute(query)
                
                usuarios = []
                for row in result:
                    usuarios.append({
                        'id': row[0],
                        'username': row[1],
                        'nombre': row[2],
                        'rol': row[3],
                        'fecha_creacion': row[4]
                    })
                
                if usuarios:
                    # Convertir a DataFrame para mejor visualización
                    df_usuarios = pd.DataFrame(usuarios)
                    
                    # Dar formato a las fechas
                    if 'fecha_creacion' in df_usuarios.columns:
                        df_usuarios['fecha_creacion'] = pd.to_datetime(df_usuarios['fecha_creacion']).dt.strftime('%Y-%m-%d %H:%M')
                    
                    # Mostrar usuarios
                    st.dataframe(df_usuarios)
                    
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
                            nombre = st.text_input("Nombre completo:", value=usuario['nombre'])
                            rol = st.selectbox(
                                "Rol:",
                                options=["admin", "user"],
                                index=0 if usuario['rol'] == "admin" else 1
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
                                            # Actualizar usuario
                                            if reset_password and new_password:
                                                # Actualizar con nueva contraseña
                                                password_hash = hashlib.sha256(new_password.encode()).hexdigest()
                                                
                                                query = text("""
                                                    UPDATE usuarios
                                                    SET nombre_completo = :nombre, role = :rol, password = :password
                                                    WHERE id = :id
                                                """)
                                                
                                                conn.execute(query, {
                                                    'nombre': nombre,
                                                    'rol': rol,
                                                    'password': password_hash,
                                                    'id': usuario['id']
                                                })
                                            else:
                                                # Actualizar sin cambiar contraseña
                                                query = text("""
                                                    UPDATE usuarios
                                                    SET nombre_completo = :nombre, role = :rol
                                                    WHERE id = :id
                                                """)
                                                
                                                conn.execute(query, {
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
            username = st.text_input("Nombre de usuario:")
            password = st.text_input("Contraseña:", type="password")
            nombre = st.text_input("Nombre completo:")
            rol = st.selectbox(
                "Rol:",
                options=["user", "admin"]
            )
            
            submit = st.form_submit_button("Crear Usuario")
            
            if submit:
                if not username or not password or not nombre:
                    st.error("Por favor, complete todos los campos.")
                else:
                    try:
                        # Verificar si el usuario ya existe
                        with engine.connect() as conn:
                            query = text("SELECT COUNT(*) FROM usuarios WHERE username = :username")
                            result = conn.execute(query, {'username': username})
                            count = result.scalar()
                            
                            if count > 0:
                                st.error(f"El usuario '{username}' ya existe.")
                            else:
                                # Crear nuevo usuario
                                password_hash = hashlib.sha256(password.encode()).hexdigest()
                                
                                query = text("""
                                    INSERT INTO usuarios (username, password, nombre_completo, role)
                                    VALUES (:username, :password, :nombre, :rol)
                                """)
                                
                                conn.execute(query, {
                                    'username': username,
                                    'password': password_hash,
                                    'nombre': nombre,
                                    'rol': rol
                                })
                                
                                st.success(f"Usuario '{username}' creado exitosamente")
                                time.sleep(1)
                                st.rerun()
                    except Exception as e:
                        st.error(f"Error al crear usuario: {e}")

def pagina_cambiar_password():
    """Página para cambiar contraseña del usuario actual"""
    st.header("Cambiar Contraseña")
    
    with st.form("cambiar_password_form"):
        password_actual = st.text_input("Contraseña actual:", type="password")
        password_nueva = st.text_input("Nueva contraseña:", type="password")
        password_confirmar = st.text_input("Confirmar nueva contraseña:", type="password")
        
        submit = st.form_submit_button("Cambiar Contraseña")
        
        if submit:
            if not password_actual or not password_nueva or not password_confirmar:
                st.error("Por favor, complete todos los campos.")
            elif password_nueva != password_confirmar:
                st.error("Las contraseñas no coinciden.")
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
                                SET password = :password
                                WHERE id = :user_id
                            """)
                            
                            conn.execute(query, {
                                'password': password_hash_nueva,
                                'user_id': st.session_state.user_id
                            })
                            
                            st.success("Contraseña cambiada exitosamente.")
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