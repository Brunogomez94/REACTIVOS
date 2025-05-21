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
                                st.button("Ver Orden Creada")
                            else:
                                st.error(message)
            else:
                st.warning(f"No hay servicios beneficiarios definidos para la licitación seleccionada.")
        else:
            st.info("Seleccione una licitación para emitir una orden de compra.")

def main():
    st.set_page_config(
        page_title="Gestión de Licitaciones",
        page_icon="📊",
        layout="wide"
    )
    
    # Configurar tablas de usuarios y órdenes de compra si no existen
    configurar_tabla_usuarios()
    configurar_tabla_ordenes_compra()
    
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

if __name__ == "__main__":
    main()