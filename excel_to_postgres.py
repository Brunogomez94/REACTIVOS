import pandas as pd
import sqlalchemy
import psycopg2
import os
import csv
import datetime
import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog
from sqlalchemy import create_engine, text, inspect

class ExcelToPostgresThinker:
    """
    Clase que implementa un enfoque de 'thinker' para transferir datos de Excel a PostgreSQL,
    con nombres de tablas predefinidos y registro de archivos procesados.
    """
    
    def __init__(self):
        """Inicializa la clase con valores por defecto"""
        self.excel_file = None
        self.schema_name = None
        # Nombres predefinidos para las tablas
        self.predefined_table_names = [
            "Ejecucion_General",
            "Ejecucion_por_Servicio", 
            "Orden_de_Compra"
        ]
        self.log_file = "excel_to_postgres_log.csv"
        self.connection_params = {
            "user": "postgres",
            "password": "tu_contraseña",
            "host": "localhost",
            "port": "5432",
            "database": "postgres"  # Usar 'postgres' como base de datos por defecto
        }
    
    def pensar_y_decidir(self):
        """Proceso de pensamiento y decisión para la transferencia de datos"""
        # Inicializar interfaz gráfica
        root = tk.Tk()
        root.withdraw()  # Ocultar la ventana principal
        
        # Paso 1: Seleccionar archivo Excel
        self._seleccionar_archivo()
        if not self.excel_file:
            print("No se seleccionó ningún archivo. Operación cancelada.")
            return
        
        # Paso 2: Leer hojas del Excel
        try:
            # Usar encoding='latin1' o 'ISO-8859-1' para manejar caracteres especiales en español
            self.excel_data = {}
            
            # Intentar leer el Excel con diferentes codificaciones
            try:
                # Intentar primero sin especificar encoding (pandas intentará detectarlo)
                self.excel_data = pd.read_excel(self.excel_file, sheet_name=None)
            except UnicodeDecodeError:
                # Si falla, intentar con latin1 que es más permisiva con caracteres especiales
                print("Reintentando lectura del Excel con encoding='latin1'...")
                # Para Excel normalmente no se especifica encoding, pero podemos aplicarlo
                # a los textos después de la lectura
                self.excel_data = pd.read_excel(self.excel_file, sheet_name=None)
                
                # Convertir columnas con encoding adecuado
                for sheet_name, df in self.excel_data.items():
                    # Convertir nombres de columnas
                    new_columns = []
                    for col in df.columns:
                        if isinstance(col, str):
                            try:
                                # Intentar manejar caracteres especiales
                                col = col.encode('latin1').decode('utf-8', errors='replace')
                            except:
                                # Si falla, mantener el original y reemplazar caracteres problemáticos
                                col = col.replace('?', '_')
                        new_columns.append(col)
                    df.columns = new_columns
            
            sheet_names = list(self.excel_data.keys())
            
            # Verificar que hay exactamente 3 hojas
            if len(sheet_names) != 3:
                messagebox.showwarning(
                    "Advertencia", 
                    f"El archivo contiene {len(sheet_names)} hojas, pero se esperaban 3.\n"
                    f"Se necesitan exactamente 3 hojas para asignarlas a las tablas predefinidas:\n"
                    f"- {self.predefined_table_names[0]}\n"
                    f"- {self.predefined_table_names[1]}\n"
                    f"- {self.predefined_table_names[2]}"
                )
                if not messagebox.askyesno("Continuar", "¿Desea continuar de todas formas?"):
                    print("Operación cancelada por el usuario.")
                    return
        except Exception as e:
            error_msg = f"Error al leer el archivo Excel: {str(e)}"
            print(error_msg)
            messagebox.showerror("Error", error_msg)
            return
        
        # Paso 3: Nombrar el esquema
        self._nombrar_esquema()
        if not self.schema_name:
            print("No se proporcionó nombre para el esquema. Operación cancelada.")
            return
        
        # Paso 4: Confirmar la operación
        if self._confirmar_operacion(sheet_names):
            # Paso 5: Ejecutar la transferencia
            if self._transferir_datos(sheet_names):
                # Paso 6: Registrar en el archivo de log
                self._registrar_log()
        
        # Preguntamos si desea procesar otro archivo
        if messagebox.askyesno("Continuar", "¿Desea procesar otro archivo Excel?"):
            self.pensar_y_decidir()  # Reiniciar el proceso
    
    def _seleccionar_archivo(self):
        """Permite al usuario seleccionar un archivo Excel"""
        print("Paso 1: Seleccionar el archivo Excel")
        self.excel_file = filedialog.askopenfilename(
            title="Seleccionar archivo Excel",
            filetypes=[("Archivos Excel", "*.xlsx;*.xls")]
        )
        
        if self.excel_file:
            print(f"Archivo seleccionado: {self.excel_file}")
        
    def _nombrar_esquema(self):
        """Permite al usuario nombrar el esquema de PostgreSQL"""
        print("\nPaso 2: Nombrar el esquema de PostgreSQL")
        self.schema_name = simpledialog.askstring(
            "Nombre del Esquema", 
            "¿Cómo quiere que se llame el esquema?",
            initialvalue="mi_esquema"
        )
        
        if self.schema_name:
            print(f"Nombre del esquema: {self.schema_name}")
    
    def _confirmar_operacion(self, sheet_names):
        """Solicita confirmación al usuario antes de proceder"""
        # Crear un mapeo entre hojas y tablas predefinidas
        sheet_to_table = {}
        for i, sheet in enumerate(sheet_names):
            if i < len(self.predefined_table_names):
                sheet_to_table[sheet] = self.predefined_table_names[i]
            else:
                # Si hay más hojas que nombres predefinidos, usar el nombre de la hoja
                sheet_to_table[sheet] = sheet
        
        mensaje = f"Se realizará la siguiente operación:\n\n" \
                 f"- Archivo Excel: {os.path.basename(self.excel_file)}\n" \
                 f"- Esquema PostgreSQL: {self.schema_name}\n" \
                 f"- Asignación de tablas:\n"
        
        for i, (sheet, table) in enumerate(sheet_to_table.items()):
            mensaje += f"  • Hoja '{sheet}' → Tabla '{table}'\n"
        
        return messagebox.askyesno("Confirmar Operación", mensaje + "\n¿Desea continuar?")
    
    def _limpiar_nombre_columna(self, col_name):
        """
        Limpia y normaliza un nombre de columna para que sea compatible con PostgreSQL
        """
        if not isinstance(col_name, str):
            # Si el nombre no es un string, convertirlo a string
            col_name = str(col_name)
        
        # Reemplazar caracteres especiales y espacios
        col_name = col_name.lower()
        col_name = col_name.replace(' ', '_')
        col_name = col_name.replace('-', '_')
        col_name = col_name.replace('.', '_')
        col_name = col_name.replace('(', '')
        col_name = col_name.replace(')', '')
        col_name = col_name.replace('á', 'a')
        col_name = col_name.replace('é', 'e')
        col_name = col_name.replace('í', 'i')
        col_name = col_name.replace('ó', 'o')
        col_name = col_name.replace('ú', 'u')
        col_name = col_name.replace('ñ', 'n')
        col_name = col_name.replace('ü', 'u')
        col_name = col_name.replace('?', '_')
        
        # Eliminar cualquier caracter que no sea alfanumérico o guion bajo
        col_name = ''.join(c for c in col_name if c.isalnum() or c == '_')
        
        # Asegurarse de que no comienza con un número
        if col_name and col_name[0].isdigit():
            col_name = 'col_' + col_name
            
        # Asegurarse de que no está vacío
        if not col_name:
            col_name = 'columna_sin_nombre'
            
        return col_name
    
    def _transferir_datos(self, sheet_names):
        """Ejecuta la transferencia de datos de Excel a PostgreSQL"""
        try:
            # Crear la cadena de conexión
            conn_str = f"postgresql://{self.connection_params['user']}:" \
                      f"{self.connection_params['password']}@" \
                      f"{self.connection_params['host']}:" \
                      f"{self.connection_params['port']}/" \
                      f"{self.connection_params['database']}"
            
            # Crear conexión a la base de datos
            engine = create_engine(conn_str, client_encoding='utf8')
            
            # Verificar si el esquema existe y crearlo con una consulta segura
            with engine.connect() as conn:
                # Verificar si el esquema existe
                inspector = inspect(engine)
                schemas = inspector.get_schema_names()
                
                if self.schema_name not in schemas:
                    # El esquema no existe, crearlo
                    conn.execute(text(f'CREATE SCHEMA "{self.schema_name}"'))
                    conn.commit()
                    print(f"\nEsquema '{self.schema_name}' creado con éxito")
                else:
                    print(f"\nEsquema '{self.schema_name}' ya existe")
            
            # Procesar cada hoja y crear una tabla para cada una
            for i, (sheet_name, df) in enumerate(self.excel_data.items()):
                # Asignar nombre de tabla predefinido según el índice
                if i < len(self.predefined_table_names):
                    table_name = self.predefined_table_names[i]
                else:
                    # Por si acaso hay más hojas que nombres predefinidos
                    table_name = sheet_name
                
                # Limpiar nombres de columnas para evitar problemas de codificación
                new_columns = {}
                for col in df.columns:
                    new_col_name = self._limpiar_nombre_columna(col)
                    new_columns[col] = new_col_name
                
                # Renombrar columnas
                df = df.rename(columns=new_columns)
                
                # Limpiar datos problemáticos en el DataFrame
                # Reemplazar NaN con None para PostgreSQL
                df = df.where(pd.notnull(df), None)
                
                # Para valores string, asegurarse de que estén codificados correctamente
                for col in df.columns:
                    if df[col].dtype == 'object':
                        # Convertir columnas de tipo objeto/string
                        df[col] = df[col].apply(lambda x: 
                            x.encode('latin1').decode('utf-8', errors='replace') 
                            if isinstance(x, str) else x)
                
                # Crear tabla en PostgreSQL bajo el esquema especificado
                table_full_name = f'"{self.schema_name}"."{table_name}"'
                
                try:
                    # Guardar datos en PostgreSQL
                    df.to_sql(
                        name=table_name,
                        schema=self.schema_name,
                        con=engine,
                        if_exists='replace',  # Opciones: 'fail', 'replace', 'append'
                        index=False,
                        # Especificar que se use UTF-8
                        dtype={col: sqlalchemy.types.Text() for col in df.columns 
                              if df[col].dtype == 'object'}
                    )
                    
                    print(f"Tabla '{table_full_name}' creada y datos importados con éxito")
                    print(f"Número de filas importadas: {len(df)}")
                    
                except Exception as e:
                    print(f"Error al importar tabla '{table_full_name}': {str(e)}")
                    
                    # Intentar un enfoque alternativo si falla
                    print("Intentando método alternativo de importación...")
                    
                    # Convertir todo a strings para evitar problemas de tipos
                    for col in df.columns:
                        df[col] = df[col].astype(str)
                    
                    # Intentar de nuevo con todos los datos como texto
                    df.to_sql(
                        name=table_name,
                        schema=self.schema_name,
                        con=engine,
                        if_exists='replace',
                        index=False,
                        dtype={col: sqlalchemy.types.Text() for col in df.columns}
                    )
                    print(f"Tabla '{table_full_name}' creada con método alternativo")
            
            print("\nTodas las tablas fueron creadas con éxito en PostgreSQL")
            messagebox.showinfo("Éxito", "Transferencia de datos completada con éxito")
            return True
            
        except Exception as e:
            error_msg = f"Error durante la importación: {str(e)}"
            print(error_msg)
            messagebox.showerror("Error", error_msg)
            return False
        
        finally:
            # Cerrar la conexión
            if 'engine' in locals():
                engine.dispose()
    
    def _registrar_log(self):
        """Registra la operación en un archivo CSV de log"""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Verificar si el archivo de log existe
        file_exists = os.path.isfile(self.log_file)
        
        # Campos a registrar
        log_data = {
            'Fecha': timestamp,
            'Archivo_Excel': os.path.basename(self.excel_file),
            'Ruta_Completa': self.excel_file,
            'Esquema': self.schema_name,
            'Tabla1': self.predefined_table_names[0],
            'Tabla2': self.predefined_table_names[1],
            'Tabla3': self.predefined_table_names[2],
            'Estado': 'Completado'
        }
        
        try:
            # Escribir en el archivo CSV con encoding explícito
            with open(self.log_file, mode='a', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=log_data.keys())
                
                # Escribir encabezados si el archivo es nuevo
                if not file_exists:
                    writer.writeheader()
                
                writer.writerow(log_data)
            
            print(f"\nOperación registrada en el archivo de log: {self.log_file}")
            
        except Exception as e:
            print(f"Error al escribir en el archivo de log: {str(e)}")
            # Intentar con otra codificación si falla
            try:
                with open(self.log_file, mode='a', newline='', encoding='latin1') as file:
                    writer = csv.DictWriter(file, fieldnames=log_data.keys())
                    if not file_exists:
                        writer.writeheader()
                    writer.writerow(log_data)
                print(f"Operación registrada con codificación alternativa")
            except Exception as e:
                print(f"No se pudo registrar en el archivo de log: {str(e)}")


# Ejemplo de uso
if __name__ == "__main__":
    # Crear instancia del thinker
    thinker = ExcelToPostgresThinker()
    
    # Iniciar el proceso de pensamiento y decisión
    thinker.pensar_y_decidir()