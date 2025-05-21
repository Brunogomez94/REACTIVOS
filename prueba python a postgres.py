import psycopg2

# Datos de conexión
DB_HOST = "localhost"
DB_NAME = "reactivos_db"
DB_USER = "postgres"
DB_PASSWORD = "Dggies12345"
DB_PORT = "5432"

try:
    # Establecer conexión
    connection = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

    # Crear un cursor para ejecutar consultas
    cursor = connection.cursor()

    # Ejecutar una consulta simple
    cursor.execute("SELECT version();")

    # Obtener el resultado
    version = cursor.fetchone()
    print(f"✅ Conexión exitosa a PostgreSQL. Versión: {version[0]}")

    # Cerrar la conexión
    cursor.close()
    connection.close()

except Exception as e:
    print(f"❌ Error al conectar con PostgreSQL: {e}")
