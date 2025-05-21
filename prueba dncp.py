import pandas as pd

# Ruta del archivo
file_path = "C:/Users/Bruno/Downloads/tienda_ordenes.csv"

# Intentamos leer el archivo con la codificación 'utf-8-sig'
try:
    df = pd.read_csv(file_path, delimiter=";", encoding="utf-8-sig")
    print("Archivo leído correctamente:")
    print(df.head())  # Imprime las primeras filas para verificar la carga
except Exception as e:
    print(f"Error al leer el archivo: {e}")
