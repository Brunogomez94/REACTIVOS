import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

# Lista de enlaces
links = [
    "https://www.contrataciones.gov.py/convenios-marco/convenio/382392-adquisicion-productos-contingencia-covid-19/compras.html?page=",
    "https://www.contrataciones.gov.py/convenios-marco/convenio/370374-adquisicion-resmas-papel-criterios-sustentabilidad/compras.html?page=",
    "https://www.contrataciones.gov.py/convenios-marco/convenio/373491-adquisicion-utiles-oficina/compras.html?page=",
    "https://www.contrataciones.gov.py/convenios-marco/convenio/373824-adquisicion-elementos-limpieza/compras.html?page=",
    "https://www.contrataciones.gov.py/convenios-marco/convenio/383440-adquisicion-agua-mineral/compras.html?page=",
    "https://www.contrataciones.gov.py/convenios-marco/convenio/386038-adquisicion-productos-uso-medico-lucha-covid-19/compras.html?page=",
    "https://www.contrataciones.gov.py/convenios-marco/convenio/387663-adquisicion-muebles-criterios-sustentabilidad/compras.html?page=",
    "https://www.contrataciones.gov.py/convenios-marco/convenio/395261-adquisicion-midazolam-atracurio-besilato-lucha-covid-19/compras.html?page=",
    "https://www.contrataciones.gov.py/convenios-marco/convenio/397873-adquisicion-muebles-oficina-criterios-sostenibilidad-segundo-llamado/compras.html?page=",
    "https://www.contrataciones.gov.py/convenios-marco/convenio/400008-adquisicion-productos-alimenticios/compras.html?page=",
    "https://www.contrataciones.gov.py/convenios-marco/convenio/400275-adquisicion-productos-uso-medico-lucha-covid-19-grupo-2/compras.html?page=",
    "https://www.contrataciones.gov.py/convenios-marco/convenio/404496-acondicionadores-aire/compras.html?page=",
    "https://www.contrataciones.gov.py/convenios-marco/convenio/412997-incorporacion-articulos-ferreteria-electricidad-tienda-virtual/compras.html?page=",
    "https://www.contrataciones.gov.py/convenios-marco/convenio/415212-incorporacion-pasajes-aereos-tienda-virtual/compras.html?page=",
    "https://www.contrataciones.gov.py/convenios-marco/convenio/419665-incorporacion-repuestos-informaticos-tienda-virtual/compras.html?page=",
    "https://www.contrataciones.gov.py/convenios-marco/convenio/422114-suministro-productos-limpieza-domisanitarios/compras.html?page=",
    "https://www.contrataciones.gov.py/convenios-marco/convenio/422116-suministro-utiles-oficina-estado-paraguayo/compras.html?page=",
    "https://www.contrataciones.gov.py/convenios-marco/convenio/450078-suministro-agua-mineral-traves-tienda-virtual/compras.html?page=",
    "https://www.contrataciones.gov.py/convenios-marco/convenio/452071-suministro-resmas-papel-criterios-sostentabilidad/compras.html?page="
]

# Palabras a excluir
exclusiones = {"Otras Acciones", "Ver Cotizaciones", "Ver Planificación", "Ver Orden de Compra"}

def obtener_max_paginas(url):
    try:
        response = requests.get(url + "1")
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        paginacion = soup.select("ul.pagination li")
        if paginacion:
            return int(paginacion[-2].text)
        return 1
    except Exception:
        return 1

def extraer_datos(url):
    max_pages = obtener_max_paginas(url)
    data = []
    for page in range(1, max_pages + 1):
        try:
            response = requests.get(f"{url}{page}")
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            filas = soup.find_all("tr")
            for fila in filas[1:]:
                columnas = [col.text.strip() for col in fila.find_all("td")]
                columnas = [col for col in columnas if not any(excl in col for excl in exclusiones)]
                enlace = fila.find("a", string="Ver Orden de Compra")
                link = enlace['href'] if enlace else "No Disponible"
                if columnas:
                    columnas.append(link)
                    data.append(columnas)
        except Exception as e:
            print(f"Error en {url}{page}: {e}")
    return data

# Extracción de todos los enlaces
all_data = []
for link in links:
    print(f"Procesando: {link}")
    all_data.extend(extraer_datos(link))

# Crear DataFrame y guardar
df = pd.DataFrame(all_data)
df.to_csv("compras_dncp.csv", index=False)
print("Extracción completada: compras_dncp.csv")
