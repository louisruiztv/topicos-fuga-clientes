from pymongo import MongoClient
import pandas as pd

def consultar_fuga_clientes():
    # 1. Configuración de conexión a MongoDB
    # Se utiliza la base de datos especificada: topicosa_workload
    client = MongoClient('mongodb://localhost:27017/')
    db = client['topicosa_workload']
    collection = db['fuga_clientes']

    print("--- Verificando persistencia en MongoDB ---")

    # 2. Consulta de validación: Conteo total de registros
    # Debe coincidir con los 7,043 registros del dataset original
    total_registros = collection.count_documents({})
    print(f"Total de registros encontrados: {total_registros}")

    # 3. Consulta de KPIs específicos (Ejemplo: Clientes Fugados)
    # Filtro basado en la variable 'label' o 'Churn' binarizada
    fugados = collection.count_documents({"label": 1})
    print(f"Total clientes en estado de fuga: {fugados}")

    # 4. Extracción de una muestra para validación de esquema
    # Verifica la presencia de campos clave como CLTV y monthlycharges
    muestra = collection.find_one({}, {"_id": 0, "customerid": 1, "monthlycharges": 1, "CLTV": 1})
    print(f"Muestra de datos técnicos: {muestra}")

    client.close()

if __name__ == "__main__":
    consultar_fuga_clientes()