from google.cloud import bigquery
from google.api_core.exceptions import ServiceUnavailable, Forbidden
from google.api_core import retry
import pandas as pd

client = bigquery.Client()  # Se crea el cliente de BigQuery

def load_arriendos(df, table_id, chunk_size=1000):
    """Load the Arriendos data to BigQuery."""
    print("Cargando los datos a BigQuery...")

    # Define el esquema para la tabla
    schema = [
        bigquery.SchemaField("LINK", "STRING"),
        bigquery.SchemaField("TIPO_INMUEBLE", "STRING"),
        bigquery.SchemaField("TIPO_NEGOCIO", "STRING"),
        bigquery.SchemaField("HABITACIONES", "STRING"),
        bigquery.SchemaField("BANIOS", "STRING"),
        bigquery.SchemaField("PRECIO", "FLOAT"),
        bigquery.SchemaField("AREA", "FLOAT"),
        bigquery.SchemaField("CONTACTO", "STRING"),
        bigquery.SchemaField("DIRECCION", "STRING"),
        bigquery.SchemaField("CIUDAD", "STRING"),
        bigquery.SchemaField("BARRIO", "STRING"),
        bigquery.SchemaField("SECTOR", "STRING"),
        bigquery.SchemaField("DEPARTAMENTO", "STRING"),
        bigquery.SchemaField("OTRAS_CARACTERISTICAS", "RECORD", fields=[
            bigquery.SchemaField("ESTRATO", "STRING"),
            bigquery.SchemaField("CLOSET", "STRING"),
            bigquery.SchemaField("GARAJE", "STRING")
            ]),
        bigquery.SchemaField("IMAGENES", "STRING")
    ]

    try:
        # Crear la tabla en BigQuery si no existe
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table, exists_ok=True)
        print(f"Tabla {table_id} creada o ya existe.")

        # Calcular el número total de chunks
        num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size != 0 else 0)

        # Dividir el DataFrame en lotes más pequeños
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, len(df))

            # Seleccionar el chunk del DataFrame usando iloc
            chunk = df.iloc[start_idx:end_idx]
            print(f"Cargando el lote {i+1} de {num_chunks}, filas {start_idx} a {end_idx}...")

            # Cargar cada chunk en BigQuery
            job = client.load_table_from_dataframe(chunk, table)
            job.result()  # Esperar a que el job termine

            print(f"Lote {i+1} cargado con éxito.")

    except ServiceUnavailable as e:
        print(f"Error de servicio al cargar los datos: {e}. Reintentando...")
        retry.Retry(predicate=retry.if_transient_error, deadline=60)
    except Forbidden as e:
        print(f"Error de permisos: {e}. Verifica los permisos del proyecto.")
    except Exception as e:
        print(f"Error inesperado: {e}")


def load_ventas(df, table_id, chunk_size=1000):
    """Load the Ventas data to BigQuery."""
    print("Cargando los datos a BigQuery...")

    # Define el esquema para la tabla
    schema = [
        bigquery.SchemaField("LINK", "STRING"),
        bigquery.SchemaField("TIPO_INMUEBLE", "STRING"),
        bigquery.SchemaField("TIPO_NEGOCIO", "STRING"),
        bigquery.SchemaField("HABITACIONES", "STRING"),
        bigquery.SchemaField("BANIOS", "STRING"),
        bigquery.SchemaField("PRECIO", "FLOAT"),
        bigquery.SchemaField("AREA", "FLOAT"),
        bigquery.SchemaField("CONTACTO", "STRING"),
        bigquery.SchemaField("DIRECCION", "STRING"),
        bigquery.SchemaField("CIUDAD", "STRING"),
        bigquery.SchemaField("BARRIO", "STRING"),
        bigquery.SchemaField("SECTOR", "STRING"),
        bigquery.SchemaField("DEPARTAMENTO", "STRING"),
        bigquery.SchemaField("OTRAS_CARACTERISTICAS", "RECORD", fields=[
            bigquery.SchemaField("ESTRATO", "STRING"),
            bigquery.SchemaField("CLOSET", "STRING"),
            bigquery.SchemaField("GARAJE", "STRING")
            ]),
        bigquery.SchemaField("IMAGENES", "STRING")
    ]

    try:
        # Crear la tabla en BigQuery si no existe
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table, exists_ok=True)
        print(f"Tabla {table_id} creada o ya existe.")

        # Calcular el número total de chunks
        num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size != 0 else 0)

        # Dividir el DataFrame en lotes más pequeños
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, len(df))

            # Seleccionar el chunk del DataFrame usando iloc
            chunk = df.iloc[start_idx:end_idx]
            print(f"Cargando el lote {i+1} de {num_chunks}, filas {start_idx} a {end_idx}...")

            # Cargar cada chunk en BigQuery
            job = client.load_table_from_dataframe(chunk, table)
            job.result()  # Esperar a que el job termine

            print(f"Lote {i+1} cargado con éxito.")

    except ServiceUnavailable as e:
        print(f"Error de servicio al cargar los datos: {e}. Reintentando...")
        retry.Retry(predicate=retry.if_transient_error, deadline=60)
    except Forbidden as e:
        print(f"Error de permisos: {e}. Verifica los permisos del proyecto.")
    except Exception as e:
        print(f"Error inesperado: {e}")