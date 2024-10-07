import pandas as pd
import json

def transform_data(properties):
    """Transform the data into a DataFrame."""
    try:
        df = pd.DataFrame(properties)  # Create a DataFrame from the properties
    except Exception as e:
        raise ValueError("Error creating DataFrame: {}".format(e))

    try:
        df.columns = [col.upper() for col in df.columns]  # Convert the column names to uppercase
    except Exception as e:
        raise ValueError("Error converting column names to uppercase: {}".format(e))

    try:
        # Upper case and strip the strings. If the column is not a string, keep the original value
        # Ignore the LINK column
        df.loc[:, df.columns != 'LINK'] = df.loc[:, 
            df.columns != 'LINK'].apply(lambda x: x.astype(str).str.upper().str.strip() 
                                        if x.dtype == 'object' or x.dtype == 'string' else x)
    except Exception as e:
        raise ValueError("Error transforming string columns: {}".format(e))

    try:
        # Replace ARRENDAR with ARRIENDO
        df['TIPO_NEGOCIO'] = df['TIPO_NEGOCIO'].replace('ARRENDAR', 'ARRIENDO', regex=True)
    except Exception as e:
        raise ValueError("Error replacing values in TIPO_NEGOCIO: {}".format(e))

    try:
        # Remove the $ and , from the price column and convert it to float
        df['PRECIO'] = df['PRECIO'].replace('[$,]', '', regex=True).astype(float)
    except Exception as e:
        raise ValueError("Error transforming PRECIO column: {}".format(e))

    try:
        # Remove the M2 from the area column and convert it to float
        df['AREA'] = df['AREA'].replace('[M2,]', '', regex=True).astype(float)
    except Exception as e:
        raise ValueError("Error transforming AREA column: {}".format(e))

    try:
        # Remove the HTTPS://WA.ME/ from the contact column
        df['CONTACTO'] = df['CONTACTO'].replace('HTTPS://WA.ME/', '', regex=True)
    except Exception as e:
        raise ValueError("Error transforming CONTACTO column: {}".format(e))

    try:
        # Replace single quotes with double quotes and convert the column to a dictionary
        df['OTRAS_CARACTERISTICAS'] = df['OTRAS_CARACTERISTICAS'].str.replace("'", '"', regex=True)
        df['OTRAS_CARACTERISTICAS'] = df['OTRAS_CARACTERISTICAS'].astype(str)
        df['OTRAS_CARACTERISTICAS'] = df['OTRAS_CARACTERISTICAS'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
        df['OTRAS_CARACTERISTICAS'] = df['OTRAS_CARACTERISTICAS'].astype(object)
    except Exception as e:
        print ("Error transforming OTRAS_CARACTERISTICAS column: {}".format(e))


    try:
        # Split the DataFrame into two DataFrames: one for arriendos and one for ventas
        df_arriendos = df[df['TIPO_NEGOCIO'] == 'ARRIENDO']
        df_ventas = df[df['TIPO_NEGOCIO'] == 'VENTA']
    except Exception as e:
        raise ValueError("Error splitting DataFrame: {}".format(e))

    return df_arriendos, df_ventas
