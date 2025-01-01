"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0 #no esta escrito como mortage si no como mortgage.

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaign_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - cons_price_idx
    - euribor_three_months



    """
    

    return

import os
import zipfile
import pandas as pd

# Función para procesar los archivos comprimidos y generar los tres archivos de salida
def process_marketing_data(input_folder, output_folder):
    # Crear carpeta de salida si no existe
    os.makedirs(output_folder, exist_ok=True)

    # Inicializar listas para almacenar datos
    client_data = []
    campaign_data = []
    economics_data = []

    # Recorrer todos los archivos .zip en la carpeta de entrada
    for file_name in os.listdir(input_folder):
        if file_name.endswith(".csv.zip"):
            file_path = os.path.join(input_folder, file_name)

            # Abrir el archivo .zip y procesar el CSV interno
            with zipfile.ZipFile(file_path, 'r') as z:
                for csv_file in z.namelist():
                    with z.open(csv_file) as f:
                        # Leer el archivo CSV
                        df = pd.read_csv(f)

                        # Procesar columnas para client.csv
                        client = df[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]].copy()
                        client["job"] = client["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
                        client["education"] = client["education"].str.replace(".", "_", regex=False).replace("unknown", pd.NA)
                        client["credit_default"] = client["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
                        client["mortgage"] = client["mortgage"].apply(lambda x: 1 if x == "yes" else 0)
                        client_data.append(client)

                        # Procesar columnas para campaign.csv
                        campaign = df[[
                            "client_id", "number_contacts", "contact_duration",
                            "previous_campaign_contacts", "previous_outcome",
                            "campaign_outcome", "day", "month"
                        ]].copy()
                        campaign["previous_outcome"] = campaign["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
                        campaign["campaign_outcome"] = campaign["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)
                        campaign["last_contact_date"] = pd.to_datetime(
                            campaign["day"].astype(str) + "-" + campaign["month"].astype(str) + "-2022",
                            format="%d-%b-%Y"
                        )
                        campaign = campaign.drop(columns=["day", "month"])
                        campaign_data.append(campaign)

                        # Procesar columnas para economics.csv
                        economics = df[["client_id", "cons_price_idx", "euribor_three_months"]].copy()
                        economics_data.append(economics)

    # Concatenar y guardar los datos
    client_df = pd.concat(client_data, ignore_index=True)
    client_df.to_csv(os.path.join(output_folder, "client.csv"), index=False)

    campaign_df = pd.concat(campaign_data, ignore_index=True)
    campaign_df.to_csv(os.path.join(output_folder, "campaign.csv"), index=False)

    economics_df = pd.concat(economics_data, ignore_index=True)
    economics_df.to_csv(os.path.join(output_folder, "economics.csv"), index=False)

    print(f"Archivos generados en {output_folder}:")
    print("- client.csv")
    print("- campaign.csv")
    print("- economics.csv")

# Rutas de entrada y salida
input_folder = "files/input"
output_folder = "files/output"

# Ejecutar la función
process_marketing_data(input_folder, output_folder)


if __name__ == "__main__":
    clean_campaign_data()
