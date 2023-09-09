
import zipfile
import requests
from io import BytesIO
import os
import csv
import psycopg2
from psycopg2 import sql

from unidecode import unidecode
import pandas as pd
import boto3

# from airflow.models import Variable

import static

FUNDS_FOLDER = "./cvm-funds"
REFERENCE = "202105"
FILE_NAME = "inf_diario_fi_{}.csv"
DOWNLOAD_URL = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{}.zip"

# s3_client = boto3.client(
#     's3',
#     region_name='us-east-1',
#     aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
#     aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY")
# )

s3_client = boto3.client(
    's3',
    region_name='us-east-1',
    aws_access_key_id="AKIAV3EJ7GT6MSO7WR53",
    aws_secret_access_key="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"
)

reference = "202309"

def downloader():
    try:
        os.makedirs(FUNDS_FOLDER, exist_ok=True)
        file_bytes = BytesIO(
            requests.get(DOWNLOAD_URL.format(reference), verify=False).content
        )
        myzip = zipfile.ZipFile(file_bytes)
        myzip.extractall(FUNDS_FOLDER)
        return True
    except Exception as err:
        print("Error downloading", err)
        return False

def load_raw_to_s3():
    print("Loading raw data to s3")
    fl_name = FILE_NAME.format(reference)
    s3_client.upload_file(
        FUNDS_FOLDER + "/" + fl_name,
        "dl-landing-zone-401868797180",
        f"cvm/raw-data/{fl_name}"
    )
    print("Finishing")
    return True



def _preprocess_data(df_funds):
    df_funds_xp = df_funds[['cnpj', 'nm_fundo', 'classif_cvm']]
    df_funds_xp["cnpj_treated"] = df_funds_xp["cnpj"].astype(str).str.zfill(14)
    df_funds_xp["nm_fundo_treated"] = df_funds_xp['nm_fundo'].apply(transform_text)
    df_funds_xp["classif_cvm_treated"] = df_funds_xp['classif_cvm'].apply(transform_text)

    df_funds_xp = df_funds_xp[['cnpj_treated', 'nm_fundo_treated', 'classif_cvm_treated']]

    new_column_names = {'cnpj_treated': 'cnpj', 'nm_fundo_treated': 'nm_fundo', 'classif_cvm_treated': 'classificacao_cvm'}
    df_funds_xp.rename(columns=new_column_names, inplace=True)

    return df_funds_xp

def transform_data():
    # read cvm
    df_cvm = pd.read_csv(f"cvm-funds/{FILE_NAME.format(reference)}", sep=";")
    df_funds_xp = pd.read_csv("data/base_funds.csv", sep=";")

    if df_cvm is None or df_funds_xp is None:
        return False


    # Preprocess data
    df_cvm["cnpj"] = df_cvm["CNPJ_FUNDO"].str.replace("[./\-]", "", regex=True)

    df_funds_xp = _preprocess_data(df_funds_xp)

    
     # Merge data
    df_joined = pd.merge(df_cvm, df_funds_xp, on='cnpj', how='left')

    # Drop NaN values
    df_final = df_joined.dropna()

    df_final = df_final[
        [
            'cnpj',
            'nm_fundo',
            'classificacao_cvm',
            'DT_COMPTC',
            'VL_TOTAL',
            'VL_QUOTA',
            'VL_PATRIM_LIQ',
            'CAPTC_DIA',
            'RESG_DIA',
            'NR_COTST'
        ]
    ]

    new_column_names = {
        'DT_COMPTC': 'dt_comptc',
        'VL_TOTAL': 'vl_total',
        'VL_QUOTA': 'vl_cota',
        'VL_PATRIM_LIQ': 'vl_pl_liq',
        'CAPTC_DIA': 'captc_dia',
        'RESG_DIA': 'resg_dia',
        'NR_COTST': 'nr_cotst'
    }
    df_final.rename(columns=new_column_names, inplace=True)

    csv_data = df_final.to_csv(index=False)

    s3_client.put_object(
        Bucket="dl-processing-zone-401868797180",
        Key=f"cvm/processed_{reference}.csv",
        Body=csv_data
    )   

    return True


def load_to_dw():
    print("Load to dw")
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
    )
    s3_client.download_file(
        "dl-processing-zone-401868797180",
        f"cvm/processed_{reference}.csv",
        f"processed_{reference}.csv"
    )
    csv_file_path = f'processed_{reference}.csv'

    insert_query = sql.SQL("""
        INSERT INTO cvm (
            cnpj, nm_fundo, classificacao_cvm, dt_comptc,
            vl_total, vl_cota, vl_pl_liq, captc_dia, resg_dia, nr_cotst
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """)

    cursor = conn.cursor()
    count = 0
    with open(csv_file_path, 'r', newline='', encoding='utf-8') as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)  # Skip header row
            for row in csv_reader:
                count += 1
                if count > 10:
                    break
                cursor.execute(insert_query, row)
            # Commit the changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()
    print("Data loaded successfully.")


def transform_text(input_string):
    # Converter para maiúsculas e remover acentos e caracteres especiais
    processed_string = unidecode(input_string.upper())
    return processed_string





# for i in range(1, 9):
#     print(f"Getting ref 20220{i}")
#     ref = f"20220{i}"
#     downloader(ref)
#     load_raw_to_s3(ref)
#     transform_data(ref)
# load_to_dw("202208")
# downloader()
transform_data()