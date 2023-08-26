
import zipfile
import requests
from io import BytesIO
import os

from unidecode import unidecode
import pandas as pd
import boto3

import static

s3_client = boto3.client(
    's3',
    region_name='us-east-1'
)



def downloader(reference):
    try:
        os.makedirs(static.FUNDS_FOLDER, exist_ok=True)
        file_bytes = BytesIO(
            requests.get(static.DOWNLOAD_URL.format(reference), verify=False).content
        )
        myzip = zipfile.ZipFile(file_bytes)
        myzip.extractall(static.FUNDS_FOLDER)
        return True
    except Exception as err:
        print("Error downloading", err)
        return False

def load_raw_to_s3(reference):
    print("Loading raw data to s3")
    fl_name = static.FILE_NAME.format(reference)
    s3_client.upload_file(
        static.FUNDS_FOLDER + "/" + fl_name,
        "dl-landing-zone-401868797180",
        f"cvm/raw-data/{fl_name}"
    )
    print("Finishing")
    return True



def transform_data(reference):
    df_cvm = pd.read_csv(f"cvm-funds/{static.FILE_NAME.format(reference)}", sep=";")

    df_cvm["cnpj"] = df_cvm["CNPJ_FUNDO"].str.replace("[./\-]", "", regex=True)

    df_funds_xp = pd.read_csv("files/base_funds.csv", sep=";")

    df_funds_xp = df_funds_xp[['cnpj', 'nm_fundo', 'classif_cvm']]


    df_funds_xp["cnpj_treated"] = df_funds_xp["cnpj"].astype(str).str.zfill(14)
    df_funds_xp["nm_fundo_treated"] = df_funds_xp['nm_fundo'].apply(transform_text)
    df_funds_xp["classif_cvm_treated"] = df_funds_xp['classif_cvm'].apply(transform_text)

    df_funds_xp = df_funds_xp[['cnpj_treated', 'nm_fundo_treated', 'classif_cvm_treated']]

    new_column_names = {'cnpj_treated': 'cnpj', 'nm_fundo_treated': 'nm_fundo', 'classif_cvm_treated': 'classificacao_cvm'}
    df_funds_xp.rename(columns=new_column_names, inplace=True)
    
    df_joined = pd.merge(df_cvm, df_funds_xp, on='cnpj', how='left')

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




def transform_text(input_string):
    # Converter para maiúsculas e remover acentos e caracteres especiais
    processed_string = unidecode(input_string.upper())
    return processed_string


for i in range(1, 9):
    print(f"Getting ref 20220{i}")
    ref = f"20220{i}"
    downloader(ref)
    load_raw_to_s3(ref)
    transform_data(ref)