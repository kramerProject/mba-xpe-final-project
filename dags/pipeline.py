
import zipfile
import requests
from io import BytesIO
import os

import pandas as pd
import boto3

import static

s3_client = boto3.client(
    's3',
    region_name='us-east-1'
)


def downloader():
    try:
        os.makedirs(static.FUNDS_FOLDER, exist_ok=True)
        file_bytes = BytesIO(
            requests.get(static.DOWNLOAD_URL, verify=False).content
        )
        myzip = zipfile.ZipFile(file_bytes)
        myzip.extractall(static.FUNDS_FOLDER)
        return True
    except Exception as err:
        print("Error downloading", err)
        return False

def load_raw_to_s3():
    print("Loading raw data to s3")
    fl_name = static.FILE_NAME.format(2021, "04")
    s3_client.upload_file(
        static.FUNDS_FOLDER + "/" + fl_name,
        "dl-landing-zone-401868797180",
        f"cvm/raw-data/{fl_name}"
    )
    print("Finishing")
    return True



def transform_data():
    fl_name = static.FILE_NAME.format(2021, "04")
    df = pd.read_csv("cvm-funds/inf_diario_fi_202104.csv", sep=";")

    return df

# downloader()
# load_raw_to_s3()
print(transform_data())