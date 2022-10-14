#first: import dependencies
import os
import time
import pandas as pd
import mysql.connector

from datetime import time
from threading import Thread
from google.cloud import storage
from fastavro import writer, parse_schema

#second: 
#database credentials
USER = ""
PASSWORD = ""
DATABASE = ""
HOST = ""
# schema credentials
SH_NAME = ""
SH_NAMESPACE = ""
SH_TYPE = ""
#JSON key as enviromental variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""
#bucket credentials
bucket_name = ""
dest_blob_path = ""

#third: query, pandas read sql (will give me a file)
cnx = mysql.connector.connect(user=USER, password=PASSWORD, host=HOST, database=DATABASE)

sql = '''SELECT TOP (1)
    DATE_FORMAT(CLOSED_DT, "%Y-%m-%d") as CLOSED_DT,
    L4,
    STD_JOB_NO,
    CIERRE_EN_TURNO,
    CIERRE_EN_WEEK,
    CIERRE_EN_MONTH
    FROM 
    dbo.OT
    WHERE 
    AREA = "VP MINA" AND L3 = "TRANSPORT"
    AND STD_JOB_NO IN ('CA0411','CA0411', 'CA0408', 'CA2038', 'CM0412')
    AND COMPLETED_CODE = "TT" AND L4 in ("CAMINONESKMS", "CAMIONESLHBR")
'''

df = pd.read_sql(sql, cnx, coerce_float=True)

#fourth: create schema
schema = parse_schema({
    'name': 'EventWriter',
    'namespace': 'avro.cv',
    'type': 'record',
    'fields': [
        {'name':'CLOSED_DT', 'type':'string','doc':'Fecha Termino Trabajo'},
        {'name':'L4', 'type':'string','doc':'Tipo de Camion'},
        {'name':'STD_JOB_NO', 'type':'string','doc':'Trabajo Realizado'},
        {'name':'CIERRE_EN_TURNO', 'type':'string','doc':'Si Trabajo fue terminado durante el turno'},
        {'name':'CIERRE_EN_WEEK', 'type':'string','doc':'Si Trabajo fue terminado hasta 7 días'},
        {'name':'CIERRE_EN_MONTH', 'type':'string','doc':'Si Trabajo fue terminado hasta 30 días'}
    ]
})
##Some functions
def avro_writer(event_list,filename):
    start_time = time.time()
    #Orden la lista de informacion y correspondencia con evento
    rows = [{'CLOSED_DT': event[0],
                'L4': event[1],
                'STD_JOB_NO': event[2],
                'CIERRE_EN_TURNO': event[3],
                'CIERRE_EN_WEEK': event[4],
                'CIERRE_EN_MONTH': event[5]
            } for event in event_list]
    if os.path.exists(filename):
        append_write = 'a+b' # append if already exists
    else:
        append_write = 'wb' # make a new file if not

    records = df.to_dict('records')
    with open('ots.avro', 'wb') as out:
        writer(out, schema, records)
        print("Se escribieron {0} registros en {1} segundos".format(len(rows),time.time() - start_time))


def event_writer(event_list,filename,ext):
    if ext == "avro":
        thread = Thread(target = avro_writer, args = (event_list,filename+".avro"))
    thread.start()

#Fifth File to ext
source_file = 'ots.avro'

#sixth: upload to gcp bucket
def upload_to_bucket(bucket_name: str, source_file: str, dest_blob_path: str):
    """Uploads the csv to avro file to the gco bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(dest_blob_path)
    blob.upload_from_filename(source_file)
    print(f'The {source_file} file has been uploaded to the GCP bucket path: {dest_blob_path}')
    return None

#GCP
upload_to_bucket(bucket_name, source_file, dest_blob_path)