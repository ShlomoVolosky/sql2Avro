import os
import json
import getsql

import pyodbc
import avro.schema
from avro.io import DatumWriter, DatumReader
from avro.datafile import DataFileWriter, DataFileReader

#Funcion Query Collahuasi Azure DataBase
def collahuasi():
    cnx = pyodbc.connect(Driver='{SQL Server}', Server = "", Database = "", Truested_Connection=yes)
    cons = cnx.cursor()

    consulta = cons.execute('''SELECT TOP (1)
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
''')
    return consulta

#Schema Json
schema = avro.schema.Parse(json.dumps({
    'name': "EventName",
    'namespace': "Space Name",
    'type':"record",
    'fields': [
        {'name':'CLOSED_DT', 'type':'string','doc':'Fecha Termino Trabajo'},
        {'name':'L4', 'type':'string','doc':'Tipo de Camion'},
        {'name':'STD_JOB_NO', 'type':'string','doc':'Trabajo Realizado'},
        {'name':'CIERRE_EN_TURNO', 'type':'string','doc':'Si Trabajo fue terminado durante el turno'},
        {'name':'CIERRE_EN_WEEK', 'type':'string','doc':'Si Trabajo fue terminado hasta 7 días'},
        {'name':'CIERRE_EN_MONTH', 'type':'string','doc':'Si Trabajo fue terminado hasta 30 días'}
    ]
}))

#Creacion Archivo AVRO
def CreateAvro():
    consulta = getsql.collahuasi()
    file = open(filename, 'wb')
    datum_writer = DatumWriter()
    row_writer = DataFileWriter(file, datum_writer, schema)
    for x in consulta:
        row_writer.append({
            'CLOSED_DT':x.CLOSED_DT,
            'L4':x.L4,
            'STD_JOB_NO':x.STD_JOB_NO,
            'CIERRE_EN_TURNO':x.CIERRE_EN_TURNO,
            'CIERRE_EN_WEEK':x.CIERRE_EN_WEEK,
            'CIERRE_EN_MONTH':x.CIERRE_EN_MONTH
        })
    row_writer.close()





