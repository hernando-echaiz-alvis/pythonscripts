# Import librerias
import time
import pandas as pd
from os import path,mkdir
import os
import numpy as np
import lightgbm as lgb
import gc
import argparse
import params_lib as pl
import files_lib as fl
from os import path
from datetime import datetime
import json
#import oracle_lib as ol
import s3_lib as s3l

# Parametros
ini = time.time()
now = datetime.now()

def leer_configuracion():
    ruta_parametros = "./config/ParametrosSalida.json"
    with open(ruta_parametros) as f:
        try:
            parametrosSalida = json.load(f)
        except IOError as e:
            e = ("El archivo de configuración no puede ser leido.")
            raise Exception(e)    
    return parametrosSalida

def carga_dataset(parametrosSalida, periodoEjecutado):
    print("Cargando dataset de zona analytics")
    rutaArchivo = pl.validar_parametros(parametrosSalida["paths"]["Ruta_Analytic_Data"], "La ruta analytic del archivo no puede ser nula")
    extensionArchivo = pl.validar_parametros(parametrosSalida["paths"]["ExtCSV"], "La extension del archivo no puede ser nula")    
    rutaArchivoPreparada = path.join(rutaArchivo, periodoEjecutado)
    #files = fl.find(f"*{extensionArchivo}", rutaArchivoPreparada)
    #if len(files) == 0:
        #raise Exception(f"No se encontraron archivos {extensionArchivo} en la ruta: " + rutaArchivoPreparada)
    #df_ad = pd.read_csv(files[0].replace("\\","/"),low_memory=False)
    prm_aws_s3_bucket=pl.validar_parametros(parametrosSalida["s3access"]["aws_s3_bucket"], "El parametro bucket es obligatorio.")
    prm_aws_access_key_id=pl.validar_parametros(parametrosSalida["s3access"]["aws_access_key_id"], "El parametro access_key_id es obligatorio.")
    prm_aws_secret_access_key=pl.validar_parametros(parametrosSalida["s3access"]["aws_secret_access_key"], "El parametro secret_access_key es obligatorio.")
    files=fl.find(prm_aws_s3_bucket,rutaArchivoPreparada)
    if not files.endswith(extensionArchivo):
        raise Exception(f"No se encontraron archivos {extensionArchivo} en la ruta: {rutaArchivoPreparada} del bucket {prm_aws_s3_bucket}")
    df_ad=s3l.readAndWriteS3(prm_aws_s3_bucket,prm_aws_access_key_id,prm_aws_secret_access_key,f"{rutaArchivoPreparada}/{files}")
    return df_ad

def carga_predict(parametrosSalida, periodoEjecutado):
    print("Cargando predict de zona analytics temp")
    rutaArchivo = pl.validar_parametros(parametrosSalida["paths"]["Ruta_PredictTemp_Data"], "La ruta predict temp del archivo no puede ser nula")
    extensionArchivo = pl.validar_parametros(parametrosSalida["paths"]["ExtCSV"], "La extension del archivo no puede ser nula")    
    rutaArchivoPreparada = path.join(rutaArchivo, periodoEjecutado)
    #files = fl.find(f"*{extensionArchivo}", rutaArchivoPreparada)
    #if len(files) == 0:
        #raise Exception(f"No se encontraron archivos {extensionArchivo} en la ruta: " + rutaArchivoPreparada)
    #df_predict = pd.read_csv(files[0].replace("\\","/"),low_memory=False)
    prm_aws_s3_bucket=pl.validar_parametros(parametrosSalida["s3access"]["aws_s3_bucket"], "El parametro bucket es obligatorio.")
    prm_aws_access_key_id=pl.validar_parametros(parametrosSalida["s3access"]["aws_access_key_id"], "El parametro access_key_id es obligatorio.")
    prm_aws_secret_access_key=pl.validar_parametros(parametrosSalida["s3access"]["aws_secret_access_key"], "El parametro secret_access_key es obligatorio.")
    files=fl.find(prm_aws_s3_bucket,rutaArchivoPreparada)
    if not files.endswith(extensionArchivo):
        raise Exception(f"No se encontraron archivos {extensionArchivo} en la ruta: {rutaArchivoPreparada} del bucket {prm_aws_s3_bucket}")
    df_predict=s3l.readAndWriteS3(prm_aws_s3_bucket,prm_aws_access_key_id,prm_aws_secret_access_key,f"{rutaArchivoPreparada}/{files}")
    return df_predict


def preparacion_salida(df_ad, df_predict, periodoEjecutado):

    #df_ad['PROBABILIDAD'] = df_predict
    df_m1 = pd.concat([df_ad[['FECHA_CARGA', 'MSISDN']], df_predict], axis = 1)
    df_m1.rename(columns = {'PROBABILIDAD': 'PROB_CHURN'}, inplace = True)
    df_m1['DECIL'] = pd.qcut(df_m1['PROB_CHURN'], 10, labels=False)
    df_m1['DECIL'] = (df_m1['DECIL'] -10)*-1 
    df_m2 = df_m1.sort_values(by = 'DECIL')[['FECHA_CARGA', 'MSISDN', 'PROB_CHURN','DECIL']]
    return df_m2

def guardar_salida(df_salida, parametrosSalida, periodoEjecutado):
    print("Guardando salida de zona analytics")
    rutaArchivo = pl.validar_parametros(parametrosSalida["paths"]["Ruta_Predict_Data"], "La ruta predict del archivo no puede ser nula")
    nombreArchivo = pl.validar_parametros(parametrosSalida["name"]["Name_Predict_Data"], "El nombre del archivo predict no puede ser nulo")
    extensionArchivo = pl.validar_parametros(parametrosSalida["paths"]["ExtCSV"], "La extension del archivo no puede ser nula")    
    rutaArchivoPreparada = path.join(rutaArchivo, periodoEjecutado, nombreArchivo + periodoEjecutado + extensionArchivo)
    #os.makedirs(os.path.dirname(rutaArchivoPreparada), exist_ok = True)
    ini = time.time()
    #df_salida.to_csv(rutaArchivoPreparada, index=False)
    prm_aws_s3_bucket=pl.validar_parametros(parametrosSalida["s3access"]["aws_s3_bucket"], "El parametro bucket es obligatorio.")
    prm_aws_access_key_id=pl.validar_parametros(parametrosSalida["s3access"]["aws_access_key_id"], "El parametro access_key_id es obligatorio.")
    prm_aws_secret_access_key=pl.validar_parametros(parametrosSalida["s3access"]["aws_secret_access_key"], "El parametro secret_access_key es obligatorio.")
    s3l.readAndWriteS3(prm_aws_s3_bucket,prm_aws_access_key_id,prm_aws_secret_access_key,rutaArchivoPreparada,"w",df_salida)
    fin = time.time()
    
def inserta_tabla_scoring(df_ad, parametrosSalida, periodoEjecutado):
    print("escribe tabla de oracle")
    rutaArchivo = pl.validar_parametros(parametrosSalida["paths"]["Ruta_Predict_Data"], "La ruta predict del archivo no puede ser nula")
    nombreArchivo = pl.validar_parametros(parametrosSalida["name"]["Name_Predict_Data"], "El nombre del archivo predict no puede ser nulo")
    extensionArchivo = pl.validar_parametros(parametrosSalida["paths"]["ExtCSV"], "La extension del archivo no puede ser nula")    
    rutaArchivoPreparada = path.join(rutaArchivo, periodoEjecutado, nombreArchivo + periodoEjecutado + extensionArchivo)
    userName = pl.validar_parametros(parametrosSalida["tabla_prediccion"]["userName"], "El parametro userName es obligatorio.")
    password = pl.validar_parametros(parametrosSalida["tabla_prediccion"]["password"], "El parametro password es obligatorio.")
    host = pl.validar_parametros(parametrosSalida["tabla_prediccion"]["host"], "El parametro host es obligatorio.")
    port = pl.validar_parametros(parametrosSalida["tabla_prediccion"]["port"], "El parametro port es obligatorio.")
    sid = pl.validar_parametros(parametrosSalida["tabla_prediccion"]["sid"], "El parametro sid es obligatorio.")
    tablePrefix = pl.validar_parametros(parametrosSalida["tabla_prediccion"]["tablePrefix"], "El parametro tableName es obligatorio.")
    #ol.insertOracleDatabaseTableScoring(userName, password, host, port, sid, tablePrefix, rutaArchivoPreparada)

def main():
    parser = argparse.ArgumentParser("salida")
    parser.add_argument(
        "--periodo",
        type=str,
        help="Periodo de salida. Formato debe ser: YYYYMMDD"
                
    )
    args = parser.parse_args()
    periodo = args.periodo
    parametrosSalida = leer_configuracion()
    df_ad = carga_dataset(parametrosSalida, periodo)
    df_predict = carga_predict(parametrosSalida, periodo)
    df_salida = preparacion_salida(df_ad, df_predict, periodo)
    guardar_salida(df_salida, parametrosSalida, periodo)
    #inserta_tabla_scoring(df_salida, parametrosSalida, periodo)    

main()
