#Import librerias
import time
import pandas as pd
from os import path,mkdir
import os
#import numpy as np
#import lightgbm as lgb
import gc
import joblib
import argparse
import params_lib as pl
import files_lib as fl
from os import path
from datetime import datetime
import json 
import s3_lib as s3l

#Parametros
ini = time.time()
now = datetime.now()

def leer_configuracion():
    ruta_parametros = "./config/ParametrosScoring.json"
    with open(ruta_parametros) as f:
        try:
            parametrosScoring = json.load(f)
        except IOError as e:
            e = ("El archivo de configuración no puede ser leido.")
            raise Exception(e)    
    return parametrosScoring

def carga_dataset(parametrosScoring, periodoEjecutado):
    print("Cargando dataset de zona analytics")
    rutaArchivo = pl.validar_parametros(parametrosScoring["paths"]["Ruta_Analytic_Data"], "La ruta raw del archivo de parametros no puede ser nula")
    extensionArchivo = pl.validar_parametros(parametrosScoring["paths"]["ExtCSV"], "La ruta raw del archivo de parametros no puede ser nula")    
    rutaArchivoPreparada = path.join(rutaArchivo, periodoEjecutado)
    #files = fl.find(f"*{extensionArchivo}", rutaArchivoPreparada)
    #if len(files) == 0:
        #raise Exception(f"No se encontraron archivos {extensionArchivo} en la ruta: " + rutaArchivoPreparada)
    #print(files[0])
    #df_ad = pd.read_csv(files[0].replace("\\","/"),low_memory=False)
    prm_aws_s3_bucket=pl.validar_parametros(parametrosScoring["s3access"]["aws_s3_bucket"], "El parametro bucket es obligatorio.")
    prm_aws_access_key_id=pl.validar_parametros(parametrosScoring["s3access"]["aws_access_key_id"], "El parametro access_key_id es obligatorio.")
    prm_aws_secret_access_key=pl.validar_parametros(parametrosScoring["s3access"]["aws_secret_access_key"], "El parametro secret_access_key es obligatorio.")
    files=fl.find(prm_aws_s3_bucket,rutaArchivoPreparada)
    if not files.endswith(extensionArchivo):
        raise Exception(f"No se encontraron archivos {extensionArchivo} en la ruta: {rutaArchivoPreparada} del bucket {prm_aws_s3_bucket}")
    df_ad=s3l.readAndWriteS3(prm_aws_s3_bucket,prm_aws_access_key_id,prm_aws_secret_access_key,f"{rutaArchivoPreparada}/{files}")
    return df_ad

def prepara_nombre_modelo(parametrosScoring):
    nombreModelo = pl.validar_parametros(parametrosScoring["name"]["Name_Analytic_Model"], "El nombre del modelo no puede ser nulo")
    rutaModelo = pl.validar_parametros(parametrosScoring["paths"]["Ruta_Analytic_Model"], "La ruta del modelo no puede ser nula")
    return path.join(rutaModelo, nombreModelo)

def scoring (df_ad, parametrosScoring):
    print("Ejecutando Scoring")
    nombreModelo = prepara_nombre_modelo(parametrosScoring)
    train_columnsf = [
    'ANTIGUEDAD', 
    'VOZ_MIN_ONNET_M1', 
    'VOZ_MIN_ONNET_AVG',
    'VOZ_MIN_OFFNET_M1', 
    'VOZ_MIN_OFFNET_AVG', 
    'DATOS_MB_M1',
    'DATOS_MB_AVG', 
    'RECARGAS_QTY_M1', 
    'RECARGAS_QTY_AVG',
    'RECARGAS_SOL_M1', 
    'RECARGAS_SOL_AVG', 
    'QTY_PAQUETES_M1',
    'QTY_PAQUETES_AVG', 
    'SOLES_PAQUETES_M1', 
    'SOLES_PAQUETES_AVG',
    'MESES_ACTIVOS', 
    'RATIO_PAQUETES', 
    'DIAS_INACTIVIDAD', 
    'REGION_CENTRO',
    'REGION_LIMA', 
    'REGION_NORTE', 
    'REGION_SUR',
    'DEPARTAMENTOXIMPORTANCIA_DEP_TURISTICO',
    'DEPARTAMENTOXIMPORTANCIA_LIMA', 
    'DEPARTAMENTOXIMPORTANCIA_RESTO'
    ]

    #Ejecución del modelo
    model = joblib.load(nombreModelo)
    ini = time.time()
    predict_valid_m1 = model.predict(df_ad[(train_columnsf)] ,num_iteration=model.best_iteration)
    predict_valid_m1 = pd.DataFrame(predict_valid_m1)

    #df_m1 = pd.concat([df_ad[['FECHA_CARGA', 'MSISDN']], predict_valid_m1], axis = 1)
    #df_m1.rename(columns = {0: 'PROB_CHURN'}, inplace = True)

    fin = time.time()
    print('Tiempo de ejecucion', (fin-ini)/60 , 'minutos')
    predict_valid_m1.columns = ["PROBABILIDAD"]

    return predict_valid_m1

def prepara_nombre_salida(parametrosScoring, periodoEjecutado):
    nombreSalida = pl.validar_parametros(parametrosScoring["name"]["Name_PredictTemp_Data"], "El nombre del archivo de salida no puede ser nulo")
    rutaSalida = pl.validar_parametros(parametrosScoring["paths"]["Ruta_PredictTemp_Data"], "La ruta del archivo de salida no puede ser nula")
    extensionArchivo = pl.validar_parametros(parametrosScoring["paths"]["ExtCSV"], "La extension del archivo de salida no puede ser nula")
    rutaSalidaPreparada = path.join(rutaSalida, periodoEjecutado)
    print(path.join(rutaSalidaPreparada, nombreSalida, periodoEjecutado, extensionArchivo))
    
    return path.join(rutaSalidaPreparada, nombreSalida + periodoEjecutado + extensionArchivo)

def guardar_salida(predict_s4t, parametrosScoring, periodoEjecutado):
    print("Guardando salida")
    nombreSalida = prepara_nombre_salida(parametrosScoring, periodoEjecutado)
    #os.makedirs(os.path.dirname(nombreSalida), exist_ok = True)
    prm_aws_s3_bucket=pl.validar_parametros(parametrosScoring["s3access"]["aws_s3_bucket"], "El parametro bucket es obligatorio.")
    prm_aws_access_key_id=pl.validar_parametros(parametrosScoring["s3access"]["aws_access_key_id"], "El parametro access_key_id es obligatorio.")
    prm_aws_secret_access_key=pl.validar_parametros(parametrosScoring["s3access"]["aws_secret_access_key"], "El parametro secret_access_key es obligatorio.")
    s3l.readAndWriteS3(prm_aws_s3_bucket,prm_aws_access_key_id,prm_aws_secret_access_key,nombreSalida,"w",predict_s4t)
    #predict_s4t.to_csv(nombreSalida, index=False)

def main():
    parser = argparse.ArgumentParser("prepare")
    parser.add_argument(
        "--periodo",
        type=str,
        help="Periodo de preparación de datos"
    )
    args = parser.parse_args()
    periodo = args.periodo
    parametrosScoring = leer_configuracion()
    df_ad = carga_dataset(parametrosScoring, periodo)
    df_predict = scoring(df_ad, parametrosScoring)
    guardar_salida(df_predict, parametrosScoring, periodo)

main()
