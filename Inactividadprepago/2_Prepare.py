# Import librerias
import numpy as np
import pandas as pd
import os
import gc
import time
import pandas as pd
from os import path
import json
import argparse
import params_lib as pl
import files_lib as fl
from datetime import datetime, date, timedelta
import s3_lib as s3l

# Parametros
ini = time.time()
now = datetime.now()


def leer_configuracion():
    ruta_parametros = "./config/ParametrosPrepare.json"
    with open(ruta_parametros) as f:
        try:
            parametrosPrepare = json.load(f)
        except IOError as e:
            e = ("El archivo de configuración no puede ser leido.")
            raise Exception(e)
    return parametrosPrepare


def carga_dataset(parametrosPrepare, periodoEjecutado):
    print("Cargando dataset de zona raw")
    rutaArchivo = pl.validar_parametros(
        parametrosPrepare["paths"]["Ruta_Raw_Data"], "La ruta raw del archivo de parametros no puede ser nula")
    extensionArchivo = pl.validar_parametros(
        parametrosPrepare["paths"]["ExtCSV"], "La ruta raw del archivo de parametros no puede ser nula")
    rutaArchivoPreparada = path.join(rutaArchivo, periodoEjecutado)
    print(f"s3_buket -------{rutaArchivoPreparada}")
    prm_aws_s3_bucket=pl.validar_parametros(parametrosPrepare["s3access"]["aws_s3_bucket"], "El parametro bucket es obligatorio.")
    print(f"s3_buket -------{prm_aws_s3_bucket}")
    prm_aws_access_key_id=pl.validar_parametros(parametrosPrepare["s3access"]["aws_access_key_id"], "El parametro access_key_id es obligatorio.")
    prm_aws_secret_access_key=pl.validar_parametros(parametrosPrepare["s3access"]["aws_secret_access_key"], "El parametro secret_access_key es obligatorio.")
    files=fl.find(prm_aws_s3_bucket,rutaArchivoPreparada)
    print(files)
    #files = fl.find(f"*{extensionArchivo}", rutaArchivoPreparada)
    #print(files[0])
    #if len(files) == 0:
        #raise Exception(
            #f"No se encontraron archivos {extensionArchivo} en la ruta: " + rutaArchivoPreparada)
    #global df_ad
    print(files,rutaArchivoPreparada)
    if not files.endswith(extensionArchivo):
        raise Exception(f"No se encontraron archivos {extensionArchivo} en la ruta: {rutaArchivoPreparada} del bucket {prm_aws_s3_bucket}")
    #df_ad = pd.read_csv(files[0].replace("\\", "/"))
    df_ad=s3l.readAndWriteS3(prm_aws_s3_bucket,prm_aws_access_key_id,prm_aws_secret_access_key,f"{rutaArchivoPreparada}/{files}")
    return df_ad


def prepare_dataset(df_ad):
    print("Preparando dataset")
   # col_name = [row[0] for row in cursor.description]
   # df_ad.columns = col_name
   # df_ad
    fin = time.time()
   # print('El proceso ha tardado: ', (fin-ini), 'Segundos')

    columnas = ['ANTIGUEDAD', 'VOZ_MIN_ONNET_M1', 'VOZ_MIN_ONNET_AVG',
                'VOZ_MIN_OFFNET_M1', 'VOZ_MIN_OFFNET_AVG', 'DATOS_MB_M1',
                'DATOS_MB_AVG', 'RECARGAS_QTY_M1', 'RECARGAS_QTY_AVG',
                'RECARGAS_SOL_M1', 'RECARGAS_SOL_AVG', 'QTY_PAQUETES_M1', 'QTY_PAQUETES_AVG',
                'SOLES_PAQUETES_M1', 'SOLES_PAQUETES_AVG', 'MESES_ACTIVOS',
                'RATIO_PAQUETES', 'DIAS_INACTIVIDAD']

    # def estandarizacion(data, variables):

    # for i in tqdm(variables):
    #data[i] = np.log(data[i])
    #data[i] = data[i].replace([np.inf, -np.inf],0)

    # return data

    ini = time.time()
    # read the file
    print("Estandarizando.....")

    #estandarizacion(df_ad, columnas)

    fin = time.time()
    print('El proceso ha tardado: ', (fin-ini), 'Segundos')

    data_prepago_dummies = pd.get_dummies(
        df_ad[['REGION', 'DEPARTAMENTOXIMPORTANCIA']])
    df_ad = pd.concat([df_ad, data_prepago_dummies], axis=1)
    del data_prepago_dummies

    df_ad = df_ad.drop(['REGION', 'DEPARTAMENTOXIMPORTANCIA'], 1)

    columnas = ['ANTIGUEDAD', 'VOZ_MIN_ONNET_M1', 'VOZ_MIN_ONNET_AVG',
                'VOZ_MIN_OFFNET_M1', 'VOZ_MIN_OFFNET_AVG', 'DATOS_MB_M1',
                'DATOS_MB_AVG', 'RECARGAS_QTY_M1', 'RECARGAS_QTY_AVG',
                'RECARGAS_SOL_M1', 'RECARGAS_SOL_AVG', 'QTY_PAQUETES_M1',
                'QTY_PAQUETES_AVG', 'SOLES_PAQUETES_M1', 'SOLES_PAQUETES_AVG',
                'MESES_ACTIVOS', 'RATIO_PAQUETES', 'DIAS_INACTIVIDAD', 'REGION_CENTRO',
                'REGION_LIMA', 'REGION_NORTE', 'REGION_SUR',
                'DEPARTAMENTOXIMPORTANCIA_DEP_TURISTICO',
                'DEPARTAMENTOXIMPORTANCIA_LIMA', 'DEPARTAMENTOXIMPORTANCIA_RESTO']
    return df_ad


def graba_dataset(parametrosPrepare, df_ad, periodoEjecutado):
    print("Grabando dataset de zona analytics")
    rutaSalida = pl.validar_parametros(
        parametrosPrepare["paths"]["Ruta_Analytic_Data"], "La ruta analytics del archivo de parametros no puede ser nula")
    rutaSalidaPreparada = path.join(rutaSalida, periodoEjecutado)
    nombreArchivoSalida = pl.validar_parametros(
        parametrosPrepare["name"]["Name_Analytic_Data"], "El nombre del archivo de salida no puede ser nulo")
    extensionArchivo = pl.validar_parametros(
        parametrosPrepare["paths"]["ExtCSV"], "La extension del archivo de salida no puede ser nula")
    nombreArchivoSalidaFinal = nombreArchivoSalida + \
        periodoEjecutado + extensionArchivo
    nombreArchivoSalidaPreparado = path.join(
        rutaSalidaPreparada, nombreArchivoSalidaFinal)
    #os.makedirs(os.path.dirname(nombreArchivoSalidaPreparado), exist_ok=True)
    prm_aws_s3_bucket=pl.validar_parametros(parametrosPrepare["s3access"]["aws_s3_bucket"], "El parametro bucket es obligatorio.")
    prm_aws_access_key_id=pl.validar_parametros(parametrosPrepare["s3access"]["aws_access_key_id"], "El parametro access_key_id es obligatorio.")
    prm_aws_secret_access_key=pl.validar_parametros(parametrosPrepare["s3access"]["aws_secret_access_key"], "El parametro secret_access_key es obligatorio.")
    #df_ad.to_csv(nombreArchivoSalidaPreparado, index=False)
    s3l.readAndWriteS3(prm_aws_s3_bucket,prm_aws_access_key_id,prm_aws_secret_access_key,nombreArchivoSalidaPreparado,"w",df_ad)


def main():
    parser = argparse.ArgumentParser("prepare")
    parser.add_argument(
        "--periodo",
        type=str,
        help="Periodo de preparación de datos"
    )
    args = parser.parse_args()
    periodo = args.periodo
    parametrosPrepare = leer_configuracion()
    df_ad = carga_dataset(parametrosPrepare, periodo)
    df_ad = prepare_dataset(df_ad)
    graba_dataset(parametrosPrepare, df_ad, periodo)


main()
