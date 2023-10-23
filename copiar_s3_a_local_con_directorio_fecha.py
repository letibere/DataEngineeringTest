from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_local import S3ToLocalFilesystemOperator
from datetime import datetime
import os

# DAG
dag = DAG(
    'copiar_s3_a_local_con_directorio_fecha',
    schedule_interval="0 0 * * *",  # Frecuencia de ejecución 
    start_date=datetime.now(),  # La fecha en que comenzara el DAG
    catchup=False,
)

# Define la ruta local base donde se guardarán los archivos con directorios dinámicos por fecha
ruta_base_local = '/compras/'
# Define la ruta s3 donde se depositaran los archivos con directorios dinámicos por fecha
ruta_base_s3 = '/clientes/compras/'
# Define una función que generará la ruta local con directorio dinámico basado en la fecha
def generar_ruta_local():
    fecha_actual = datetime.now().strftime('%Y%m%d')
    ruta_local_con_fecha = os.path.join(ruta_base_local, fecha_actual)
    return ruta_local_con_fecha
# Define una función que generará la ruta en s3 con directorio dinámico basado en la fecha
def generar_ruta_s3():
    fecha_actual = datetime.now().strftime('%Y%m%d')
    ruta_s3_con_fecha = os.path.join(ruta_base_s3, fecha_actual)
    return ruta_s3_con_fecha
# Define el operador para copiar desde S3 a local
copy_s3_to_local = S3ToLocalFilesystemOperator(
    task_id='copiar_desde_s3_a_local',
    bucket_name='compras',
    prefix=generar_ruta_s3(),# Utiliza la función para generar la ruta dinámica
    local_file=generar_ruta_local(),  # Utiliza la función para generar la ruta dinámica
    aws_conn_id='[id coneccion a aws]',
    replace=True,  # Reemplazar el archivo local si ya existe
    dag=dag,
)