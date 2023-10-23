from airflow import DAG
from airflow.providers.python.operators.python_operator import PythonOperator
from datetime import datetime
import os
import json
import pyodbc
dag = DAG(
    'leer_json_local_dag',
    schedule_interval=None,  # Define la frecuencia de ejecución o utiliza un disparador externo
    start_date=datetime(2023, 4, 10),  # La fecha en la que quieres que comience el DAG
    catchup=False,
)

# Define la ruta local base donde se guardarán los archivos con directorios dinámicos por fecha
ruta_base_local = '/compras/'
# Define una función que generará la ruta local con directorio dinámico basado en la fecha
def generar_ruta_local():
    fecha_actual = datetime.now().strftime('%Y%m%d')
    ruta_local_con_fecha = os.path.join(ruta_base_local, fecha_actual)
    return ruta_local_con_fecha 
# Conectar a la base de datos SQL Server
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=nombre_del_servidor;DATABASE=nombre_de_la_base_de_datos;UID=usuario;PWD=contraseña')

# Imprimir los resultados
for resultado in resultados:
    print("customer_id:", resultado.customer_id)

def leer_json_local():
    ruta_json_local = generar_ruta_local(),  # Utiliza la función para generar la ruta dinámica
    cursor_rfc             = conn.cursor()
    cursor_nombre_articulo = conn.cursor()
    cursor_nombre_tienda   = conn.cursor()
    cont_reg   = 0
    # Usamos XCom para pasar los valores a la siguiente tarea
    fecha_actual = datetime.now()
    timestamp_inicio = fecha_actual.timestamp()
    kwargs['ti'].xcom_push(key='Fecha_inicio', value=timestamp_inicio)
    kwargs['ti'].xcom_push(key='Registros_leidos', value=0)
    kwargs['ti'].xcom_push(key='Registros_procesados', value=0)
    kwargs['ti'].xcom_push(key='Status', value=1)
    with open(ruta_json_local, 'r') as archivo_json:
        data = json.load(archivo_json)

    for elemento in data:
        cont_reg           = cont_reg + 1
        customer_id        = elemento["customer_id"]
        store_id           = elemento["store_id"]
        creation_timestamp = elemento["creation_timestamp"]
        items_bought       = elemento["items_bought"]    
        # se define zona horaria   de UTC
        utc_timezone = pytz.utc
        # Define la zona horaria de la Ciudad de México
        mexico_timezone = pytz.timezone('America/Mexico_City')
        # Convierte la fecha y hora actual a la zona horaria de la Ciudad de México
        fecha_hora_mexico = creation_timestamp.astimezone(mexico_timezone)
        # Obtiene la fecha en formato AAAA-MM-DD
        fecha_formateada = fecha_hora_mexico.strftime('%Y-%m-%d')

        # Crear consulta  RFC
        consulta_rfc = "SELECT UPPER(RFC) FROM clientes WHERE cliente_id = ?"
        # Ejecutar consulta RFC
        cursor_rfc.execute(consulta_rfc, customer_id)
        # Recuperar RFC
        rfc_cliente = cursor_rfc.fetchone()
        # Crear consulta  nombre_tienda
        consulta_nombre_tienda = "SELECT REPLACE(UPPER(nombre_tienda),'[^A-Z0-9]', '') FROM tiendas WHERE tienda_id  = ?"
        # Ejecutar consulta nombre_tienda
        cursor_nombre_tienda.execute(consulta_nombre_tienda, store_id)
        # Recuperar nombre_tienda
        nombre_tienda = cursor_nombre_tienda.fetchone()
# Procesa los elementos dentro de "items_bought"
        cantidad_comprada = 0
        total_comprado    = 0
        item_id_ant       = 0
        cont              = 0
        for item in items_bought:
            item_id = item["item_id"]
            quantity = item["quantity"]
            total_price = item["total_price"]
            if item_id_ant <> item_id
                if cont = 0
                    # Crear consulta  nombre_articulo
                    consulta_nombre_articulo = "SELECT REPLACE(UPPER(nombre_articulo),'[^A-Z0-9]', '') FROM articulos WHERE articulo_id = ?"
                    # Ejecutar consulta nombre_articulo
                    cursor_nombre_articulo.execute(consulta_nombre_articulo, item_id)
                    # Recuperar nombre_articulo
                    nombre_articulo = cursor_nombre_articulo.fetchone()
                else :
                    cont = 0
                    total_comprado_mx = total_comprado *  18.24
                     # Usamos XCom para pasar los valores a la siguiente tarea
                     xcom_push(key='rfc_cliente', value=rfc_cliente)
                     xcom_push(key='nombre_articulo', value=nombre_articulo)
                     xcom_push(key='cantidad_comprada', value=cantidad_comprada)
                     xcom_push(key='customer_id', value=customer_id)
                     xcom_push(key='nombre_tienda', value=nombre_tienda)
                     xcom_push(key='total_comprado_mx', value=total_comprado_mx)
                     xcom_push(key='fecha_formateada', value=fecha_formateada)
            else :
                cantidad_comprada = cantidad_comprada + quantity
                total_comprado = total_comprado + total_price
                cont = cont + 1
            item_id_ant= item_id 
    fecha_final = datetime.now()
    timestamp_final = fecha_final.timestamp()
    xcom_push(key='Fecha_inicio', value=timestamp_inicio)
    xcom_push(key='Fecha_fin', value=timestamp_final)
    xcom_push(key='Registros_leidos', value=cont_reg)
    xcom_push(key='Registros_procesados', value=cont_reg)
    xcom_push(key='Status', value=2)           
            
            
procesar_json_local = PythonOperator(
    task_id='procesar_json_local',
    python_callable=leer_json_local,
    dag=dag,
)
# Cerrar la conexión
conn.close()




