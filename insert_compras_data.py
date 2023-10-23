from airflow.providers.amazon.aws.transfers.redshift_to_sql import RedshiftToSqlOperator

def insertar_en_redshift(**kwargs):
    # Obtenemos los valores pasados desde el primer proceso utilizando XCom
    ti                = kwargs['ti']
    rfc_cliente       = ti.xcom_pull(key='rfc_cliente')
    nombre_articulo   = ti.xcom_pull(key='nombre_articulo')
    cantidad_comprada = ti.xcom_pull(key='cantidad_comprada')
    customer_id       = ti.xcom_pull(key='customer_id')
    nombre_tienda     = ti.xcom_pull(key='nombre_tienda')
    total_comprado_mx = ti.xcom_pull(key='total_comprado_mx')
    fecha_formateada  = ti.xcom_pull(key='fecha_formateada')
                    
    # gENERAMOS LA INSERCION
    sql = f"INSERT INTO compras (rfc_cliente, nombre_articulo, cantidad_comprada, id_articulo, nombre_tienda, total_comprado, fecha_compra) " \
          f"VALUES ('{rfc_cliente}', '{nombre_articulo}', {cantidad_comprada}, {customer_id}, '{nombre_tienda}', {total_comprado_mx}, '{fecha_formateada}');"

    # Insertamos los valores en Redshift
    insertar_en_redshift_task = RedshiftToSqlOperator(
        task_id='insertar_en_redshift',
        sql=sql,
        redshift_conn_id="CONEXION_REDSHIFT",
        autocommit=True,
        dag=dag,
    )

dag = DAG(
    'proceso_insertar_en_redshift',
    schedule_interval=None,
    start_date=datetime(2023, 10, 22),
    catchup=False,
)

insertar_en_redshift_task = PythonOperator(
    task_id='insertar_en_redshift',
    python_callable=insertar_en_redshift,
    provide_context=True,
    dag=dag,
)
