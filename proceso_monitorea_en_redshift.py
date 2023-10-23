from airflow.providers.amazon.aws.transfers.redshift_to_sql import RedshiftToSqlOperator

def insertar_en_redshift(**kwargs):
    # Obtenemos los valores pasados desde el primer proceso utilizando XCom
    ti                 = kwargs['ti']
    Fecha_inicio       = ti.xcom_pull(key='Fecha_inicio')
    Fecha_fin          = ti.xcom_pull(key='Fecha_fin')
    Registros_leidos   = ti.xcom_pull(key='Registros_leidos')
    Registros_procesados  = ti.xcom_pull(key='Registros_procesados')
    Status                = ti.xcom_pull(key='Status')
                    
    # gENERAMOS LA INSERCION
    sql = f"INSERT INTO compras (Fecha_inicio, Fecha_fin, Registros_leidos, Registros_procesados, Status  " \
          f"VALUES ('{Fecha_inicio}', '{Fecha_fin}', {Registros_leidos}, {Registros_procesados}, '{Status}'');"

    # Insertamos los valores en Redshift
    insertar_en_redshift_task = RedshiftToSqlOperator(
        task_id='insertar_en_redshift',
        sql=sql,
        redshift_conn_id="CONEXION_REDSHIFT",
        autocommit=True,
        dag=dag,
    )

dag = DAG(
    'proceso_monitorea_en_redshift',
    schedule_interval=None,
    start_date=datetime(2023, 10, 22),
    catchup=False,
)

insertar_en_redshift_task = PythonOperator(
    task_id='proceso_monitorea_en_redshift',
    python_callable=proceso_monitorea_en_redshift,
    provide_context=True,
    dag=dag,
)
