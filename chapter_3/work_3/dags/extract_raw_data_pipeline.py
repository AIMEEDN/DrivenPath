# 📆 Para definir fechas como start_date en el DAG
from datetime import datetime

# 🛠️ Objeto principal para construir el DAG
from airflow import DAG

# 🔁 Operador vacío usado como punto de inicio visual
from airflow.operators.empty import EmptyOperator

# 🐍 Para ejecutar funciones Python personalizadas dentro del flujo
from airflow.operators.python import PythonOperator

# 🧮 Permite ejecutar instrucciones SQL sobre una conexión configurada
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# 💻 Ejecuta comandos Bash dentro del contenedor (como dbt run)
from airflow.operators.bash import BashOperator

# 📊 Usado para crear y guardar datos como DataFrame
import pandas as pd

# 🗂️ Para manipular carpetas, como crear `/opt/airflow/data` si no existe
import os

# 📁 Función principal: genera un DataFrame de muestra y lo guarda como `.csv`
def save_raw_data():
    print("Extrayendo datos crudos desde la fuente... 🛠️")
    
    df = pd.DataFrame({
        'person_name': ['Alice'],
        'user_name': ['alice_123'],
        'email': ['alice@example.com'],
        'personal_number': [123456789],
        'birth_date': ['1990-01-01'],
        'address': ['123 Main St'],
        'phone': ['+52 123 456 7890'],
        'mac_address': ['00:1A:2B:3C:4D:5E'],
        'ip_address': ['192.168.1.1'],
        'iban': ['MX29 0020 0128 1234 5678'],
        'accessed_at': ['2025-06-30 10:00:00'],
        'session_duration': [120],
        'download_speed': [50],
        'upload_speed': [20],
        'consumed_traffic': [500],
        'unique_id': ['ABC123']
    })

    os.makedirs('/opt/airflow/data/', exist_ok=True)  # 🔧 Crea la carpeta si no existe
    df.to_csv('/opt/airflow/data/raw_data.csv', index=False)  # 💾 Guarda el .csv en la ruta esperada
    print("✅ Archivo guardado como /opt/airflow/data/raw_data.csv")

# ⚙️ Argumentos por defecto para todas las tareas
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

# 🧬 Se define el DAG con cronograma, fecha de inicio y comportamiento
with DAG(
    dag_id='extract_raw_data_pipeline',
    default_args=default_args,
    description='DataDriven Main Pipeline.',
    schedule_interval='0 7 * * *',     # 🕖 Ejecuta diario a las 7:00 AM UTC
    start_date=datetime(2024, 9, 22),  # 🧭 Fecha de inicio
    catchup=False,                     # ⛔ No ejecuta DAGs pasados si se inicia tarde
    tags=['tutorial'],
) as dag:

    # 🟢 Nodo visual de inicio (opcional pero útil para graficar)
    start = EmptyOperator(task_id='start')

    # 🐍 Ejecuta la función `save_raw_data` que genera el CSV
    extract_raw_data_task = PythonOperator(
        task_id='extract_raw_data',
        python_callable=save_raw_data,
    )

    # 🧱 Crea el esquema `driven_raw` si no existe
    create_raw_schema_task = SQLExecuteQueryOperator(
        task_id='create_raw_schema',
        conn_id='postgres_conn',
        sql='CREATE SCHEMA IF NOT EXISTS driven_raw;',
    )

    # 🗃️ Crea la tabla que recibirá el .csv
    create_raw_table_task = SQLExecuteQueryOperator(
        task_id='create_raw_table',
        conn_id='postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS driven_raw.raw_batch_data (
                person_name VARCHAR(100),
                user_name VARCHAR(100),
                email VARCHAR(100),
                personal_number NUMERIC,
                birth_date VARCHAR(100),
                address VARCHAR(100),
                phone VARCHAR(100),
                mac_address VARCHAR(100),
                ip_address VARCHAR(100),
                iban VARCHAR(100),
                accessed_at TIMESTAMP,
                session_duration INT,
                download_speed INT,
                upload_speed INT,
                consumed_traffic INT,
                unique_id VARCHAR(100)
            );
        """,
    )

    # 📨 Carga el .csv a la tabla usando COPY (Postgres debe tener montado ese archivo)
    load_raw_data_task = SQLExecuteQueryOperator(
        task_id='load_raw_data',
        conn_id='postgres_conn',
        sql="""
            COPY driven_raw.raw_batch_data(
                person_name, user_name, email, personal_number, birth_date,
                address, phone, mac_address, ip_address, iban, accessed_at,
                session_duration, download_speed, upload_speed, consumed_traffic,
                unique_id
            )
            FROM '/opt/airflow/data/raw_data.csv'
            DELIMITER ','
            CSV HEADER;
        """,
    )

    # 📀 Ejecuta dbt para el modelo de staging
    run_dbt_staging_task = BashOperator(
        task_id='run_dbt_staging',
        bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:staging',
    )

    # 🏗️ Ejecuta dbt para el modelo trusted (confiable)
    run_dbt_trusted_task = BashOperator(
        task_id='run_dbt_trusted',
        bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:trusted',
    )

    # 🔗 Flujo de ejecución: inicio → extracción + schema → tabla → carga → dbt staging → dbt trusted
    start >> extract_raw_data_task
    [extract_raw_data_task, create_raw_schema_task] >> create_raw_table_task
    create_raw_table_task >> load_raw_data_task >> run_dbt_staging_task >> run_dbt_trusted_task

# 🧠 Expone el DAG al scheduler aunque esté definido en un archivo con nombre distinto
globals()["dag"] = dag