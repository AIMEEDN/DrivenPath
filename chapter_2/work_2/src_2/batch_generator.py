import random 
import csv 
import logging 
import uuid 
import polars as pl 
 
from faker import Faker 
from datetime import date, datetime, timedelta 
 
# Configure logging. 
logging.basicConfig( 
    level=logging.INFO,                     
    format='%(asctime)s - %(levelname)s - %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S', 
    handlers=[logging.StreamHandler()] 
) 

def create_data(locale: str = "es_MX") -> Faker:
    """
    Crea una instancia de Faker para generar datos sintéticos localizados.
    
    Args:
        locale (str): El código de localización para los datos falsos.
    
    Returns:
        Faker: Instancia configurada con la localización especificada.
    """
    logging.info(f"Creando datos sintéticos para el país: {locale.split('_')[-1]}")
    return Faker(locale)

def generate_record(fake: Faker) -> list:
    """
    Genera un único registro falso de usuario con datos variados.

    Args:
        fake (Faker): Instancia de Faker configurada con localización.

    Returns:
        list: Lista con datos sintéticos como nombre, email, IP, etc.
    """

    # Datos personales falsos
    person_name = fake.name()
    user_name = person_name.replace(" ", "").lower()
    email = f"{user_name}@{fake.free_email_domain()}"
    personal_number = fake.ssn()
    birth_date = fake.date_of_birth()
    address = fake.address().replace("\n", ", ")
    phone_number = fake.phone_number()

    # Datos técnicos
    mac_address = fake.mac_address()
    ip_address = fake.ipv4()
    iban = fake.iban()

    # Métricas de conexión
    accessed_at = fake.date_time_between(start_date="-1y", end_date="now")
    session_duration = random.randint(0, 36_000)
    download_speed = random.randint(0, 1_000)
    upload_speed = random.randint(0, 800)
    consumed_traffic = random.randint(0, 2_000_000)

    return [
        person_name, user_name, email, personal_number, birth_date,
        address, phone_number, mac_address, ip_address, iban,
        accessed_at, session_duration, download_speed,
        upload_speed, consumed_traffic
    ]

def write_to_csv(file_path: str, rows: int) -> None:
    """
    Genera múltiples registros de usuario falsos y los guarda en un archivo CSV.

    Args:
        file_path (str): Ruta donde se guardará el archivo CSV.
        rows (int): Cantidad de registros falsos a generar.
    """

    # Instanciar Faker con datos mexicanos
    fake = create_data("es_MX")

    # Encabezados del archivo CSV
    headers = [
        "person_name", "user_name", "email", "personal_number", "birth_date", "address",
        "phone", "mac_address", "ip_address", "iban", "accessed_at",
        "session_duration", "download_speed", "upload_speed", "consumed_traffic"
    ]

    # Escribir archivo
    with open(file_path, mode="w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)

        for _ in range(rows):
            writer.writerow(generate_record(fake))

    # Registrar acción en los logs
    logging.info(f"Se escribieron {rows} registros en el archivo CSV: {file_path}")

def add_id(file_name: str) -> None:
    """
    Agrega una columna 'unique_id' con un UUID único a cada fila del archivo CSV.

    Args:
        file_name (str): Ruta del archivo CSV que será procesado.
    """

    # Cargar CSV a un DataFrame de Polars
    df = pl.read_csv(file_name)

    # Generar lista de UUIDs únicos para cada fila
    uuid_list = [str(uuid.uuid4()) for _ in range(df.height)]

    # Agregar la nueva columna al DataFrame
    df = df.with_columns(pl.Series(name="unique_id", values=uuid_list))

    # Guardar el DataFrame actualizado nuevamente en CSV
    df.write_csv(file_name)

    # Loggear la acción
    logging.info(f"Se añadieron UUIDs únicos al archivo: {file_name}")

def update_datetime(file_name: str, run: str) -> None:
    """
    Actualiza la columna 'accessed_at' en un archivo CSV con el timestamp correspondiente.

    Args:
        file_name (str): Ruta al archivo CSV que será actualizado.
        run (str): Define el tipo de ejecución (e.g. "next" para la fecha de ayer).
    """
    if run == "next":
        # Obtener hora actual sin microsegundos
        current_time = datetime.now().replace(microsecond=0)

        # Calcular la fecha/hora de ayer
        yesterday_time = str(current_time - timedelta(days=1))

        # Leer archivo CSV con Polars
        df = pl.read_csv(file_name)

        # Reemplazar todos los valores en 'accessed_at' por la fecha de ayer
        df = df.with_columns(pl.lit(yesterday_time).alias("accessed_at"))

        # Guardar nuevamente el archivo actualizado
        df.write_csv(file_name)

        # Log de confirmación
        logging.info(f"Actualizada la columna 'accessed_at' con fecha de ayer en: {file_name}")

if __name__ == "__main__":

    # Iniciar el registro del proceso
    logging.info(f"Iniciando procesamiento de lote para {date.today()}.")

    # Definir ruta de salida con fecha de hoy
    output_file = f"data_2/batch_{date.today()}.csv"

    # Definir cantidad de registros
    if str(date.today()) == "2024-09-14":
        records = 100_372
        run_type = "first"
    else:
        records = random.randint(0, 1_000)
        run_type = "next"

    # Generar y guardar datos
    write_to_csv(output_file, records)

    # Agregar columna de UUIDs
    add_id(output_file)

    # Actualizar fecha de acceso si es carga posterior
    update_datetime(output_file, run_type)

    # Finalizar proceso
    logging.info(f"Procesamiento de lote finalizado para {date.today()}.")