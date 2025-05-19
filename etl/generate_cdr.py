import pyodbc
import random
import datetime
from faker import Faker
from tqdm import tqdm
from dotenv import load_dotenv
import os
import logging
import time
from typing import Optional, Tuple
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import yaml
from concurrent.futures import ThreadPoolExecutor
import calendar
import sys
from contextlib import contextmanager
import backoff

def check_environment():
    """Vérifie que toutes les variables d'environnement nécessaires sont définies"""
    required_vars = [
        'DB_SERVER', 'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_DRIVER',
        'GMAIL_USERNAME', 'GMAIL_APP_PASSWORD', 'RECIPIENT_EMAIL'
    ]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

def check_config():
    """Vérifie que la configuration est valide"""
    required_sections = [
        'generation', 'geography', 'data_types', 'ranges',
        'time_distribution', 'logging', 'database'
    ]
    for section in required_sections:
        if section not in config:
            raise ValueError(f"Missing required configuration section: {section}")

# Load configuration
try:
    with open('config.yml', 'r') as file:
        config = yaml.safe_load(file)
except FileNotFoundError:
    print("Error: config.yml file not found")
    sys.exit(1)
except yaml.YAMLError as e:
    print(f"Error parsing config.yml: {str(e)}")
    sys.exit(1)

# Configure logging
try:
    logging.basicConfig(
        level=getattr(logging, config['logging']['level']),
        format=config['logging']['format'],
        handlers=[
            logging.FileHandler(config['logging']['file']),
            logging.StreamHandler()
        ]
    )
except Exception as e:
    print(f"Error configuring logging: {str(e)}")
    sys.exit(1)

# Load environment variables from .env file
load_dotenv()

# Check environment variables
try:
    check_environment()
except EnvironmentError as e:
    logging.error(str(e))
    sys.exit(1)

# Check configuration
try:
    check_config()
except ValueError as e:
    logging.error(str(e))
    sys.exit(1)

# Constants
BELGIUM_LAT = (config['geography']['latitude']['min'], config['geography']['latitude']['max'])
BELGIUM_LON = (config['geography']['longitude']['min'], config['geography']['longitude']['max'])
table_name = config['database']['table_name']

# Database connection details
server = os.getenv('DB_SERVER')
database = os.getenv('DB_NAME')
username = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
driver = os.getenv('DB_DRIVER')

conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=yes'

fake = Faker('fr_BE')

def get_connection(max_retries: int = 3, retry_delay: int = 5) -> Optional[pyodbc.Connection]:
    """Establish database connection with retry logic"""
    for attempt in range(max_retries):
        try:
            conn = pyodbc.connect(conn_str)
            logging.info("Database connection established successfully")
            return conn
        except Exception as e:
            if attempt < max_retries - 1:
                logging.warning(f"Connection attempt {attempt + 1} failed: {str(e)}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logging.error(f"Failed to connect after {max_retries} attempts: {str(e)}")
                return None

def get_last_inserted_id(cursor) -> int:
    """Get the last inserted ID from the table"""
    try:
        # Vérifier d'abord si la table existe
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = '{table_name}'
        """)
        table_exists = cursor.fetchone()[0] > 0
        
        if not table_exists:
            logging.info(f"Table {table_name} does not exist yet. Starting from ID 0.")
            return 0
            
        cursor.execute(f"SELECT MAX(id) FROM {table_name}")
        result = cursor.fetchone()
        return result[0] if result[0] is not None else 0
    except Exception as e:
        logging.error(f"Error getting last inserted ID: {str(e)}")
        return 0

def send_completion_email():
    """Send email notification when CDR generation is complete"""
    try:
        # Gmail SMTP configuration
        smtp_server = 'smtp.gmail.com'
        smtp_port = 587
        smtp_username = os.getenv('GMAIL_USERNAME')
        smtp_password = os.getenv('GMAIL_APP_PASSWORD')
        sender_email = os.getenv('GMAIL_USERNAME')
        recipient_email = os.getenv('RECIPIENT_EMAIL')

        # Create message
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = 'CDR Generation Complete'

        # Email body
        body = f"""
        CDR Generation Process Completed Successfully
        
        Details:
        - Total Records: {config['generation']['total_records']}
        - Completion Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        - Database: {database}
        - Table: {table_name}
        
        Please check the log file for more details.
        """
        
        msg.attach(MIMEText(body, 'plain'))

        # Send email using Gmail SMTP
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.send_message(msg)
            
        logging.info("Completion email sent successfully")
    except Exception as e:
        logging.error(f"Failed to send completion email: {str(e)}")

def get_operator_code():
    """Get operator code based on market share distribution"""
    operators = config['data_types']['operators']
    weights = [op['market_share'] for op in operators]
    selected_operator = random.choices(operators, weights=weights, k=1)[0]
    return str(selected_operator['code'])

def generate_imei():
    # Generate a random 14-digit number
    imei = ''.join([str(random.randint(0, 9)) for _ in range(14)])
    # Calculate check digit using Luhn algorithm
    total = 0
    for i in range(14):
        digit = int(imei[i])
        if i % 2 == 0:
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    check_digit = (10 - (total % 10)) % 10
    return imei + str(check_digit)

def get_season(date):
    """Determine the season based on the date"""
    month = date.month
    if month in [12, 1, 2]:
        return 'winter'
    elif month in [3, 4, 5]:
        return 'spring'
    elif month in [6, 7, 8]:
        return 'summer'
    else:
        return 'autumn'

def is_holiday(date):
    """Check if the date is a Belgian holiday"""
    # Liste simplifiée des jours fériés belges
    holidays = [
        (1, 1),   # Nouvel An
        (5, 1),   # Fête du Travail
        (7, 21),  # Fête Nationale
        (8, 15),  # Assomption
        (11, 1),  # Toussaint
        (11, 11), # Armistice
        (12, 25)  # Noël
    ]
    return (date.month, date.day) in holidays

def is_rush_hour(hour):
    """Check if the given hour is during rush hour"""
    morning_rush = config['geography']['mobility_patterns']['rush_hours']['morning']
    evening_rush = config['geography']['mobility_patterns']['rush_hours']['evening']
    return morning_rush[0] <= hour <= morning_rush[1] or evening_rush[0] <= hour <= evening_rush[1]

def get_region():
    """Get region based on population density"""
    regions = config['geography']['population_density']
    return random.choices(
        list(regions.keys()),
        weights=list(regions.values()),
        k=1
    )[0]

def generate_signal_strength():
    """Generate realistic signal strength in dBm"""
    return random.randint(
        config['ranges']['signal_strength']['min'],
        config['ranges']['signal_strength']['max']
    )

def generate_random_start_time():
    """Generate random datetime with improved time distribution"""
    base_date = fake.date_this_year()
    rand = random.random()
    
    # Utiliser la distribution temporelle du config
    if rand < config['time_distribution']['business_hours']:
        hour = random.randint(
            config['time_distribution']['business_hours_range']['start'],
            config['time_distribution']['business_hours_range']['end']
        )
    elif rand < (config['time_distribution']['business_hours'] + config['time_distribution']['evening_hours']):
        hour = random.randint(
            config['time_distribution']['evening_hours_range']['start'],
            config['time_distribution']['evening_hours_range']['end']
        )
    else:
        # Heures de nuit
        hour = random.choice(
            list(range(0, config['time_distribution']['business_hours_range']['start'])) +
            list(range(config['time_distribution']['evening_hours_range']['end'] + 1, 24))
        )
    
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return datetime.datetime.combine(base_date, datetime.time(hour, minute, second))

def get_call_type():
    """Get call type based on distribution"""
    types = config['data_types']['call_types']['distribution']
    return random.choices(
        list(types.keys()),
        weights=list(types.values()),
        k=1
    )[0]

def get_call_status():
    """Get call status based on distribution"""
    statuses = config['data_types']['call_status']['distribution']
    return random.choices(
        list(statuses.keys()),
        weights=list(statuses.values()),
        k=1
    )[0]

def get_antenna_type():
    """Get antenna type based on distribution"""
    types = config['data_types']['antenna_types']['distribution']
    return random.choices(
        list(types.keys()),
        weights=list(types.values()),
        k=1
    )[0]

def generate_batch(batch_size, start_id):
    """Generate a batch of CDR records"""
    values = []
    for i in range(batch_size):
        start_time = generate_random_start_time()
        is_weekend = start_time.weekday() >= 5
        is_holiday_day = is_holiday(start_time)
        is_rush_hour_time = is_rush_hour(start_time.hour)
        
        # Appliquer le facteur weekend si nécessaire
        if is_weekend:
            if random.random() > config['geography']['mobility_patterns']['weekend_factor']:
                continue
        
        # Générer les coordonnées
        if random.random() < config['geography']['shared_home_percentage']:
            lat_home, lon_home = random.choice(shared_home_coords)
        else:
            lat_home = round(random.uniform(*BELGIUM_LAT), 6)
            lon_home = round(random.uniform(*BELGIUM_LON), 6)
        
        # Générer les données de l'appel
        call_type = get_call_type()
        status = get_call_status()
        antenna_type = get_antenna_type()
        region = get_region()
        
        record = (
            fake.msisdn(),
            fake.msisdn(),
            start_time,
            random.randint(config['ranges']['duration']['min'], config['ranges']['duration']['max']),
            call_type,
            status,
            generate_imei(),
            generate_imei(),
            get_operator_code(),
            f"ANT{random.randint(config['ranges']['antenna_id']['min'], config['ranges']['antenna_id']['max'])}",
            antenna_type,
            round(random.uniform(*BELGIUM_LAT), 6),
            round(random.uniform(*BELGIUM_LON), 6),
            random.randint(config['ranges']['altitude']['min'], config['ranges']['altitude']['max']),
            f"ANT{random.randint(config['ranges']['antenna_id']['min'], config['ranges']['antenna_id']['max'])}",
            antenna_type,
            round(random.uniform(*BELGIUM_LAT), 6),
            round(random.uniform(*BELGIUM_LON), 6),
            random.randint(config['ranges']['altitude']['min'], config['ranges']['altitude']['max']),
            get_operator_code(),
            lat_home,
            lon_home,
            generate_signal_strength(),
            region,
            1 if is_weekend else 0,
            1 if is_holiday_day else 0,
            1 if is_rush_hour_time else 0,
            get_season(start_time)
        )
        values.append(record)
    return values

def validate_batch_data(batch_data):
    """Validate a batch of CDR records before insertion"""
    validation_results = []
    for record in batch_data:
        is_valid = True
        error_messages = []
        
        # Validate signal strength
        if not (-120 <= record[22] <= -30):  # signal_strength is at index 22
            is_valid = False
            error_messages.append(f"Invalid signal strength: {record[22]}")
            
        # Validate IMEI lengths
        if len(record[6]) != 15 or len(record[7]) != 15:  # imei_caller and imei_receiver
            is_valid = False
            error_messages.append("Invalid IMEI length")
            
        # Validate phone numbers
        if len(record[0]) < 10 or len(record[1]) < 10:  # caller and callee
            is_valid = False
            error_messages.append("Invalid phone number length")
            
        # Validate coordinates
        if not (49.5 <= record[11] <= 51.5) or not (2.5 <= record[12] <= 6.5):  # antenna coordinates
            is_valid = False
            error_messages.append("Invalid antenna coordinates")
            
        validation_results.append({
            'is_valid': is_valid,
            'error_messages': error_messages
        })
    
    return validation_results

def calculate_batch_metrics(batch_data, cursor):
    """Calculate and store metrics for a batch of CDR records"""
    try:
        # Calculate basic metrics
        total_calls = len(batch_data)
        completed_calls = sum(1 for r in batch_data if r[5] == 'completed')  # status is at index 5
        avg_duration = sum(r[3] for r in batch_data) / total_calls  # duration is at index 3
        unique_callers = len(set(r[0] for r in batch_data))  # caller is at index 0
        
        # Store metrics
        cursor.execute("""
            INSERT INTO cdr_metrics (metric_name, metric_value, metric_details)
            VALUES 
                ('batch_total_calls', ?, ?),
                ('batch_completed_calls', ?, ?),
                ('batch_avg_duration', ?, ?),
                ('batch_unique_callers', ?, ?)
        """, (
            total_calls, f"Batch size: {total_calls}",
            completed_calls, f"Completed calls: {completed_calls}",
            avg_duration, f"Average duration: {avg_duration:.2f}",
            unique_callers, f"Unique callers: {unique_callers}"
        ))
        
        logging.info(f"Batch metrics calculated and stored: {total_calls} records processed")
    except Exception as e:
        logging.error(f"Error calculating batch metrics: {str(e)}")

def process_batch(batch_data, cursor, table_name):
    """Process a batch of records with validation and metrics"""
    try:
        # Validate the batch
        validation_results = validate_batch_data(batch_data)
        invalid_records = sum(1 for r in validation_results if not r['is_valid'])
        
        if invalid_records > 0:
            logging.warning(f"Found {invalid_records} invalid records in batch")
            # Log validation errors
            for i, result in enumerate(validation_results):
                if not result['is_valid']:
                    logging.warning(f"Record {i} validation errors: {', '.join(result['error_messages'])}")
        
        # Insert valid records
        valid_records = [record for i, record in enumerate(batch_data) if validation_results[i]['is_valid']]
        if valid_records:
            placeholders = ','.join(['?'] * len(valid_records[0]))
            insert_query = f'''
                INSERT INTO {table_name} (
                    caller, callee, start_time, duration, type, status,
                    imei_caller, imei_receiver, operator_code_emission, antenna_id_emission,
                    antenna_type_emission, antenna_lat_emission, antenna_lon_emission, antenna_altitude_emission,
                    antenna_id_reception, antenna_type_reception, antenna_lat_reception, antenna_lon_reception,
                    antenna_altitude_reception, operator_code_reception, caller_lat_home, caller_lon_home,
                    signal_strength, region, is_weekend, is_holiday, is_rush_hour, season
                ) VALUES ({placeholders})
            '''
            cursor.executemany(insert_query, valid_records)
            
            # Calculate and store metrics
            calculate_batch_metrics(valid_records, cursor)
            
            return len(valid_records)
        return 0
        
    except Exception as e:
        logging.error(f"Error processing batch: {str(e)}")
        raise

def cleanup_existing_objects(cursor):
    """Clean up existing database objects before creating new ones"""
    try:
        # First check if partition objects exist and drop them if they do
        try:
            cursor.execute("""
                IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = 'PS_CDR_DateRange')
                BEGIN
                    DROP PARTITION SCHEME PS_CDR_DateRange;
                END
            """)
        except Exception as e:
            logging.warning(f"Error checking/dropping partition scheme: {str(e)}")

        try:
            cursor.execute("""
                IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PF_CDR_DateRange')
                BEGIN
                    DROP PARTITION FUNCTION PF_CDR_DateRange;
                END
            """)
        except Exception as e:
            logging.warning(f"Error checking/dropping partition function: {str(e)}")

        # Liste des autres objets à supprimer
        objects_to_drop = [
            # Drop views first
            "DROP VIEW IF EXISTS v_cdr_region_stats",
            "DROP VIEW IF EXISTS v_cdr_operator_stats",
            "DROP VIEW IF EXISTS v_cdr_daily_stats",
            
            # Drop stored procedures
            "DROP PROCEDURE IF EXISTS sp_maintain_partitions",
            "DROP PROCEDURE IF EXISTS sp_validate_cdr",
            
            # Drop tables
            "DROP TABLE IF EXISTS cdr_validation_log",
            "DROP TABLE IF EXISTS cdr_metrics",
            "DROP TABLE IF EXISTS cdr_records"
        ]
        
        for drop_statement in objects_to_drop:
            try:
                cursor.execute(drop_statement)
                logging.debug(f"Successfully executed: {drop_statement}")
            except Exception as e:
                logging.warning(f"Error executing {drop_statement}: {str(e)}")
                continue
                
        logging.info("Successfully cleaned up existing database objects")
    except Exception as e:
        logging.error(f"Error during cleanup: {str(e)}")
        raise

def execute_sql_file(cursor, sql_file):
    """Execute SQL file statement by statement"""
    try:
        with open(sql_file, 'r') as file:
            content = file.read()
            
        # Split the content into individual statements
        statements = []
        current_statement = []
        in_procedure = False
        
        for line in content.split('\n'):
            # Skip comments and empty lines
            if line.strip().startswith('--') or not line.strip():
                continue
                
            # Check if we're starting a stored procedure
            if 'CREATE OR ALTER PROCEDURE' in line.upper():
                in_procedure = True
                current_statement = [line]
                continue
                
            # If we're in a procedure, keep adding lines until we hit END;
            if in_procedure:
                current_statement.append(line)
                if line.strip().upper() == 'END;':
                    statements.append('\n'.join(current_statement))
                    current_statement = []
                    in_procedure = False
                continue
                
            # Normal statement handling
            current_statement.append(line)
            if line.strip().endswith(';'):
                statements.append('\n'.join(current_statement))
                current_statement = []
        
        # Add any remaining statement
        if current_statement:
            statements.append('\n'.join(current_statement))
        
        # Execute each statement
        for statement in statements:
            if statement.strip():
                try:
                    # Remove the trailing semicolon if present
                    statement = statement.strip().rstrip(';')
                    cursor.execute(statement)
                    logging.debug(f"Successfully executed: {statement[:100]}...")
                except Exception as e:
                    logging.error(f"Error executing statement: {statement[:100]}...")
                    logging.error(f"Error details: {str(e)}")
                    raise
                    
    except Exception as e:
        logging.error(f"Error reading or executing SQL file: {str(e)}")
        raise

@contextmanager
def managed_connection():
    """Gestionnaire de contexte pour la connexion à la base de données"""
    conn = None
    try:
        conn = get_connection()
        if conn is None:
            raise ConnectionError("Impossible d'établir la connexion à la base de données")
        yield conn
    except Exception as e:
        logging.error(f"Erreur de connexion: {str(e)}")
        raise
    finally:
        if conn:
            try:
                conn.close()
                logging.info("Connexion à la base de données fermée")
            except Exception as e:
                logging.error(f"Erreur lors de la fermeture de la connexion: {str(e)}")

@backoff.on_exception(
    backoff.expo,
    (pyodbc.Error, ConnectionError),
    max_tries=5,
    max_time=300
)
def execute_with_retry(cursor, query, params=None):
    """Exécute une requête avec mécanisme de retry"""
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor
    except pyodbc.Error as e:
        logging.error(f"Erreur lors de l'exécution de la requête: {str(e)}")
        raise

def save_progress(cursor, batch_start: int, total_inserted: int):
    """Sauvegarde la progression dans une table de suivi"""
    try:
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'cdr_generation_progress')
            CREATE TABLE cdr_generation_progress (
                id INT IDENTITY(1,1) PRIMARY KEY,
                batch_start INT,
                total_inserted INT,
                timestamp DATETIME DEFAULT GETDATE()
            )
        """)
        
        cursor.execute("""
            INSERT INTO cdr_generation_progress (batch_start, total_inserted)
            VALUES (?, ?)
        """, (batch_start, total_inserted))
        
        cursor.connection.commit()
        logging.info(f"Progression sauvegardée: batch {batch_start}, total {total_inserted}")
    except Exception as e:
        logging.error(f"Erreur lors de la sauvegarde de la progression: {str(e)}")

def get_last_progress(cursor) -> Tuple[int, int]:
    """Récupère la dernière progression sauvegardée"""
    try:
        cursor.execute("""
            SELECT TOP 1 batch_start, total_inserted
            FROM cdr_generation_progress
            ORDER BY timestamp DESC
        """)
        result = cursor.fetchone()
        return (result[0], result[1]) if result else (0, 0)
    except Exception as e:
        logging.error(f"Erreur lors de la récupération de la progression: {str(e)}")
        return (0, 0)

def process_batch_with_retry(batch_data, cursor, table_name, max_retries=3):
    """Traite un batch avec mécanisme de retry"""
    for attempt in range(max_retries):
        try:
            return process_batch(batch_data, cursor, table_name)
        except (pyodbc.Error, ConnectionError) as e:
            if attempt < max_retries - 1:
                logging.warning(f"Tentative {attempt + 1}/{max_retries} échouée: {str(e)}")
                time.sleep(2 ** attempt)  # Backoff exponentiel
                continue
            else:
                logging.error(f"Échec après {max_retries} tentatives: {str(e)}")
                raise

def main():
    try:
        # Configuration
        TOTAL_RECORDS = config['generation']['total_records']
        BATCH_SIZE = config['generation']['batch_size']
        MAX_WORKERS = config['generation']['parallel_processing']['max_workers']
        
        # Vérifier que le fichier SQL existe
        sql_file = 'sql/01_create_tables.sql'
        if not os.path.exists(sql_file):
            raise FileNotFoundError(f"SQL file not found: {sql_file}")
        
        # Générer les coordonnées partagées
        global shared_home_coords
        shared_home_coords = [
            (round(random.uniform(*BELGIUM_LAT), 6), round(random.uniform(*BELGIUM_LON), 6))
            for _ in range(int(TOTAL_RECORDS * config['geography']['shared_home_percentage']))
        ]
        
        with managed_connection() as conn:
            cursor = conn.cursor()
            
            # Nettoyer les objets existants
            try:
                cleanup_existing_objects(cursor)
                conn.commit()
            except Exception as e:
                logging.error(f"Erreur lors du nettoyage des objets existants: {str(e)}")
                sys.exit(1)
            
            # Créer la table si elle n'existe pas
            try:
                execute_sql_file(cursor, sql_file)
                conn.commit()
                logging.info("Objets de base de données créés avec succès")
            except Exception as e:
                logging.error(f"Erreur lors de la création des objets: {str(e)}")
                sys.exit(1)
            
            # Récupérer la dernière progression
            last_batch_start, total_inserted = get_last_progress(cursor)
            if last_batch_start > 0:
                logging.info(f"Reprise à partir du batch {last_batch_start}, total inséré: {total_inserted}")
                records_to_insert = TOTAL_RECORDS - total_inserted
            else:
                records_to_insert = TOTAL_RECORDS
                total_inserted = 0

            # Génération des données
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                for batch_start in tqdm(range(last_batch_start, records_to_insert, BATCH_SIZE)):
                    try:
                        # Générer le batch
                        batch_data = generate_batch(
                            min(BATCH_SIZE, records_to_insert - batch_start),
                            batch_start
                        )
                        
                        # Traiter le batch avec retry
                        inserted_count = process_batch_with_retry(batch_data, cursor, table_name)
                        total_inserted += inserted_count
                        
                        # Sauvegarder la progression
                        save_progress(cursor, batch_start + BATCH_SIZE, total_inserted)
                        conn.commit()
                        
                        # Log de progression
                        logging.info(f"Batch traité avec succès. {inserted_count} enregistrements insérés. Progression: {total_inserted}/{records_to_insert}")
                        
                    except Exception as e:
                        logging.error(f"Erreur fatale lors du traitement du batch: {str(e)}")
                        # Sauvegarder la progression avant de quitter
                        try:
                            save_progress(cursor, batch_start, total_inserted)
                            conn.commit()
                        except:
                            pass
                        raise

            logging.info(f"Génération des CDR terminée. Total inséré: {total_inserted}")
            print(f"Génération des CDR terminée. Total inséré: {total_inserted}")
            
            # Envoyer l'email de complétion
            send_completion_email()

    except Exception as e:
        logging.error(f"Erreur fatale: {str(e)}")
        raise

if __name__ == "__main__":
    main()
