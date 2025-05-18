import pyodbc
import random
import datetime
from faker import Faker
from tqdm import tqdm
from dotenv import load_dotenv
import os
import logging
import time
from typing import Optional
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import yaml

# Load configuration
with open('config.yml', 'r') as file:
    config = yaml.safe_load(file)

# Configure logging
logging.basicConfig(
    level=getattr(logging, config['logging']['level']),
    format=config['logging']['format'],
    handlers=[
        logging.FileHandler(config['logging']['file']),
        logging.StreamHandler()
    ]
)

# Load environment variables from .env file
load_dotenv()

fake = Faker('fr_BE')

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

# MSSQL Server connection details from environment variables
# Get database configuration from environment variables
server = os.getenv('DB_SERVER')
database = os.getenv('DB_NAME')
username = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
driver = os.getenv('DB_DRIVER')
table_name = os.getenv('CDR_TABLE')


conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=yes'

# Number of records to insert
TOTAL_RECORDS = 10_000_000
BATCH_SIZE = 10_000  # Optimized batch size for better performance

# Function to generate random datetime within a given hour range
def generate_random_start_time():
    rand = random.random()
    base_date = fake.date_this_year()
    if rand < 0.5:
        hour = random.randint(7, 18)
        minute = random.randint(0, 59)
    elif rand < 0.9:
        hour = random.randint(19, 21)
        minute = random.randint(0, 59)
    else:
        hour = random.choice(list(range(22, 24)) + list(range(0, 7)))
        minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return datetime.datetime.combine(base_date, datetime.time(hour, minute, second))

# Belgium lat/lon bounding box
BELGIUM_LAT = (49.5, 51.5)
BELGIUM_LON = (2.5, 6.5)

# Generate shared home coordinates for 40% of callers
shared_home_coords = [(round(random.uniform(*BELGIUM_LAT), 6), round(random.uniform(*BELGIUM_LON), 6)) for _ in range(int(TOTAL_RECORDS * 0.4))]

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
        smtp_password = os.getenv('GMAIL_APP_PASSWORD')  # Use App Password, not regular password
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
        - Total Records: {TOTAL_RECORDS}
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

# Main execution
try:
    conn = get_connection()
    if conn is None:
        logging.error("Could not establish database connection. Exiting.")
        exit(1)

    cursor = conn.cursor()
    
    # Get the last inserted ID to resume from
    last_id = get_last_inserted_id(cursor)
    if last_id > 0:
        logging.info(f"Resuming from ID {last_id}")
        records_to_insert = TOTAL_RECORDS - last_id
    else:
        records_to_insert = TOTAL_RECORDS

    # Create the table if it doesn't exist and add indexes
    cursor.execute(f'''
        IF OBJECT_ID('{table_name}', 'U') IS NULL
        BEGIN
            CREATE TABLE {table_name} (
                id INT IDENTITY(1,1) PRIMARY KEY,
                caller VARCHAR(20),
                callee VARCHAR(20),
                start_time DATETIME,
                duration INT,
                type VARCHAR(10),
                status VARCHAR(10),
                imei_caller VARCHAR(20),
                imei_receiver VARCHAR(20),
                operator_code_emission VARCHAR(10),
                antenna_id_emission VARCHAR(20),
                antenna_type_emission VARCHAR(10),
                antenna_lat_emission FLOAT,
                antenna_lon_emission FLOAT,
                antenna_altitude_emission INT,
                antenna_id_reception VARCHAR(20),
                antenna_type_reception VARCHAR(10),
                antenna_lat_reception FLOAT,
                antenna_lon_reception FLOAT,
                antenna_altitude_reception INT,
                operator_code_reception VARCHAR(10),
                caller_lat_home FLOAT,
                caller_lon_home FLOAT
            );

            CREATE INDEX idx_start_time ON {table_name}(start_time);
            CREATE INDEX idx_caller ON {table_name}(caller);
            CREATE INDEX idx_operator_emission ON {table_name}(operator_code_emission);
        END
    ''')
    conn.commit()

    for batch_start in tqdm(range(0, records_to_insert, BATCH_SIZE)):
        retry_count = 0
        max_retries = 3
        while retry_count < max_retries:
            try:
                values = []
                for _ in range(min(BATCH_SIZE, records_to_insert - batch_start)):
                    if random.random() < 0.4:
                        lat_home, lon_home = random.choice(shared_home_coords)
                    else:
                        lat_home = round(random.uniform(*BELGIUM_LAT), 6)
                        lon_home = round(random.uniform(*BELGIUM_LON), 6)

                    record = (
                        fake.msisdn(),
                        fake.msisdn(),
                        generate_random_start_time(),
                        random.randint(config['ranges']['duration']['min'], config['ranges']['duration']['max']),
                        random.choice(config['data_types']['call_types']),
                        random.choice(config['data_types']['call_status']),
                        generate_imei(),
                        generate_imei(),
                        get_operator_code(),
                        f"ANT{random.randint(config['ranges']['antenna_id']['min'], config['ranges']['antenna_id']['max'])}",
                        random.choice(config['data_types']['antenna_types']),
                        round(random.uniform(config['geography']['latitude']['min'], config['geography']['latitude']['max']), 6),
                        round(random.uniform(config['geography']['longitude']['min'], config['geography']['longitude']['max']), 6),
                        random.randint(config['ranges']['altitude']['min'], config['ranges']['altitude']['max']),
                        f"ANT{random.randint(config['ranges']['antenna_id']['min'], config['ranges']['antenna_id']['max'])}",
                        random.choice(config['data_types']['antenna_types']),
                        round(random.uniform(config['geography']['latitude']['min'], config['geography']['latitude']['max']), 6),
                        round(random.uniform(config['geography']['longitude']['min'], config['geography']['longitude']['max']), 6),
                        random.randint(config['ranges']['altitude']['min'], config['ranges']['altitude']['max']),
                        get_operator_code(),
                        lat_home,
                        lon_home
                    )
                    values.append(record)

                placeholders = ','.join(['?'] * len(values[0]))
                insert_query = f'''
                    INSERT INTO {table_name} (
                        caller, callee, start_time, duration, type, status,
                        imei_caller, imei_receiver, operator_code_emission, antenna_id_emission,
                        antenna_type_emission, antenna_lat_emission, antenna_lon_emission, antenna_altitude_emission,
                        antenna_id_reception, antenna_type_reception, antenna_lat_reception, antenna_lon_reception,
                        antenna_altitude_reception, operator_code_reception, caller_lat_home, caller_lon_home
                    ) VALUES ({placeholders})
                '''
                cursor.executemany(insert_query, values)
                conn.commit()
                logging.info(f"Successfully inserted batch of {len(values)} records. Total progress: {batch_start + len(values)}/{records_to_insert}")
                break  # Success, exit retry loop
            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    logging.warning(f"Error inserting batch (attempt {retry_count}/{max_retries}): {str(e)}. Retrying...")
                    time.sleep(5)  # Wait before retry
                    # Try to reconnect if connection was lost
                    try:
                        conn = get_connection()
                        cursor = conn.cursor()
                    except:
                        pass
                else:
                    logging.error(f"Failed to insert batch after {max_retries} attempts: {str(e)}")
                    raise  # Re-raise the exception after all retries failed

    logging.info("Finished inserting CDR records.")
    print("Finished inserting CDR records.")
    
    # Send completion email
    send_completion_email()

except Exception as e:
    logging.error(f"Fatal error: {str(e)}")
    raise
finally:
    if 'conn' in locals():
        conn.close()
        logging.info("Database connection closed")
