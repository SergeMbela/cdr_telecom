import pandas as pd
from datetime import datetime, timedelta
import sqlalchemy as sa
from sqlalchemy import create_engine, URL
from dotenv import load_dotenv
import os
import logging
from tqdm import tqdm
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('meteo_data_generation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
logger.info("Environment variables loaded")

# Belgian cities with their coordinates (latitude, longitude)
BELGIAN_CITIES = {
    'Brussels': (50.8503, 4.3517),
    'Antwerp': (51.2195, 4.4025),
    'Ghent': (51.0543, 3.7174),
    'Charleroi': (50.4108, 4.4445),
    'Liège': (50.6326, 5.5797),
    'Bruges': (51.2093, 3.2247),
    'Namur': (50.4669, 4.8675),
    'Leuven': (50.8798, 4.7005),
    'Mons': (50.4542, 3.9561),
    'Aalst': (50.9378, 4.0400),
    'Kortrijk': (50.8270, 3.2514),
    'Ostend': (51.2154, 2.9286),
    'Tournai': (50.6067, 3.3883),
    'Genk': (50.9650, 5.5007),
    'Seraing': (50.5833, 5.5000),
    'Roeselare': (50.9445, 3.1229),
    'Verviers': (50.5911, 5.8656),
    'Mouscron': (50.7447, 3.2064),
    'Beveren': (51.2119, 4.2564),
    'Hasselt': (50.9311, 5.3378)
}

# Date range
start_date = datetime(2025, 4, 1)
end_date = datetime(2025, 5, 31)

# Create empty DataFrame to store all data
all_data = []
logger.info(f"Starting data generation for {len(BELGIAN_CITIES)} cities from {start_date.date()} to {end_date.date()}")

# Generate dates
dates = pd.date_range(start=start_date, end=end_date, freq='D')

# Generate weather data for each city
for city, (lat, lon) in tqdm(BELGIAN_CITIES.items(), desc="Generating city data"):
    logger.info(f"Processing data for {city}")
    try:
        # Generate random temperature (between 5 and 25 degrees Celsius)
        # Temperature varies by latitude (north is colder)
        base_temp = 15 - (lat - 50) * 2  # Base temperature varies by latitude
        temp_variation = np.random.normal(0, 3, len(dates))  # Daily variation
        temperatures = base_temp + temp_variation
        
        # Generate precipitation (0-30mm)
        # Coastal cities (like Ostend) have more rain
        is_coastal = city in ['Ostend', 'Bruges']
        base_precip = 5 if is_coastal else 3
        precip_variation = np.random.gamma(2, 2, len(dates))
        precipitations = base_precip * precip_variation
        
        # Create DataFrame for this city
        data = pd.DataFrame({
            'date': dates,
            'city': city,
            'temperature': temperatures,
            'precipitation': precipitations
        })
        
        # Add rain column (1 if precipitation > 0.1mm)
        data['rain'] = (data['precipitation'] > 0.1).astype(int)
        
        # Add time_begin and time_end
        data['time_begin'] = data['date']
        data['time_end'] = data['date']
        
        # Calculate pluviometry index
        conditions = [
            (data['precipitation'] == 0),
            (data['precipitation'] > 0) & (data['precipitation'] <= 2),
            (data['precipitation'] > 2) & (data['precipitation'] <= 5),
            (data['precipitation'] > 5) & (data['precipitation'] <= 10),
            (data['precipitation'] > 10) & (data['precipitation'] <= 20),
            (data['precipitation'] > 20)
        ]
        values = [0, 1, 2, 3, 4, 5]
        data['pluviometry_index'] = np.select(conditions, values, default=0)
        
        all_data.append(data)
        logger.info(f"Successfully generated {len(data)} records for {city}")
    except Exception as e:
        logger.error(f"Error generating data for {city}: {str(e)}")

# Combine all city data
df = pd.concat(all_data, ignore_index=True)
logger.info(f"Total records generated: {len(df)}")

# Database connection
DB_SERVER = os.getenv('DB_SERVER', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'CDR_DB')
DB_USER = os.getenv('DB_USER', 'sa')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'YourStrong@Passw0rd')

logger.info(f"Connecting to database server: {DB_SERVER}")

# Create SQLAlchemy URL for master database
master_url = URL.create(
    drivername="mssql+pyodbc",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_SERVER,
    database="master",
    query={"driver": "ODBC Driver 18 for SQL Server", "TrustServerCertificate": "yes"}
)

# Create engine for master database
master_engine = create_engine(master_url)

# Check if database exists and create if it doesn't
with master_engine.connect() as conn:
    # Check if database exists
    result = conn.execute(sa.text(f"SELECT database_id FROM sys.databases WHERE Name = '{DB_NAME}'"))
    if not result.fetchone():
        logger.info(f"Creating database {DB_NAME}...")
        conn.execute(sa.text(f"CREATE DATABASE {DB_NAME}"))
        logger.info(f"Database {DB_NAME} created successfully.")
    else:
        logger.info(f"Database {DB_NAME} already exists.")

# Create SQLAlchemy URL for the target database
connection_url = URL.create(
    drivername="mssql+pyodbc",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_SERVER,
    database=DB_NAME,
    query={"driver": "ODBC Driver 18 for SQL Server", "TrustServerCertificate": "yes"}
)

# Create engine
engine = create_engine(connection_url)

# Save to database
logger.info("Starting data insertion into database...")
try:
    df.to_sql('meteo_data', engine, if_exists='replace', index=False)
    logger.info(f"Successfully inserted {len(df)} records into meteo_data table")
except Exception as e:
    logger.error(f"Error inserting data into database: {str(e)}")

# Print summary statistics
logger.info("\nData Summary:")
logger.info(f"Total records: {len(df)}")
logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
logger.info(f"Number of cities: {df['city'].nunique()}")
logger.info("\nTemperature statistics:")
logger.info(df['temperature'].describe())
logger.info("\nPrecipitation statistics:")
logger.info(df['precipitation'].describe())
logger.info("\nPluviometry index distribution:")
logger.info(df['pluviometry_index'].value_counts().sort_index())
