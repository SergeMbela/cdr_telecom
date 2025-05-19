# 📦 Import des librairies
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, URL
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# 🔌 Connexion à SQL Server using environment variables
DB_SERVER = os.getenv('DB_SERVER', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'CDR_DB')
DB_USER = os.getenv('DB_USER', 'sa')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'YourStrong@Passw0rd')

# Create SQLAlchemy URL
connection_url = URL.create(
    drivername="mssql+pyodbc",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_SERVER,
    database=DB_NAME,
    query={"driver": "ODBC Driver 18 for SQL Server", "TrustServerCertificate": "yes"}
)

engine = create_engine(connection_url)

# 📥 Requête SQL : récupération d'un échantillon
query = """
SELECT TOP 10000 
    id, caller, callee, start_time, duration, type, status,
    antenna_lat_emission, antenna_lon_emission,
    caller_lat_home, caller_lon_home,
    operator_code_emission, operator_code_reception
FROM dbo.data_cdr
WHERE type = 'voice'
"""
df = pd.read_sql(query, engine)

# 🧪 Calcul d'une variable dérivée : distance domicile ↔ antenne
from math import radians, sin, cos, sqrt, atan2

def haversine(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2):
        return None
    R = 6371  # rayon Terre en km
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

df['distance_home_antenna_km'] = df.apply(
    lambda row: haversine(row['caller_lat_home'], row['caller_lon_home'],
                          row['antenna_lat_emission'], row['antenna_lon_emission']),
    axis=1
)

# 🔍 Analyse bi-variée 1 : Durée vs Distance
fig1 = px.scatter(df, x='distance_home_antenna_km', y='duration',
                  title="Durée des appels en fonction de la distance à l'antenne",
                  labels={'distance_home_antenna_km': "Distance domicile ↔ antenne (km)", 'duration': "Durée (s)"})
fig1.show()

# 🔍 Analyse bi-variée 2 : Statut vs Opérateur
df_status = df.groupby(['operator_code_emission', 'status']).size().reset_index(name='count')
fig2 = px.bar(df_status, x='operator_code_emission', y='count', color='status',
              barmode='group', title="Nombre d'appels par statut et opérateur")
fig2.show()

# 🔍 Analyse bi-variée 3 : Heure d'appel vs Type (si plusieurs types)
df['hour'] = pd.to_datetime(df['start_time']).dt.hour
fig3 = px.histogram(df, x='hour', color='type', barmode='group',
                    title="Volume d'appels par heure et type")
fig3.show() 