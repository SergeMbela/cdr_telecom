# 📦 Import des librairies
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, URL, Column, Integer, String, Float, DateTime, select
from sqlalchemy.orm import declarative_base, Session
from dotenv import load_dotenv
import os
from datetime import datetime

# Load environment variables
load_dotenv()

# Create SQLAlchemy Base
Base = declarative_base()

# Define CDR Model
class CDR(Base):
    __tablename__ = 'data_cdr'
    
    id = Column(Integer, primary_key=True)
    caller = Column(String)
    callee = Column(String)
    start_time = Column(DateTime)
    duration = Column(Float)
    type = Column(String)
    status = Column(String)
    antenna_lat_emission = Column(Float)
    antenna_lon_emission = Column(Float)
    caller_lat_home = Column(Float)
    caller_lon_home = Column(Float)
    operator_code_emission = Column(String)
    operator_code_reception = Column(String)

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

# Create session
with Session(engine) as session:
    # Query using SQLAlchemy ORM
    query = select(CDR).where(CDR.type == 'voice').limit(10000)
    results = session.execute(query).scalars().all()
    
    # Convert to DataFrame
    df = pd.DataFrame([{
        'id': r.id,
        'caller': r.caller,
        'callee': r.callee,
        'start_time': r.start_time,
        'duration': r.duration,
        'type': r.type,
        'status': r.status,
        'antenna_lat_emission': r.antenna_lat_emission,
        'antenna_lon_emission': r.antenna_lon_emission,
        'caller_lat_home': r.caller_lat_home,
        'caller_lon_home': r.caller_lon_home,
        'operator_code_emission': r.operator_code_emission,
        'operator_code_reception': r.operator_code_reception
    } for r in results])

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

# 🔍 Analyse de la distribution des durées d'appels
fig4 = px.histogram(df, x='duration',
                    title="Distribution des durées d'appels",
                    labels={'duration': "Durée (s)", 'count': "Nombre d'appels"},
                    nbins=50)
fig4.show()

# 🔍 Analyse de la durée des appels par heure de la journée
fig5 = px.box(df, x='hour', y='duration',
              title="Distribution des durées d'appels par heure de la journée",
              labels={'hour': "Heure de la journée", 'duration': "Durée (s)"})
fig5.show()
