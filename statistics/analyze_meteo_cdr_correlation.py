import pandas as pd
import numpy as np
from sqlalchemy import create_engine, URL, text
from dotenv import load_dotenv
import os
import logging
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('meteo_cdr_correlation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
logger.info("Environment variables loaded")

# Database connection
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

# Create engine
engine = create_engine(connection_url)

logger.info("Fetching data from database...")

# Fetch CDR data
cdr_query = """
SELECT 
    CAST(start_time AS DATE) as date,
    COUNT(*) as call_count,
    AVG(duration) as avg_duration,
    COUNT(CASE WHEN status = 'success' THEN 1 END) as successful_calls,
    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_calls,
    CAST(COUNT(CASE WHEN status = 'success' THEN 1 END) AS FLOAT) / COUNT(*) * 100 as success_rate
FROM data_cdr
WHERE start_time >= '2025-04-01' AND start_time <= '2025-05-31'
GROUP BY CAST(start_time AS DATE)
ORDER BY date
"""

# Fetch meteorological data
meteo_query = """
SELECT 
    date,
    AVG(temperature) as avg_temperature,
    AVG(precipitation) as avg_precipitation,
    AVG(pluviometry_index) as avg_pluviometry,
    SUM(CASE WHEN rain = 1 THEN 1 ELSE 0 END) as rainy_days
FROM meteo_data
GROUP BY date
ORDER BY date
"""

# Execute queries
cdr_df = pd.read_sql(cdr_query, engine)
meteo_df = pd.read_sql(meteo_query, engine)

# Ensure both 'date' columns are datetime for merging
cdr_df['date'] = pd.to_datetime(cdr_df['date'])
meteo_df['date'] = pd.to_datetime(meteo_df['date'])

# Ensure success_rate is numeric if present
if 'success_rate' in cdr_df.columns:
    cdr_df['success_rate'] = pd.to_numeric(cdr_df['success_rate'], errors='coerce')

logger.info(f"Retrieved {len(cdr_df)} days of CDR data and {len(meteo_df)} days of meteorological data")

# Merge the datasets
merged_df = pd.merge(cdr_df, meteo_df, on='date', how='inner')

# Calculate correlations
corr_columns = [
    'call_count', 'avg_duration', 'successful_calls', 'failed_calls',
    'avg_temperature', 'avg_precipitation', 'avg_pluviometry', 'rainy_days'
]
if 'success_rate' in merged_df.columns:
    corr_columns.append('success_rate')
correlation_matrix = merged_df[corr_columns].corr()

logger.info("\nCorrelation Matrix:")
logger.info(correlation_matrix)

# Create visualizations

# 1. Correlation Heatmap
fig1 = px.imshow(
    correlation_matrix,
    title="Correlation Matrix: CDR vs Meteorological Data",
    color_continuous_scale='RdBu',
    aspect='auto'
)
fig1.write_html("correlation_heatmap.html")

# 2. Daily Call Volume vs Temperature
fig2 = px.scatter(
    merged_df,
    x='avg_temperature',
    y='call_count',
    title="Daily Call Volume vs Temperature",
    labels={
        'avg_temperature': 'Average Temperature (°C)',
        'call_count': 'Number of Calls'
    }
)
fig2.write_html("calls_vs_temperature.html")

# 3. Call Duration vs Precipitation
fig3 = px.scatter(
    merged_df,
    x='avg_precipitation',
    y='avg_duration',
    title="Average Call Duration vs Precipitation",
    labels={
        'avg_precipitation': 'Average Precipitation (mm)',
        'avg_duration': 'Average Call Duration (seconds)'
    }
)
fig3.write_html("duration_vs_precipitation.html")

# 4. Time Series of Calls and Weather
fig4 = go.Figure()
fig4.add_trace(go.Scatter(
    x=merged_df['date'],
    y=merged_df['call_count'],
    name='Call Volume',
    yaxis='y1'
))
fig4.add_trace(go.Scatter(
    x=merged_df['date'],
    y=merged_df['avg_temperature'],
    name='Temperature',
    yaxis='y2'
))
fig4.update_layout(
    title="Call Volume and Temperature Over Time",
    yaxis=dict(title="Number of Calls"),
    yaxis2=dict(title="Temperature (°C)", overlaying="y", side="right")
)
fig4.write_html("calls_temperature_timeseries.html")

# 5. Success Rate vs Temperature
fig5 = px.scatter(
    merged_df,
    x='avg_temperature',
    y='success_rate',
    title="Call Success Rate vs Temperature",
    labels={
        'avg_temperature': 'Average Temperature (°C)',
        'success_rate': 'Success Rate (%)'
    }
)
fig5.write_html("success_rate_vs_temperature.html")

# 6. Failed Calls vs Temperature
fig6 = px.scatter(
    merged_df,
    x='avg_temperature',
    y='failed_calls',
    title="Number of Failed Calls vs Temperature",
    labels={
        'avg_temperature': 'Average Temperature (°C)',
        'failed_calls': 'Number of Failed Calls'
    }
)
fig6.write_html("failed_calls_vs_temperature.html")

# 7. Temperature Distribution by Call Status
fig7 = go.Figure()
fig7.add_trace(go.Box(
    y=merged_df['avg_temperature'],
    x=['Temperature'] * len(merged_df),
    name='All Calls',
    boxpoints='all'
))
fig7.update_layout(
    title="Temperature Distribution for All Calls",
    yaxis_title="Temperature (°C)"
)
fig7.write_html("temperature_distribution.html")

# Print key findings
logger.info("\nKey Findings:")
logger.info(f"1. Correlation between call volume and temperature: {correlation_matrix.loc['call_count', 'avg_temperature']:.3f}")
logger.info(f"2. Correlation between call duration and precipitation: {correlation_matrix.loc['avg_duration', 'avg_precipitation']:.3f}")
logger.info(f"3. Correlation between failed calls and rainy days: {correlation_matrix.loc['failed_calls', 'rainy_days']:.3f}")

# Print additional findings
logger.info("\nAdditional Findings:")
if 'success_rate' in correlation_matrix.index and 'avg_temperature' in correlation_matrix.columns:
    logger.info(f"4. Correlation between success rate and temperature: {correlation_matrix.loc['success_rate', 'avg_temperature']:.3f}")
else:
    logger.info("4. Correlation between success rate and temperature: N/A (not available)")
logger.info(f"5. Average temperature on days with high failure rate: {merged_df[merged_df['failed_calls'] > merged_df['failed_calls'].median()]['avg_temperature'].mean():.1f}°C")
logger.info(f"6. Average temperature on days with low failure rate: {merged_df[merged_df['failed_calls'] <= merged_df['failed_calls'].median()]['avg_temperature'].mean():.1f}°C")

# Save the merged dataset
merged_df.to_csv('meteo_cdr_correlations.csv', index=False)
logger.info("\nAnalysis complete. Results saved to:")
logger.info("1. correlation_heatmap.html")
logger.info("2. calls_vs_temperature.html")
logger.info("3. duration_vs_precipitation.html")
logger.info("4. calls_temperature_timeseries.html")
logger.info("5. success_rate_vs_temperature.html")
logger.info("6. failed_calls_vs_temperature.html")
logger.info("7. temperature_distribution.html")
logger.info("8. meteo_cdr_correlations.csv") 