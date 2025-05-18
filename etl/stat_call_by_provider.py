import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv
import os
import yaml
import logging
from datetime import datetime

# Load configuration
with open('config.yml', 'r') as file:
    config = yaml.safe_load(file)

# Configure logging
logging.basicConfig(
    level=getattr(logging, config['logging']['level']),
    format=config['logging']['format'],
    handlers=[
        logging.FileHandler('statistics.log'),
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()

def get_connection():
    """Establish database connection"""
    try:
        conn_str = f'DRIVER={os.getenv("DB_DRIVER")};SERVER={os.getenv("DB_SERVER")};DATABASE={os.getenv("DB_NAME")};UID={os.getenv("DB_USER")};PWD={os.getenv("DB_PASSWORD")};Encrypt=yes;TrustServerCertificate=yes'
        conn = pyodbc.connect(conn_str)
        logging.info("Database connection established")
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {str(e)}")
        raise

def get_operator_name(code):
    """Get operator name from code"""
    operators = {op['code']: op['name'] for op in config['data_types']['operators']}
    return operators.get(int(code), f"Unknown ({code})")

def generate_statistics():
    """Generate call statistics by operator"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Query to get statistics
        query = f"""
        SELECT 
            operator_code_emission as operator,
            COUNT(*) as total_calls,
            SUM(CASE WHEN type = 'voice' THEN 1 ELSE 0 END) as voice_calls,
            SUM(CASE WHEN type = 'sms' THEN 1 ELSE 0 END) as sms_calls,
            SUM(CASE WHEN type = 'data' THEN 1 ELSE 0 END) as data_calls,
            AVG(duration) as avg_duration,
            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as completion_rate
        FROM {config['database']['table_name']}
        GROUP BY operator_code_emission
        ORDER BY total_calls DESC
        """

        # Execute query and convert to DataFrame
        df = pd.read_sql(query, conn)
        
        # Add operator names
        df['operator_name'] = df['operator'].apply(get_operator_name)
        
        # Create output directory if it doesn't exist
        os.makedirs('statistics', exist_ok=True)
        
        # Generate timestamp for filenames
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save to CSV
        csv_file = f'statistics/call_stats_{timestamp}.csv'
        df.to_csv(csv_file, index=False)
        logging.info(f"Statistics saved to {csv_file}")

        # Create visualizations
        plt.style.use('seaborn')
        
        # 1. Total Calls by Operator
        plt.figure(figsize=(12, 6))
        sns.barplot(data=df, x='operator_name', y='total_calls')
        plt.title('Total Calls by Operator')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'statistics/total_calls_{timestamp}.png')
        
        # 2. Call Types Distribution
        plt.figure(figsize=(12, 6))
        df_melted = pd.melt(df, 
                           id_vars=['operator_name'],
                           value_vars=['voice_calls', 'sms_calls', 'data_calls'],
                           var_name='Call Type',
                           value_name='Count')
        sns.barplot(data=df_melted, x='operator_name', y='Count', hue='Call Type')
        plt.title('Call Types Distribution by Operator')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'statistics/call_types_{timestamp}.png')
        
        # 3. Average Duration
        plt.figure(figsize=(12, 6))
        sns.barplot(data=df, x='operator_name', y='avg_duration')
        plt.title('Average Call Duration by Operator (seconds)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'statistics/avg_duration_{timestamp}.png')
        
        # 4. Completion Rate
        plt.figure(figsize=(12, 6))
        sns.barplot(data=df, x='operator_name', y='completion_rate')
        plt.title('Call Completion Rate by Operator (%)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'statistics/completion_rate_{timestamp}.png')

        logging.info("Statistics and visualizations generated successfully")
        
        # Print summary
        print("\nCall Statistics Summary:")
        print("=" * 80)
        print(df.to_string(index=False))
        print("\nVisualizations have been saved in the 'statistics' directory")

    except Exception as e:
        logging.error(f"Error generating statistics: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    generate_statistics()
