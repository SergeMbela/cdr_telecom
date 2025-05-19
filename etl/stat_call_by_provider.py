import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv
import os
import yaml
import logging
from datetime import datetime
from sqlalchemy import create_engine, text

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
    """Establish database connection using SQLAlchemy"""
    try:
        conn_str = f"mssql+pyodbc://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_SERVER')}/{os.getenv('DB_NAME')}?driver={os.getenv('DB_DRIVER')}&TrustServerCertificate=yes"
        engine = create_engine(conn_str)
        logging.info("Database connection established")
        return engine
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
        engine = get_connection()

        # Query to get statistics
        query = text("""
        SELECT 
            operator_code_emission as operator,
            CAST(COUNT(*) AS BIGINT) as total_calls,
            CAST(SUM(CASE WHEN type = 'voice' THEN 1 ELSE 0 END) AS BIGINT) as voice_calls,
            CAST(SUM(CASE WHEN type = 'sms' THEN 1 ELSE 0 END) AS BIGINT) as sms_calls,
            CAST(SUM(CASE WHEN type = 'data' THEN 1 ELSE 0 END) AS BIGINT) as data_calls,
            CAST(AVG(CAST(duration AS DECIMAL(18,2))) AS DECIMAL(18,2)) as avg_duration,
            CAST(
                CAST(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS DECIMAL(18,2)) * 100.0 / 
                NULLIF(CAST(COUNT(*) AS DECIMAL(18,2)), 0) 
                AS DECIMAL(18,2)
            ) as completion_rate
        FROM [cdr_telecom].[dbo].[cdr_data]
        GROUP BY operator_code_emission
        ORDER BY total_calls DESC
        """)

        # Execute query and convert to DataFrame
        df = pd.read_sql(query, engine)
        
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
        plt.style.use('default')  # Use default style instead of seaborn
        
        # 1. Total Calls by Operator
        plt.figure(figsize=(12, 6))
        sns.barplot(data=df, x='operator_name', y='total_calls')
        plt.title('Total Calls by Operator')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'statistics/total_calls_{timestamp}.png')
        plt.close()  # Close the figure to free memory
        
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
        plt.close()  # Close the figure to free memory
        
        # 3. Average Duration
        plt.figure(figsize=(12, 6))
        sns.barplot(data=df, x='operator_name', y='avg_duration')
        plt.title('Average Call Duration by Operator (seconds)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'statistics/avg_duration_{timestamp}.png')
        plt.close()  # Close the figure to free memory
        
        # 4. Completion Rate
        plt.figure(figsize=(12, 6))
        sns.barplot(data=df, x='operator_name', y='completion_rate')
        plt.title('Call Completion Rate by Operator (%)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'statistics/completion_rate_{timestamp}.png')
        plt.close()  # Close the figure to free memory

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
        if 'engine' in locals():
            engine.dispose()
            logging.info("Database connection closed")

if __name__ == "__main__":
    generate_statistics()
