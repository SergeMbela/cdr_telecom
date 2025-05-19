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

def generate_status_statistics():
    """Generate call status statistics by operator"""
    try:
        engine = get_connection()

        # Query to get status statistics
        query = text("""
        SELECT 
            operator_code_emission as operator,
            status,
            CAST(COUNT(*) AS BIGINT) as count,
            CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY operator_code_emission) AS DECIMAL(10,2)) as percentage
        FROM [cdr_telecom].[dbo].[cdr_data]
        GROUP BY operator_code_emission, status
        ORDER BY operator_code_emission, count DESC
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
        csv_file = f'statistics/status_stats_{timestamp}.csv'
        df.to_csv(csv_file, index=False)
        logging.info(f"Status statistics saved to {csv_file}")

        # Create visualizations
        plt.style.use('default')
        
        # 1. Status Distribution by Operator (Count)
        plt.figure(figsize=(15, 8))
        sns.barplot(data=df, x='operator_name', y='count', hue='status')
        plt.title('Call Status Distribution by Operator')
        plt.xticks(rotation=45)
        plt.xlabel('Operator')
        plt.ylabel('Number of Calls')
        plt.legend(title='Status', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        plt.savefig(f'statistics/status_distribution_{timestamp}.png')
        plt.close()

        # 2. Status Distribution by Operator (Percentage)
        plt.figure(figsize=(15, 8))
        sns.barplot(data=df, x='operator_name', y='percentage', hue='status')
        plt.title('Call Status Distribution by Operator (Percentage)')
        plt.xticks(rotation=45)
        plt.xlabel('Operator')
        plt.ylabel('Percentage (%)')
        plt.legend(title='Status', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        plt.savefig(f'statistics/status_percentage_{timestamp}.png')
        plt.close()

        # 3. Create a pivot table for better visualization
        pivot_df = df.pivot_table(
            index='operator_name',
            columns='status',
            values='percentage',
            aggfunc='sum'
        ).fillna(0)

        # Save pivot table to CSV
        pivot_csv = f'statistics/status_pivot_{timestamp}.csv'
        pivot_df.to_csv(pivot_csv)
        logging.info(f"Status pivot table saved to {pivot_csv}")

        logging.info("Status statistics and visualizations generated successfully")
        
        # Print summary
        print("\nCall Status Statistics Summary:")
        print("=" * 80)
        print("\nBy Count:")
        print(df.pivot_table(
            index='operator_name',
            columns='status',
            values='count',
            aggfunc='sum'
        ).fillna(0).to_string())
        
        print("\nBy Percentage:")
        print(pivot_df.to_string())
        print("\nVisualizations have been saved in the 'statistics' directory")

    except Exception as e:
        logging.error(f"Error generating status statistics: {str(e)}")
        raise
    finally:
        if 'engine' in locals():
            engine.dispose()
            logging.info("Database connection closed")

if __name__ == "__main__":
    generate_status_statistics() 