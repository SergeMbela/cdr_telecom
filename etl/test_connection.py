import pyodbc
from dotenv import load_dotenv
import os

def test_sql_server_connection():
    # Load environment variables
    load_dotenv()
    
    # Get database configuration from environment variables
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_NAME')
    username = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    driver = os.getenv('DB_DRIVER')
    
    print("Testing SQL Server connection with the following parameters:")
    print(f"Server: {server}")
    print(f"Database: {database}")
    print(f"Username: {username}")
    print(f"Password: {password}")
    print(f"Driver: {driver}")

    try:
        # Create connection string
        conn_str = (
            f'DRIVER={{{driver}}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={username};'
            f'PWD={password};'
            f'Encrypt=yes;'
            f'TrustServerCertificate=yes;'
        )
        
        # Attempt to connect
        print("\nAttempting to connect to SQL Server...")
        conn = pyodbc.connect(conn_str)
        
        # If connection successful, print server info
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        print("\nConnection successful!")
        print(f"SQL Server version: {version}")
        
        # Test database access
        print("\nTesting database access...")
        cursor.execute("SELECT DB_NAME()")
        current_db = cursor.fetchone()[0]
        print(f"Connected to database: {current_db}")
        
        # Close connection
        cursor.close()
        conn.close()
        print("\nConnection test completed successfully!")
        
    except Exception as e:
        print("\nError connecting to SQL Server:")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    test_sql_server_connection()
	