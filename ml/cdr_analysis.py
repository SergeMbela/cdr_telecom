import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import classification_report, confusion_matrix
import pyodbc
import logging
from datetime import datetime, timedelta
import joblib
import os
from dotenv import load_dotenv

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ml/cdr_analysis.log'),
        logging.StreamHandler()
    ]
)

# Chargement des variables d'environnement
load_dotenv()

def get_db_connection():
    """Établit une connexion à la base de données"""
    try:
        conn_str = f'DRIVER={os.getenv("DB_DRIVER")};SERVER={os.getenv("DB_SERVER")};DATABASE={os.getenv("DB_NAME")};UID={os.getenv("DB_USER")};PWD={os.getenv("DB_PASSWORD")}'
        return pyodbc.connect(conn_str)
    except Exception as e:
        logging.error(f"Erreur de connexion à la base de données: {str(e)}")
        raise

def load_data(days=30):
    """Charge les données CDR des derniers jours"""
    try:
        conn = get_db_connection()
        query = f"""
        SELECT 
            caller,
            start_time,
            duration,
            type,
            status,
            signal_strength,
            region,
            is_weekend,
            is_holiday,
            is_rush_hour,
            season
        FROM cdr_records
        WHERE start_time >= DATEADD(day, -{days}, GETDATE())
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        logging.error(f"Erreur lors du chargement des données: {str(e)}")
        raise

def preprocess_data(df):
    """Prétraite les données pour l'analyse"""
    try:
        # Conversion des types de données
        df['start_time'] = pd.to_datetime(df['start_time'])
        df['hour'] = df['start_time'].dt.hour
        df['day_of_week'] = df['start_time'].dt.dayofweek
        
        # Création de features
        df['call_duration_category'] = pd.cut(
            df['duration'],
            bins=[0, 60, 300, 600, float('inf')],
            labels=['very_short', 'short', 'medium', 'long']
        )
        
        # Encodage des variables catégorielles
        categorical_cols = ['type', 'status', 'region', 'season', 'call_duration_category']
        df = pd.get_dummies(df, columns=categorical_cols)
        
        # Features pour la prédiction
        features = [
            'hour', 'day_of_week', 'signal_strength',
            'is_weekend', 'is_holiday', 'is_rush_hour'
        ] + [col for col in df.columns if col.startswith(('type_', 'status_', 'region_', 'season_', 'call_duration_category_'))]
        
        return df, features
    except Exception as e:
        logging.error(f"Erreur lors du prétraitement des données: {str(e)}")
        raise

def train_models(df, features, target):
    """Entraîne les modèles de machine learning"""
    try:
        X = df[features]
        y = df[target]
        
        # Division des données
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Normalisation
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # Modèles
        models = {
            'random_forest': RandomForestClassifier(n_estimators=100, random_state=42),
            'gradient_boosting': GradientBoostingClassifier(n_estimators=100, random_state=42)
        }
        
        results = {}
        for name, model in models.items():
            # Entraînement
            model.fit(X_train_scaled, y_train)
            
            # Évaluation
            y_pred = model.predict(X_test_scaled)
            results[name] = {
                'model': model,
                'scaler': scaler,
                'features': features,
                'accuracy': model.score(X_test_scaled, y_test),
                'classification_report': classification_report(y_test, y_pred),
                'confusion_matrix': confusion_matrix(y_test, y_pred)
            }
            
            # Sauvegarde du modèle
            joblib.dump({
                'model': model,
                'scaler': scaler,
                'features': features
            }, f'ml/models/{name}_model.joblib')
            
            logging.info(f"Modèle {name} entraîné avec une précision de {results[name]['accuracy']:.2f}")
        
        return results
    except Exception as e:
        logging.error(f"Erreur lors de l'entraînement des modèles: {str(e)}")
        raise

def predict_call_status(new_data, model_name='random_forest'):
    """Prédit le statut d'un appel pour de nouvelles données"""
    try:
        # Chargement du modèle
        model_data = joblib.load(f'ml/models/{model_name}_model.joblib')
        model = model_data['model']
        scaler = model_data['scaler']
        features = model_data['features']
        
        # Prétraitement des nouvelles données
        new_data_scaled = scaler.transform(new_data[features])
        
        # Prédiction
        predictions = model.predict(new_data_scaled)
        probabilities = model.predict_proba(new_data_scaled)
        
        return predictions, probabilities
    except Exception as e:
        logging.error(f"Erreur lors de la prédiction: {str(e)}")
        raise

def analyze_temporal_patterns(df):
    """Analyse les patterns temporels des appels"""
    try:
        # Patterns par heure
        hourly_patterns = df.groupby('hour').agg({
            'duration': 'mean',
            'signal_strength': 'mean',
            'status': lambda x: (x == 'completed').mean()
        }).reset_index()
        
        # Patterns par jour de la semaine
        daily_patterns = df.groupby('day_of_week').agg({
            'duration': 'mean',
            'signal_strength': 'mean',
            'status': lambda x: (x == 'completed').mean()
        }).reset_index()
        
        # Patterns par région
        regional_patterns = df.groupby('region').agg({
            'duration': 'mean',
            'signal_strength': 'mean',
            'status': lambda x: (x == 'completed').mean()
        }).reset_index()
        
        return {
            'hourly': hourly_patterns,
            'daily': daily_patterns,
            'regional': regional_patterns
        }
    except Exception as e:
        logging.error(f"Erreur lors de l'analyse des patterns: {str(e)}")
        raise

def main():
    try:
        # Création du dossier models s'il n'existe pas
        os.makedirs('ml/models', exist_ok=True)
        
        # Chargement des données
        logging.info("Chargement des données...")
        df = load_data(days=30)
        
        # Prétraitement
        logging.info("Prétraitement des données...")
        df_processed, features = preprocess_data(df)
        
        # Entraînement des modèles
        logging.info("Entraînement des modèles...")
        results = train_models(df_processed, features, 'status')
        
        # Analyse des patterns
        logging.info("Analyse des patterns temporels...")
        patterns = analyze_temporal_patterns(df)
        
        # Sauvegarde des résultats d'analyse
        for pattern_type, pattern_data in patterns.items():
            pattern_data.to_csv(f'ml/analysis/{pattern_type}_patterns.csv', index=False)
        
        logging.info("Analyse terminée avec succès!")
        
    except Exception as e:
        logging.error(f"Erreur dans l'analyse: {str(e)}")
        raise

if __name__ == "__main__":
    main() 