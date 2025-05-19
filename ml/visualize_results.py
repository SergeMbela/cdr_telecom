import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import logging
from datetime import datetime

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ml/visualization.log'),
        logging.StreamHandler()
    ]
)

def create_visualization_directory():
    """Crée le répertoire pour les visualisations"""
    os.makedirs('ml/visualizations', exist_ok=True)

def plot_hourly_patterns(hourly_data):
    """Visualise les patterns horaires"""
    plt.figure(figsize=(15, 10))
    
    # Pattern de durée moyenne
    plt.subplot(3, 1, 1)
    sns.lineplot(data=hourly_data, x='hour', y='duration')
    plt.title('Durée moyenne des appels par heure')
    plt.xlabel('Heure')
    plt.ylabel('Durée moyenne (secondes)')
    
    # Pattern de force du signal
    plt.subplot(3, 1, 2)
    sns.lineplot(data=hourly_data, x='hour', y='signal_strength')
    plt.title('Force moyenne du signal par heure')
    plt.xlabel('Heure')
    plt.ylabel('Force du signal (dBm)')
    
    # Pattern de taux de réussite
    plt.subplot(3, 1, 3)
    sns.lineplot(data=hourly_data, x='hour', y='status')
    plt.title('Taux de réussite des appels par heure')
    plt.xlabel('Heure')
    plt.ylabel('Taux de réussite')
    
    plt.tight_layout()
    plt.savefig('ml/visualizations/hourly_patterns.png')
    plt.close()

def plot_daily_patterns(daily_data):
    """Visualise les patterns journaliers"""
    plt.figure(figsize=(15, 10))
    
    # Pattern de durée moyenne
    plt.subplot(3, 1, 1)
    sns.barplot(data=daily_data, x='day_of_week', y='duration')
    plt.title('Durée moyenne des appels par jour')
    plt.xlabel('Jour de la semaine')
    plt.ylabel('Durée moyenne (secondes)')
    plt.xticks(range(7), ['Lun', 'Mar', 'Mer', 'Jeu', 'Ven', 'Sam', 'Dim'])
    
    # Pattern de force du signal
    plt.subplot(3, 1, 2)
    sns.barplot(data=daily_data, x='day_of_week', y='signal_strength')
    plt.title('Force moyenne du signal par jour')
    plt.xlabel('Jour de la semaine')
    plt.ylabel('Force du signal (dBm)')
    plt.xticks(range(7), ['Lun', 'Mar', 'Mer', 'Jeu', 'Ven', 'Sam', 'Dim'])
    
    # Pattern de taux de réussite
    plt.subplot(3, 1, 3)
    sns.barplot(data=daily_data, x='day_of_week', y='status')
    plt.title('Taux de réussite des appels par jour')
    plt.xlabel('Jour de la semaine')
    plt.ylabel('Taux de réussite')
    plt.xticks(range(7), ['Lun', 'Mar', 'Mer', 'Jeu', 'Ven', 'Sam', 'Dim'])
    
    plt.tight_layout()
    plt.savefig('ml/visualizations/daily_patterns.png')
    plt.close()

def plot_regional_patterns(regional_data):
    """Visualise les patterns régionaux"""
    plt.figure(figsize=(15, 10))
    
    # Pattern de durée moyenne
    plt.subplot(3, 1, 1)
    sns.barplot(data=regional_data, x='region', y='duration')
    plt.title('Durée moyenne des appels par région')
    plt.xlabel('Région')
    plt.ylabel('Durée moyenne (secondes)')
    plt.xticks(rotation=45)
    
    # Pattern de force du signal
    plt.subplot(3, 1, 2)
    sns.barplot(data=regional_data, x='region', y='signal_strength')
    plt.title('Force moyenne du signal par région')
    plt.xlabel('Région')
    plt.ylabel('Force du signal (dBm)')
    plt.xticks(rotation=45)
    
    # Pattern de taux de réussite
    plt.subplot(3, 1, 3)
    sns.barplot(data=regional_data, x='region', y='status')
    plt.title('Taux de réussite des appels par région')
    plt.xlabel('Région')
    plt.ylabel('Taux de réussite')
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    plt.savefig('ml/visualizations/regional_patterns.png')
    plt.close()

def plot_model_performance(results):
    """Visualise les performances des modèles"""
    plt.figure(figsize=(10, 6))
    
    # Comparaison des précisions
    accuracies = {name: result['accuracy'] for name, result in results.items()}
    plt.bar(accuracies.keys(), accuracies.values())
    plt.title('Comparaison des précisions des modèles')
    plt.xlabel('Modèle')
    plt.ylabel('Précision')
    plt.ylim(0, 1)
    
    # Ajout des valeurs sur les barres
    for i, v in enumerate(accuracies.values()):
        plt.text(i, v + 0.01, f'{v:.2f}', ha='center')
    
    plt.tight_layout()
    plt.savefig('ml/visualizations/model_performance.png')
    plt.close()

def main():
    try:
        create_visualization_directory()
        
        # Chargement des données d'analyse
        hourly_data = pd.read_csv('ml/analysis/hourly_patterns.csv')
        daily_data = pd.read_csv('ml/analysis/daily_patterns.csv')
        regional_data = pd.read_csv('ml/analysis/regional_patterns.csv')
        
        # Création des visualisations
        logging.info("Création des visualisations...")
        plot_hourly_patterns(hourly_data)
        plot_daily_patterns(daily_data)
        plot_regional_patterns(regional_data)
        
        logging.info("Visualisations créées avec succès!")
        
    except Exception as e:
        logging.error(f"Erreur lors de la création des visualisations: {str(e)}")
        raise

if __name__ == "__main__":
    main() 