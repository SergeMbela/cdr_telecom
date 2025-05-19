import os
import pandas as pd
import matplotlib.pyplot as plt
from fpdf import FPDF
import seaborn as sns
from datetime import datetime
import numpy as np
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from PIL import Image
import io

class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 15)
        self.cell(0, 10, 'Analyse des Corrélations CDR-Météo', 0, 1, 'C')
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

def create_correlation_heatmap(df, title):
    plt.figure(figsize=(10, 8))
    sns.heatmap(df, annot=True, cmap='coolwarm', center=0, fmt='.2f')
    plt.title(title)
    
    # Convert plot to image
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    plt.close()
    return Image.open(buf)

def create_time_series_plot(df, x_col, y_col, title):
    plt.figure(figsize=(12, 6))
    plt.plot(df[x_col], df[y_col])
    plt.title(title)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Convert plot to image
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    plt.close()
    return Image.open(buf)

def create_scatter_plot(df, x_col, y_col, title):
    plt.figure(figsize=(10, 6))
    plt.scatter(df[x_col], df[y_col], alpha=0.5)
    plt.title(title)
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    
    # Add trend line
    z = np.polyfit(df[x_col], df[y_col], 1)
    p = np.poly1d(z)
    plt.plot(df[x_col], p(df[x_col]), "r--", alpha=0.8)
    
    # Convert plot to image
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    plt.close()
    return Image.open(buf)

def generate_report():
    # Initialize PDF
    pdf = PDF()
    pdf.add_page()
    
    # Add title
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(0, 10, 'Rapport d\'Analyse des Corrélations CDR-Météo', 0, 1, 'C')
    pdf.ln(10)
    
    # Add date
    pdf.set_font('Arial', 'I', 12)
    pdf.cell(0, 10, f'Date de génération: {datetime.now().strftime("%Y-%m-%d %H:%M")}', 0, 1, 'C')
    pdf.ln(10)
    
    # Load data
    df = pd.read_csv('meteo_cdr_correlations.csv')
    
    # 1. Corrélations globales
    pdf.set_font('Arial', 'B', 14)
    pdf.cell(0, 10, '1. Corrélations Globales', 0, 1)
    pdf.ln(5)
    
    correlation_matrix = df[['call_count', 'avg_duration', 'successful_calls', 'failed_calls', 
                           'avg_temperature', 'avg_precipitation', 'avg_pluviometry', 'rainy_days']].corr()
    heatmap = create_correlation_heatmap(correlation_matrix, 'Matrice de Corrélation')
    pdf.image(heatmap, x=10, y=None, w=190)
    pdf.ln(10)
    
    # 2. Analyse temporelle
    pdf.add_page()
    pdf.set_font('Arial', 'B', 14)
    pdf.cell(0, 10, '2. Analyse Temporelle', 0, 1)
    pdf.ln(5)
    
    # Volume d'appels vs Température
    scatter = create_scatter_plot(df, 'avg_temperature', 'call_count', 
                                'Volume d\'Appels vs Température')
    pdf.image(scatter, x=10, y=None, w=190)
    pdf.ln(5)
    
    # Durée des appels vs Précipitations
    scatter = create_scatter_plot(df, 'avg_precipitation', 'avg_duration',
                                'Durée des Appels vs Précipitations')
    pdf.image(scatter, x=10, y=None, w=190)
    pdf.ln(5)
    
    # Série temporelle des appels
    time_series = create_time_series_plot(df, 'date', 'call_count',
                                        'Évolution du Volume d\'Appels')
    pdf.image(time_series, x=10, y=None, w=190)
    pdf.ln(5)
    
    # 3. Analyse des échecs
    pdf.add_page()
    pdf.set_font('Arial', 'B', 14)
    pdf.cell(0, 10, '3. Analyse des Échecs', 0, 1)
    pdf.ln(5)
    
    # Taux de succès vs Température
    scatter = create_scatter_plot(df, 'avg_temperature', 'success_rate',
                                'Taux de Succès vs Température')
    pdf.image(scatter, x=10, y=None, w=190)
    pdf.ln(5)
    
    # Appels échoués vs Température
    scatter = create_scatter_plot(df, 'avg_temperature', 'failed_calls',
                                'Appels Échoués vs Température')
    pdf.image(scatter, x=10, y=None, w=190)
    pdf.ln(5)
    
    # 4. Statistiques descriptives
    pdf.add_page()
    pdf.set_font('Arial', 'B', 14)
    pdf.cell(0, 10, '4. Statistiques Descriptives', 0, 1)
    pdf.ln(5)
    
    stats = df[['call_count', 'avg_duration', 'successful_calls', 'failed_calls',
                'avg_temperature', 'avg_precipitation', 'avg_pluviometry', 'rainy_days']].describe()
    stats_str = stats.to_string()
    
    pdf.set_font('Courier', '', 10)
    for line in stats_str.split('\n'):
        pdf.cell(0, 10, line, 0, 1)
    
    # Save PDF
    pdf.output('statistics/pdf/cdr_meteo_analysis_report.pdf')

if __name__ == "__main__":
    generate_report() 