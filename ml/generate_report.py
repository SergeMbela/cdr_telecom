import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
import os
import logging
from datetime import datetime
import time
from functools import wraps
import requests
from requests.exceptions import RequestException

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ml/report_generation.log'),
        logging.StreamHandler()
    ]
)

def retry_on_connection_error(max_retries=3, delay=5):
    """Décorateur pour réessayer en cas d'erreur de connexion"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except (RequestException, IOError) as e:
                    retries += 1
                    if retries == max_retries:
                        logging.error(f"Échec après {max_retries} tentatives: {str(e)}")
                        raise
                    logging.warning(f"Tentative {retries}/{max_retries} échouée. Nouvelle tentative dans {delay} secondes...")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

def safe_file_operation(func):
    """Décorateur pour gérer les opérations sur les fichiers de manière sécurisée"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except IOError as e:
            logging.error(f"Erreur d'accès au fichier: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Erreur inattendue: {str(e)}")
            raise
    return wrapper

@safe_file_operation
def create_report_directory():
    """Crée le répertoire pour les rapports"""
    os.makedirs('ml/reports', exist_ok=True)

@retry_on_connection_error(max_retries=3, delay=5)
def load_data(file_path):
    """Charge les données avec gestion des erreurs"""
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        logging.error(f"Erreur lors du chargement des données depuis {file_path}: {str(e)}")
        raise

def create_metrics_table(data, title, columns):
    """Crée un tableau de métriques formaté"""
    table_data = [[title] + columns]
    for row in data:
        table_data.append(row)
    
    table = Table(table_data, colWidths=[2*inch] + [1.5*inch] * len(columns))
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 14),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 12),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))
    return table

def generate_report():
    """Génère le rapport PDF avec les visualisations et analyses"""
    try:
        # Création du répertoire pour les rapports
        create_report_directory()
        
        # Nom du fichier PDF avec timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        pdf_path = f'ml/reports/cdr_analysis_report_{timestamp}.pdf'
        
        # Création du document
        doc = SimpleDocTemplate(
            pdf_path,
            pagesize=letter,
            rightMargin=72,
            leftMargin=72,
            topMargin=72,
            bottomMargin=72
        )
        
        # Styles
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            spaceAfter=30
        )
        heading_style = ParagraphStyle(
            'CustomHeading',
            parent=styles['Heading2'],
            fontSize=18,
            spaceAfter=20
        )
        subheading_style = ParagraphStyle(
            'CustomSubHeading',
            parent=styles['Heading3'],
            fontSize=14,
            spaceAfter=15
        )
        body_style = ParagraphStyle(
            'CustomBody',
            parent=styles['Normal'],
            fontSize=12,
            spaceAfter=12
        )
        
        # Contenu du rapport
        content = []
        
        # Titre
        content.append(Paragraph("Rapport d'Analyse des CDR", title_style))
        content.append(Paragraph(f"Généré le {datetime.now().strftime('%d/%m/%Y à %H:%M')}", body_style))
        content.append(Spacer(1, 20))
        
        # Introduction
        content.append(Paragraph("Introduction", heading_style))
        content.append(Paragraph("""
            Ce rapport présente une analyse approfondie des Call Detail Records (CDR) collectés. 
            Il inclut des visualisations des patterns temporels, géographiques et des performances 
            des modèles de prédiction, ainsi qu'une analyse détaillée de la qualité du service.
        """, body_style))
        content.append(Spacer(1, 20))
        
        # Résumé Exécutif
        content.append(Paragraph("Résumé Exécutif", heading_style))
        content.append(Paragraph("""
            Ce rapport présente les principales découvertes de l'analyse des CDR, incluant :
            • Les tendances d'utilisation du réseau
            • La qualité du service par région
            • Les performances des opérateurs
            • Les patterns saisonniers
            • Les recommandations pour l'optimisation du réseau
        """, body_style))
        content.append(PageBreak())
        
        # Patterns horaires
        content.append(Paragraph("Patterns Horaires", heading_style))
        content.append(Paragraph("""
            Les graphiques ci-dessous montrent les variations de la durée moyenne des appels, 
            de la force du signal et du taux de réussite au cours de la journée.
        """, body_style))
        
        try:
            content.append(Image('ml/visualizations/hourly_patterns.png', width=6*inch, height=4*inch))
        except Exception as e:
            logging.error(f"Erreur lors du chargement de l'image hourly_patterns.png: {str(e)}")
            content.append(Paragraph("Image non disponible", body_style))
        
        # Analyse des heures de pointe
        content.append(Paragraph("Analyse des Heures de Pointe", subheading_style))
        try:
            hourly_data = load_data('ml/analysis/hourly_patterns.csv')
            peak_hours = hourly_data.nlargest(3, 'duration')
            peak_hours_data = [
                ['Heure', 'Durée moyenne', 'Taux de réussite'],
                *[[f"{row['hour']}h", f"{row['duration']:.2f}s", f"{row['status']:.2%}"] 
                  for _, row in peak_hours.iterrows()]
            ]
            content.append(create_metrics_table(peak_hours_data, "Heures de Pointe", ['Durée', 'Taux']))
        except Exception as e:
            logging.error(f"Erreur lors de l'analyse des heures de pointe: {str(e)}")
            content.append(Paragraph("Données non disponibles", body_style))
        
        content.append(Spacer(1, 20))
        
        # Patterns journaliers
        content.append(Paragraph("Patterns Journaliers", heading_style))
        content.append(Paragraph("""
            Cette section présente les variations des métriques clés selon les jours de la semaine, 
            permettant d'identifier les tendances hebdomadaires.
        """, body_style))
        
        try:
            content.append(Image('ml/visualizations/daily_patterns.png', width=6*inch, height=4*inch))
        except Exception as e:
            logging.error(f"Erreur lors du chargement de l'image daily_patterns.png: {str(e)}")
            content.append(Paragraph("Image non disponible", body_style))
        
        # Analyse des jours de la semaine
        content.append(Paragraph("Analyse par Jour", subheading_style))
        try:
            daily_data = load_data('ml/analysis/daily_patterns.csv')
            daily_metrics = [
                ['Jour', 'Durée moyenne', 'Signal moyen', 'Taux de réussite'],
                *[[['Lun', 'Mar', 'Mer', 'Jeu', 'Ven', 'Sam', 'Dim'][int(row['day_of_week'])],
                   f"{row['duration']:.2f}s",
                   f"{row['signal_strength']:.2f} dBm",
                   f"{row['status']:.2%}"]
                  for _, row in daily_data.iterrows()]
            ]
            content.append(create_metrics_table(daily_metrics, "Métriques Journalières", ['Durée', 'Signal', 'Taux']))
        except Exception as e:
            logging.error(f"Erreur lors de l'analyse des jours: {str(e)}")
            content.append(Paragraph("Données non disponibles", body_style))
        
        content.append(PageBreak())
        
        # Patterns régionaux
        content.append(Paragraph("Patterns Régionaux", heading_style))
        content.append(Paragraph("""
            L'analyse géographique montre les variations des métriques selon les régions, 
            mettant en évidence les différences de qualité de service et d'utilisation.
        """, body_style))
        
        try:
            content.append(Image('ml/visualizations/regional_patterns.png', width=6*inch, height=4*inch))
        except Exception as e:
            logging.error(f"Erreur lors du chargement de l'image regional_patterns.png: {str(e)}")
            content.append(Paragraph("Image non disponible", body_style))
        
        # Analyse régionale détaillée
        content.append(Paragraph("Analyse Régionale Détaillée", subheading_style))
        try:
            regional_data = load_data('ml/analysis/regional_patterns.csv')
            regional_metrics = [
                ['Région', 'Durée moyenne', 'Signal moyen', 'Taux de réussite'],
                *[[row['region'],
                   f"{row['duration']:.2f}s",
                   f"{row['signal_strength']:.2f} dBm",
                   f"{row['status']:.2%}"]
                  for _, row in regional_data.iterrows()]
            ]
            content.append(create_metrics_table(regional_metrics, "Métriques Régionales", ['Durée', 'Signal', 'Taux']))
        except Exception as e:
            logging.error(f"Erreur lors de l'analyse régionale: {str(e)}")
            content.append(Paragraph("Données non disponibles", body_style))
        
        content.append(Spacer(1, 20))
        
        # Performances des modèles
        content.append(Paragraph("Performances des Modèles", heading_style))
        content.append(Paragraph("""
            Cette section compare les performances des différents modèles de prédiction utilisés 
            pour analyser les CDR.
        """, body_style))
        
        try:
            content.append(Image('ml/visualizations/model_performance.png', width=6*inch, height=3*inch))
        except Exception as e:
            logging.error(f"Erreur lors du chargement de l'image model_performance.png: {str(e)}")
            content.append(Paragraph("Image non disponible", body_style))
        
        content.append(Spacer(1, 20))
        
        # Métriques de qualité
        content.append(Paragraph("Métriques de Qualité", heading_style))
        content.append(Paragraph("""
            Les métriques suivantes fournissent une vue d'ensemble de la qualité du service 
            et des performances du réseau.
        """, body_style))
        
        # Calcul des métriques de qualité
        try:
            quality_metrics = [
                ['Métrique', 'Valeur', 'Tendance'],
                ['Durée moyenne des appels', f"{hourly_data['duration'].mean():.2f} secondes", '↑'],
                ['Force moyenne du signal', f"{hourly_data['signal_strength'].mean():.2f} dBm", '→'],
                ['Taux moyen de réussite', f"{hourly_data['status'].mean():.2%}", '↑'],
                ['Meilleur jour pour les appels', daily_data.loc[daily_data['status'].idxmax(), 'day_of_week'], '-'],
                ['Région avec le meilleur signal', regional_data.loc[regional_data['signal_strength'].idxmax(), 'region'], '-'],
                ['Heure de pointe', f"{hourly_data.loc[hourly_data['duration'].idxmax(), 'hour']}h", '-'],
                ['Taux de réussite minimum', f"{regional_data['status'].min():.2%}", '→'],
                ['Signal minimum', f"{regional_data['signal_strength'].min():.2f} dBm", '→']
            ]
            content.append(create_metrics_table(quality_metrics, "Métriques de Qualité", ['Valeur', 'Tendance']))
        except Exception as e:
            logging.error(f"Erreur lors du calcul des métriques de qualité: {str(e)}")
            content.append(Paragraph("Métriques non disponibles", body_style))
        
        content.append(Spacer(1, 20))
        
        # Recommandations
        content.append(Paragraph("Recommandations", heading_style))
        content.append(Paragraph("""
            Basé sur l'analyse des données, voici les principales recommandations pour 
            l'optimisation du réseau :
            
            1. Renforcement du réseau dans les régions avec un signal faible
            2. Optimisation des ressources pendant les heures de pointe
            3. Amélioration de la couverture pendant les weekends
            4. Mise en place de maintenance préventive pendant les périodes creuses
            5. Développement de stratégies spécifiques par région
        """, body_style))
        content.append(Spacer(1, 20))
        
        # Conclusion
        content.append(Paragraph("Conclusion", heading_style))
        content.append(Paragraph("""
            Cette analyse des CDR révèle des patterns importants dans l'utilisation du réseau 
            et la qualité du service. Les visualisations et métriques présentées permettent 
            d'identifier les périodes et régions nécessitant une attention particulière pour 
            l'optimisation du réseau. Les recommandations fournies peuvent servir de base pour 
            l'amélioration continue de la qualité du service.
        """, body_style))
        
        # Génération du PDF
        try:
            doc.build(content)
            logging.info(f"Rapport généré avec succès: {pdf_path}")
        except Exception as e:
            logging.error(f"Erreur lors de la génération du PDF: {str(e)}")
            raise
        
    except Exception as e:
        logging.error(f"Erreur lors de la génération du rapport: {str(e)}")
        raise

if __name__ == "__main__":
    generate_report() 