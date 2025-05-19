import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from dotenv import load_dotenv
from datetime import datetime
import glob
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import sys
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch

# Load environment variables
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database configuration
DB_SERVER = os.getenv('DB_SERVER', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'CDR_DB')
DB_USER = os.getenv('DB_USER', 'sa')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'YourStrong@Passw0rd')
CDR_TABLE = os.getenv('CDR_TABLE', 'cdr_records')
REPORT_DIR = 'statistics'

# Create SQLAlchemy URL
connection_url = URL.create(
    drivername="mssql+pyodbc",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_SERVER,
    database=DB_NAME,
    query={"driver": "ODBC Driver 18 for SQL Server", "TrustServerCertificate": "yes"}
)

def get_engine():
    try:
        return create_engine(connection_url)
    except Exception as e:
        logging.error(f"Failed to create database engine: {str(e)}")
        sys.exit(1)

def execute_query(query, params=None):
    """Execute a SQL query and return results as a DataFrame"""
    try:
        with get_engine().connect() as connection:
            return pd.read_sql_query(text(query), connection, params=params)
    except Exception as e:
        logging.error(f"Query execution failed: {str(e)}")
        raise

def analyze_call_statuses():
    try:
        query = f"""
            SELECT 
                status,
                COUNT(*) as count,
                CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {CDR_TABLE}) as DECIMAL(5,2)) as percentage,
                CAST(AVG(CAST(duration as FLOAT)) as DECIMAL(10,2)) as avg_duration
            FROM {CDR_TABLE}
            GROUP BY status
            ORDER BY count DESC
        """
        df = execute_query(query)
        print('Call Status Statistics:')
        print(df)
        
        # Save to CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = os.path.join(REPORT_DIR, f'call_stats_{timestamp}.csv')
        df.to_csv(csv_path, index=False)
        
        # Create visualization
        plt.figure(figsize=(8, 6))
        sns.barplot(data=df, x='status', y='count')
        plt.title('Call Status Distribution')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(REPORT_DIR, f'call_status_analysis_{timestamp}.png'))
        plt.close()
        
        return df, timestamp
    except Exception as e:
        logging.error(f"Error in analyze_call_statuses: {str(e)}")
        raise

def analyze_null_values():
    try:
        # Get column names
        query = f"SELECT TOP 0 * FROM {CDR_TABLE}"
        columns = execute_query(query).columns.tolist()
        
        # Count nulls for each column
        null_counts = []
        for col in columns:
            query = f"SELECT COUNT(*) as null_count FROM {CDR_TABLE} WHERE {col} IS NULL"
            count = execute_query(query)['null_count'][0]
            null_counts.append(count)
        
        df = pd.DataFrame([null_counts], columns=columns)
        print('Null Value Counts:')
        print(df.T)
        
        # Create visualization
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        plt.figure(figsize=(12, 1))
        sns.heatmap(df, annot=True, cmap='Reds', cbar=False)
        plt.title('Null Values Heatmap')
        plt.tight_layout()
        plt.savefig(os.path.join(REPORT_DIR, f'null_values_heatmap_{timestamp}.png'))
        plt.close()
        
        return df, timestamp
    except Exception as e:
        logging.error(f"Error in analyze_null_values: {str(e)}")
        raise

def analyze_value_counts():
    try:
        # Get column names
        query = f"SELECT TOP 0 * FROM {CDR_TABLE}"
        columns = execute_query(query).columns.tolist()
        categorical_columns = [col for col in columns if col not in ['duration', 'timestamp']]
        
        all_results = []
        for column in categorical_columns:
            safe_column = f'[{column}]'
            query = f"""
                SELECT 
                    {safe_column} as value,
                    COUNT(*) as count,
                    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {CDR_TABLE}) as DECIMAL(5,2)) as percentage,
                    MIN(duration) as min_duration,
                    MAX(duration) as max_duration,
                    AVG(CAST(duration as FLOAT)) as avg_duration,
                    COUNT(CASE WHEN duration = 0 THEN 1 END) as zero_duration_count,
                    CAST(COUNT(CASE WHEN duration = 0 THEN 1 END) * 100.0 / COUNT(*) as DECIMAL(5,2)) as zero_duration_percentage,
                    COUNT(CASE WHEN status LIKE '%fail%' OR status LIKE '%error%' THEN 1 END) as failed_calls,
                    CAST(COUNT(CASE WHEN status LIKE '%fail%' OR status LIKE '%error%' THEN 1 END) * 100.0 / COUNT(*) as DECIMAL(5,2)) as failed_calls_percentage
                FROM {CDR_TABLE}
                GROUP BY {safe_column}
                ORDER BY count DESC
            """
            print(f"Executing query for column: {column}")
            df = execute_query(query)
            print(f'DEBUG: Raw results for {column}:', df.head())
            all_results.append(df)
            
            # Create visualization
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            plt.figure(figsize=(15, 5))
            sns.barplot(data=df, x='value', y='count')
            plt.title(f'{column} Value Counts')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            plt.savefig(os.path.join(REPORT_DIR, f'{column}_distribution_{timestamp}.png'))
            plt.close()
        
        # Save combined results
        if all_results:
            combined_df = pd.concat(all_results)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            combined_df.to_csv(os.path.join(REPORT_DIR, f'value_counts_{timestamp}.csv'), index=False)
            return combined_df, timestamp
        
        return None, None
    except Exception as e:
        logging.error(f"Error in analyze_value_counts: {str(e)}")
        raise

def generate_html_report(stats_df, stats_timestamp, null_df, null_timestamp, value_counts_df, value_counts_timestamp):
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        html_path = os.path.join(REPORT_DIR, 'html', f'cdr_statistics_report_{timestamp}.html')
        # Ensure the html directory exists
        os.makedirs(os.path.join(REPORT_DIR, 'html'), exist_ok=True)
        # HTML content
        html = f'''
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>CDR Statistics Report</title>
            <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
            <style>
                .metric-card {{
                    background: #f8f9fa;
                    border-radius: 8px;
                    padding: 20px;
                    margin-bottom: 20px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                .metric-value {{
                    font-size: 24px;
                    font-weight: bold;
                    color: #0d6efd;
                }}
                .metric-label {{
                    color: #6c757d;
                    font-size: 14px;
                }}
            </style>
        </head>
        <body class="bg-light">
        <div class="container py-4">
            <h1 class="mb-4">CDR Statistics Report</h1>
            
            <div class="row mb-4">
                <div class="col-md-4">
                    <div class="metric-card">
                        <div class="metric-value">{stats_df['count'].sum():,}</div>
                        <div class="metric-label">Total Calls</div>
                    </div>
                </div>
                <div class="col-md-4">
                    <div class="metric-card">
                        <div class="metric-value">{stats_df[stats_df['status'] == 'failed']['count'].sum():,}</div>
                        <div class="metric-label">Failed Calls</div>
                    </div>
                </div>
                <div class="col-md-4">
                    <div class="metric-card">
                        <div class="metric-value">{stats_df['avg_duration'].mean():.2f}</div>
                        <div class="metric-label">Average Duration (seconds)</div>
                    </div>
                </div>
            </div>

            <h2>Call Status Summary</h2>
            {stats_df.to_html(classes='table table-striped', index=False)}
            <img src="../call_status_analysis_{stats_timestamp}.png" class="img-fluid my-3" alt="Call Status Distribution">
            
            <h2>Null Values Analysis</h2>
            {null_df.T.to_html(classes='table table-striped')}
            <img src="../null_values_heatmap_{null_timestamp}.png" class="img-fluid my-3" alt="Null Values Heatmap">
            
            <h2>Value Counts Analysis</h2>
            <a href="../value_counts_{value_counts_timestamp}.csv" download class="btn btn-primary mb-3">Download Value Counts CSV</a>
            {value_counts_df.head(20).to_html(classes='table table-bordered', index=False)}
            
            <footer class="mt-5 text-muted">Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</footer>
        </div>
        </body>
        </html>
        '''
        
        # Save HTML
        with open(html_path, 'w') as f:
            f.write(html)
        print(f'HTML report generated: {html_path}')
        return html_path
    except Exception as e:
        logging.error(f"Error in generate_html_report: {str(e)}")
        raise

def generate_pdf_report(stats_df, stats_timestamp, null_df, null_timestamp, value_counts_df, value_counts_timestamp):
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        pdf_path = os.path.join(REPORT_DIR, 'html', f'cdr_statistics_report_{timestamp}.pdf')
        
        # Ensure the html directory exists
        os.makedirs(os.path.join(REPORT_DIR, 'html'), exist_ok=True)
        
        # Create the PDF document
        doc = SimpleDocTemplate(pdf_path, pagesize=letter)
        styles = getSampleStyleSheet()
        story = []
        
        # Title
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            spaceAfter=30
        )
        story.append(Paragraph("CDR Statistics Report", title_style))
        story.append(Spacer(1, 20))
        
        # Summary Metrics
        metrics_style = ParagraphStyle(
            'Metrics',
            parent=styles['Normal'],
            fontSize=14,
            spaceAfter=12
        )
        story.append(Paragraph(f"Total Calls: {stats_df['count'].sum():,}", metrics_style))
        story.append(Paragraph(f"Failed Calls: {stats_df[stats_df['status'] == 'failed']['count'].sum():,}", metrics_style))
        story.append(Paragraph(f"Average Duration: {stats_df['avg_duration'].mean():.2f} seconds", metrics_style))
        story.append(Spacer(1, 20))
        
        # Call Status Summary
        story.append(Paragraph("Call Status Summary", styles['Heading2']))
        story.append(Spacer(1, 10))
        
        # Convert DataFrame to list of lists for the table
        stats_data = [stats_df.columns.tolist()] + stats_df.values.tolist()
        stats_table = Table(stats_data)
        stats_table.setStyle(TableStyle([
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
        story.append(stats_table)
        story.append(Spacer(1, 20))
        
        # Add Call Status Distribution Plot
        plot_path = os.path.join(REPORT_DIR, f'call_status_analysis_{stats_timestamp}.png')
        if os.path.exists(plot_path):
            img = Image(plot_path, width=6*inch, height=4*inch)
            story.append(img)
            story.append(Spacer(1, 20))
        
        # Null Values Analysis
        story.append(Paragraph("Null Values Analysis", styles['Heading2']))
        story.append(Spacer(1, 10))
        
        # Convert null_df to list of lists for the table
        null_data = [['Column', 'Null Count']] + [[col, count] for col, count in null_df.T[0].items()]
        null_table = Table(null_data)
        null_table.setStyle(TableStyle([
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
        story.append(null_table)
        story.append(Spacer(1, 20))
        
        # Add Null Values Heatmap
        heatmap_path = os.path.join(REPORT_DIR, f'null_values_heatmap_{null_timestamp}.png')
        if os.path.exists(heatmap_path):
            img = Image(heatmap_path, width=6*inch, height=1*inch)
            story.append(img)
            story.append(Spacer(1, 20))
        
        # Value Counts Analysis
        story.append(Paragraph("Value Counts Analysis", styles['Heading2']))
        story.append(Spacer(1, 10))
        
        # Convert value_counts_df to list of lists for the table (first 20 rows)
        value_data = [value_counts_df.columns.tolist()] + value_counts_df.head(20).values.tolist()
        value_table = Table(value_data)
        value_table.setStyle(TableStyle([
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
        story.append(value_table)
        
        # Footer
        story.append(Spacer(1, 30))
        footer_style = ParagraphStyle(
            'Footer',
            parent=styles['Normal'],
            fontSize=10,
            textColor=colors.grey
        )
        story.append(Paragraph(f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", footer_style))
        
        # Build the PDF
        doc.build(story)
        print(f'PDF report generated: {pdf_path}')
        return pdf_path
    except Exception as e:
        logging.error(f"Error in generate_pdf_report: {str(e)}")
        raise

def main():
    try:
        # Ensure report directory exists
        os.makedirs(REPORT_DIR, exist_ok=True)
        
        # Run analyses
        print("Starting analyze_call_statuses...")
        stats_df, stats_timestamp = analyze_call_statuses()
        print("analyze_call_statuses completed.")
        
        print("Starting analyze_null_values...")
        null_df, null_timestamp = analyze_null_values()
        print("analyze_null_values completed.")
        
        print("Starting analyze_value_counts...")
        value_counts_df, value_counts_timestamp = analyze_value_counts()
        print("analyze_value_counts completed.")
        
        # Generate reports
        print("Starting generate_html_report...")
        if all([stats_df is not None, null_df is not None, value_counts_df is not None]):
            generate_html_report(stats_df, stats_timestamp, null_df, null_timestamp, 
                               value_counts_df, value_counts_timestamp)
            generate_pdf_report(stats_df, stats_timestamp, null_df, null_timestamp,
                              value_counts_df, value_counts_timestamp)
        else:
            print("One or more DataFrames are None. Reports not generated.")
        print("Reports generation completed.")
        
    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main() 