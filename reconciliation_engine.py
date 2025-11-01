
import polars as pl
from pathlib import Path

class ReconciliationEngine:

    
    def __init__(self, claims_path, invoices_path):

        self.claims_path = Path(claims_path)
        self.invoices_path = Path(invoices_path)
        self.claims_df = None
        self.invoices_df = None
        self.reconciliation_df = None
        
    def load_data(self):
  
        # read CSV files
        self.claims_df = pl.read_csv(self.claims_path)
        self.invoices_df = pl.read_csv(self.invoices_path)
     
        
    def process_reconciliation(self):
  
        
        invoice_totals = self.invoices_df.group_by('claim_id').agg(
            pl.col('transaction_value').sum().alias('total_transaction_value')
        )
        
        reconciliation = self.claims_df.join(
            invoice_totals, 
            on='claim_id', 
            how='left'
        )
        

        reconciliation = reconciliation.with_columns(
            pl.col('total_transaction_value').fill_null(0)
        )

        reconciliation = reconciliation.with_columns([

            (pl.col('total_transaction_value') - pl.col('benefit_amount')).alias('variance'),
            pl.when(
                pl.col('total_transaction_value') == pl.col('benefit_amount')
            ).then(pl.lit('BALANCED'))
            .when(
                pl.col('total_transaction_value') > pl.col('benefit_amount')
            ).then(pl.lit('OVERPAID'))
            .otherwise(pl.lit('UNDERPAID'))
            .alias('reconciliation_status')
        ])
        
        self.reconciliation_df = reconciliation
        

    def generate_statistics(self):
        total_claims = len(self.reconciliation_df)
        
        status_counts = self.reconciliation_df.group_by('reconciliation_status').agg(
            pl.len().alias('count')
        )
        

        status_dict = {row['reconciliation_status']: row['count'] 
                      for row in status_counts.to_dicts()}
        
        balanced = status_dict.get('BALANCED', 0)
        overpaid = status_dict.get('OVERPAID', 0)
        underpaid = status_dict.get('UNDERPAID', 0)
        
        # calculate percentage
        # the if is for the devison by zero
        balanced_pct = (balanced / total_claims * 100) if total_claims > 0 else 0
        overpaid_pct = (overpaid / total_claims * 100) if total_claims > 0 else 0
        underpaid_pct = (underpaid / total_claims * 100) if total_claims > 0 else 0
        
        total_overpaid = self.reconciliation_df.filter(
            pl.col('reconciliation_status') == 'OVERPAID'
        ).select(pl.col('variance').sum()).item()
        
        if total_overpaid is None:
            total_overpaid = 0
        
        total_underpaid = abs(self.reconciliation_df.filter(
            pl.col('reconciliation_status') == 'UNDERPAID'
        ).select(pl.col('variance').sum()).item() or 0)
        
 
        claim_status_counts = self.reconciliation_df.group_by('claim_status').agg(
            pl.len().alias('count')
        )
        claim_status_dict = {row['claim_status']: row['count'] 
                            for row in claim_status_counts.to_dicts()}

        provider_stats = self.reconciliation_df.group_by('provider_name').agg([
            pl.len().alias('count'),
            pl.col('variance').sum().alias('total_variance')
        ]).sort('total_variance', descending=True).head(5)
        
 
        insurance_stats = self.reconciliation_df.group_by('insurance_company').agg([
            pl.len().alias('count'),
            pl.col('variance').sum().alias('total_variance'),
            pl.col('variance').mean().alias('avg_variance')
        ]).sort('total_variance', descending=True)
        
        return {
            'total_claims': total_claims,
            'balanced': balanced,
            'balanced_pct': balanced_pct,
            'overpaid': overpaid,
            'overpaid_pct': overpaid_pct,
            'underpaid': underpaid,
            'underpaid_pct': underpaid_pct,
            'total_overpaid_amount': total_overpaid,
            'total_underpaid_amount': total_underpaid,
            'claim_status_counts': claim_status_dict,
            'top_providers': provider_stats.to_dicts(),
            'insurance_stats': insurance_stats.to_dicts()
        }
    
    def generate_html_report(self, output_path='report.html'):

        stats = self.generate_statistics()
        
        rows = self.reconciliation_df.to_dicts()
        
        table_rows = ""
        for row in rows:
            table_rows += "<tr>"
            table_rows += f"<td>{row['claim_id']}</td>"
            table_rows += f"<td>{row['patient_id']}</td>"
            table_rows += f"<td>{row['date_of_service']}</td>"
            table_rows += f"<td>{row['provider_name']}</td>"
            table_rows += f"<td>{row['insurance_company']}</td>"
            table_rows += f"<td>${row['charges_amount']:,.2f}</td>"
            table_rows += f"<td>${row['benefit_amount']:,.2f}</td>"
            table_rows += f"<td>${row['total_transaction_value']:,.2f}</td>"
            

            claim_status = row['claim_status']
            status_class = ''
            if claim_status == 'Approved':
                status_class = 'status-balanced'
            elif claim_status == 'Denied':
                status_class = 'status-overpaid'
            elif claim_status == 'Pending':
                status_class = 'status-underpaid'
            table_rows += f'<td><span class="status-badge {status_class}">{claim_status}</span></td>'
            
            status = row['reconciliation_status']
            badge_class = ''
            if status == 'BALANCED':
                badge_class = 'status-balanced'
            elif status == 'OVERPAID':
                badge_class = 'status-overpaid'
            elif status == 'UNDERPAID':
                badge_class = 'status-underpaid'
            
            table_rows += f'<td><span class="status-badge {badge_class}">{status}</span></td>'
            table_rows += f"<td>${row['variance']:,.2f}</td>"
            table_rows += "</tr>"
        
        table_html = f"""
        <table class="data-table">
            <thead>
                <tr>
                    <th>Claim ID</th>
                    <th>Patient ID</th>
                    <th>Date of Service</th>
                    <th>Provider</th>
                    <th>Insurance Company</th>
                    <th>Charges Amount</th>
                    <th>Benefit Amount</th>
                    <th>Total Transaction Value</th>
                    <th>Claim Status</th>
                    <th>Reconciliation Status</th>
                    <th>Variance</th>
                </tr>
            </thead>
            <tbody>
                {table_rows}
            </tbody>
        </table>
        """
        
        # create HTML report
        html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Insurance Claims Reconciliation Report</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            line-height: 1.6;
            color: #333;
            background: #f5f5f5;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            padding: 40px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        h1 {{
            color: #2c3e50;
            margin-bottom: 10px;
            font-size: 32px;
        }}
        
        .subtitle {{
            color: #7f8c8d;
            margin-bottom: 30px;
            font-size: 16px;
        }}
        
        h2 {{
            color: #34495e;
            margin: 30px 0 20px 0;
            font-size: 24px;
            border-bottom: 2px solid #3498db;
            padding-bottom: 10px;
        }}
        
        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 30px 0;
        }}
        
        .stat-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 25px;
            border-radius: 8px;
            color: white;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        
        .stat-card.balanced {{
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        }}
        
        .stat-card.overpaid {{
            background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%);
        }}
        
        .stat-card.underpaid {{
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        }}
        
        .stat-card h3 {{
            font-size: 14px;
            font-weight: 500;
            opacity: 0.9;
            margin-bottom: 10px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        
        .stat-value {{
            font-size: 36px;
            font-weight: bold;
            margin-bottom: 5px;
        }}
        
        .stat-detail {{
            font-size: 14px;
            opacity: 0.9;
        }}
        
        .amount-cards {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        
        .amount-card {{
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            border-left: 4px solid #3498db;
        }}
        
        .amount-card.overpaid {{
            border-left-color: #e74c3c;
        }}
        
        .amount-card.underpaid {{
            border-left-color: #f39c12;
        }}
        
        .amount-label {{
            font-size: 14px;
            color: #7f8c8d;
            margin-bottom: 8px;
            font-weight: 500;
        }}
        
        .amount-value {{
            font-size: 28px;
            font-weight: bold;
            color: #2c3e50;
        }}
        
        .data-table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            font-size: 14px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        .data-table thead {{
            background: #34495e;
            color: white;
        }}
        
        .data-table th {{
            padding: 12px;
            text-align: left;
            font-weight: 600;
            text-transform: uppercase;
            font-size: 12px;
            letter-spacing: 0.5px;
        }}
        
        .data-table td {{
            padding: 12px;
            border-bottom: 1px solid #ecf0f1;
        }}
        
        .data-table tbody tr:hover {{
            background: #f8f9fa;
        }}
        
        .data-table tbody tr:nth-child(even) {{
            background: #fafafa;
        }}
        
        .status-badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 600;
            text-transform: uppercase;
        }}
        
        .status-balanced {{
            background: #d4edda;
            color: #155724;
        }}
        
        .status-overpaid {{
            background: #f8d7da;
            color: #721c24;
        }}
        
        .status-underpaid {{
            background: #fff3cd;
            color: #856404;
        }}
        
        h3 {{
            color: #34495e;
            margin: 25px 0 15px 0;
            font-size: 20px;
        }}
        
        .insight-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}
        
        .insight-card {{
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
            border-left: 4px solid #3498db;
        }}
        
        .insight-label {{
            font-size: 14px;
            color: #7f8c8d;
            margin-bottom: 8px;
        }}
        
        .insight-value {{
            font-size: 32px;
            font-weight: bold;
            color: #2c3e50;
        }}
        
        .insight-table {{
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
            font-size: 14px;
            background: white;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        
        .insight-table thead {{
            background: #ecf0f1;
            color: #2c3e50;
        }}
        
        .insight-table th {{
            padding: 12px;
            text-align: left;
            font-weight: 600;
        }}
        
        .insight-table td {{
            padding: 10px 12px;
            border-bottom: 1px solid #ecf0f1;
        }}
        
        .insight-table tbody tr:hover {{
            background: #f8f9fa;
        }}
        
        .footer {{
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #ecf0f1;
            text-align: center;
            color: #7f8c8d;
            font-size: 14px;
        }}
        
        @media print {{
            body {{
                background: white;
            }}
            .container {{
                box-shadow: none;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Insurance Claims Reconciliation Report</h1>
        <p class="subtitle">Analysis of claim payments vs. invoice transactions (Powered by Polars)</p>
        
        <h2>Executive Summary</h2>
        
        <div class="summary">
            <div class="stat-card">
                <h3>Total Claims</h3>
                <div class="stat-value">{stats['total_claims']:,}</div>
                <div class="stat-detail">All processed claims</div>
            </div>
            
            <div class="stat-card balanced">
                <h3>Balanced Claims</h3>
                <div class="stat-value">{stats['balanced']:,}</div>
                <div class="stat-detail">{stats['balanced_pct']:.1f}% of total</div>
            </div>
            
            <div class="stat-card overpaid">
                <h3>Overpaid Claims</h3>
                <div class="stat-value">{stats['overpaid']:,}</div>
                <div class="stat-detail">{stats['overpaid_pct']:.1f}% of total</div>
            </div>
            
            <div class="stat-card underpaid">
                <h3>Underpaid Claims</h3>
                <div class="stat-value">{stats['underpaid']:,}</div>
                <div class="stat-detail">{stats['underpaid_pct']:.1f}% of total</div>
            </div>
        </div>
        
        <div class="amount-cards">
            <div class="amount-card overpaid">
                <div class="amount-label">Total Overpaid Amount</div>
                <div class="amount-value">${stats['total_overpaid_amount']:,.2f}</div>
            </div>
            
            <div class="amount-card underpaid">
                <div class="amount-label">Total Underpaid Amount</div>
                <div class="amount-value">${stats['total_underpaid_amount']:,.2f}</div>
            </div>
        </div>
        
        <h2>Additional Insights</h2>
        
        <h3>Claims by Status</h3>
        <div class="insight-grid">
            <div class="insight-card">
                <div class="insight-label">Approved Claims</div>
                <div class="insight-value">{stats['claim_status_counts'].get('Approved', 0):,}</div>
            </div>
            <div class="insight-card">
                <div class="insight-label">Pending Claims</div>
                <div class="insight-value">{stats['claim_status_counts'].get('Pending', 0):,}</div>
            </div>
            <div class="insight-card">
                <div class="insight-label">Denied Claims</div>
                <div class="insight-value">{stats['claim_status_counts'].get('Denied', 0):,}</div>
            </div>
        </div>
        
        <h3>Top 5 Providers by Total Variance</h3>
        <table class="insight-table">
            <thead>
                <tr>
                    <th>Provider Name</th>
                    <th>Number of Claims</th>
                    <th>Total Variance</th>
                </tr>
            </thead>
            <tbody>
                {''.join(f'''
                <tr>
                    <td>{provider['provider_name']}</td>
                    <td>{provider['count']:,}</td>
                    <td>${provider['total_variance']:,.2f}</td>
                </tr>
                ''' for provider in stats['top_providers'])}
            </tbody>
        </table>
        
        <h3>Analysis by Insurance Company</h3>
        <table class="insight-table">
            <thead>
                <tr>
                    <th>Insurance Company</th>
                    <th>Number of Claims</th>
                    <th>Total Variance</th>
                    <th>Average Variance</th>
                </tr>
            </thead>
            <tbody>
                {''.join(f'''
                <tr>
                    <td>{ins['insurance_company']}</td>
                    <td>{ins['count']:,}</td>
                    <td>${ins['total_variance']:,.2f}</td>
                    <td>${ins['avg_variance']:,.2f}</td>
                </tr>
                ''' for ins in stats['insurance_stats'])}
            </tbody>
        </table>
        
        <h2>Detailed Reconciliation Table</h2>
        
        {table_html}
    </div>
</body>
</html>
"""
        
        # write to file
        output_file = Path(output_path)
        output_file.write_text(html_content)
        

        
        return stats
    
    def run(self, output_path='report.html'):

        self.load_data()
        self.process_reconciliation()
        stats = self.generate_html_report(output_path)
        
    
        
        return stats

def main():
 
    engine = ReconciliationEngine(
        claims_path='data/claims.csv',
        invoices_path='data/invoices.csv'
    )
    
    engine.run(output_path='report.html')

if __name__ == '__main__':
    main()