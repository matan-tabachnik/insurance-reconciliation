# Insurance Claims Reconciliation Engine

A Python-based reconciliation system that matches insurance claims with invoice transactions and generates detailed HTML reports for analysis.

## 📋 Overview

This project provides a solution for reconciling healthcare insurance claims against invoices. It generates synthetic patient, claim, and invoice data, then performs reconciliation analysis to identify balanced, overpaid, and underpaid claims.

## ✨ Features

- **Synthetic Data Generation**: Creates healthcare data including:
  - 200 unique patients with demographics
  - 2-20 claims per patient
  - 1-5 invoices per claim
  
- **Reconciliation Engine**: 
  - Matches invoice transactions to insurance benefit amounts
  - Identifies discrepancies (BALANCED, OVERPAID, UNDERPAID)
  - Calculates variances and aggregates statistics
  
- **HTML Report**:
  - Executive summary with key metrics
  - Detailed reconciliation table with all claims
  - Additional insights 

## 🛠️ Tech Stack

- **Python 3.8+**
- **Polars**: 
- **Faker**

## 📁 Project Structure

```
insurance-reconciliation/
│
├── generate_data.py          # Script to generate synthetic CSV data
├── reconciliation_engine.py  # Main reconciliation engine
├── README.md                  # Project documentation
├── requirements.txt           # Python dependencies
│
├── data/                      # Generated CSV files (created by script)
│   ├── patients.csv
│   ├── claims.csv
│   └── invoices.csv
│
└── report.html               # Generated reconciliation report
```

##  Installation

### Prerequisites

- Python 3.8 or higher
- pip (Python package manager)

### Setup

1. **Clone the repository**:
```bash
git clone https://github.com/matan-tabachnik/insurance-reconciliation.git
cd insurance-reconciliation
```

2. **Create a virtual environment**:
```bash
python -m venv venv

# On Windows
venv\Scripts\activate

# On macOS/Linux
source venv/bin/activate
```

3. **Install dependencies**:
```bash
pip install -r requirements.txt
```

## 📊 Usage

### Step 1: Generate Data

Generate synthetic patient, claim, and invoice data:

```bash
python generate_data.py
```

This will create three CSV files in the `data/` directory:
- `patients.csv` - 200 unique patients
- `claims.csv` - Multiple claims per patient (2-20)
- `invoices.csv` - Multiple invoices per claim (1-5)

**Sample Output:**
```
Files created:
data/patients.csv (200 rows)
data/claims.csv (2147 rows)
data/invoices.csv (6532 rows)
Average claims per patient: 10.7
Average invoices per claim: 3.0
```

### Step 2: Run Reconciliation

Process the data and generate the HTML report:

```bash
python reconciliation_engine.py
```

This will:
1. Read the CSV files from `data/`
2. Perform reconciliation analysis
3. Generate `report.html` in the project root

### Step 3: View the Report

```bash
start report.html
```
or Open `report.html` in your web browser to view the interactive reconciliation report.

## 📈 Data Schema

### Patients (`patients.csv`)
| Field | Type | Description |
|-------|------|-------------|
| patient_id | String | Unique patient identifier (P0001, P0002, ...) |
| name | String | Patient name |
| age | Integer | Patient age (18-90) |
| state | String | US state code |
| insurance_plan | String | Insurance plan type |

### Claims (`claims.csv`)
| Field | Type | Description |
|-------|------|-------------|
| claim_id | String | Unique claim identifier (C000001, C000002, ...) |
| patient_id | String | Foreign key to patient |
| date_of_service | Date | Service date (YYYY-MM-DD) |
| charges_amount | Float | Total charges for services |
| benefit_amount | Float | Amount approved by insurance (≤ charges_amount) |
| claim_status | String | Approved, Pending, or Denied |
| provider_name | String | Healthcare provider |
| insurance_company | String | Insurance company name |

### Invoices (`invoices.csv`)
| Field | Type | Description |
|-------|------|-------------|
| invoice_id | String | Unique invoice identifier (I0000001, I0000002, ...) |
| claim_id | String | Foreign key to claim |
| type_of_bill | String | "fee" or "procedure payment" |
| transaction_value | Float | Transaction amount (can be positive, negative, or zero) |
| invoice_date | Date | Invoice date (YYYY-MM-DD) |
| payment_status | String | Paid, Pending, or Overdue |
| payment_method | String | Payment method used |

## 🔍 Reconciliation Logic

The engine performs the following reconciliation:

1. **Calculate Total Transactions**: Sum all invoice `transaction_value` for each claim
2. **Compare with Benefit Amount**: Compare total transactions against the insurance `benefit_amount`
3. **Assign Status**:
   - **BALANCED**: `total_transaction_value == benefit_amount`
   - **OVERPAID**: `total_transaction_value > benefit_amount`
   - **UNDERPAID**: `total_transaction_value < benefit_amount`
4. **Calculate Variance**: `variance = total_transaction_value - benefit_amount`

## 📊 Report Output

The generated HTML report includes:

### Executive Summary
- Total number of claims
- Count and percentage of:
  - Balanced claims
  - Overpaid claims
  - Underpaid claims
- Total overpaid and underpaid amounts

### Additional Insights
- Claims breakdown by status (Approved/Pending/Denied)
- Top 5 providers by total variance
- Analysis by insurance company

### Detailed Table
Complete list of all claims with:
- Claim ID, Patient ID, Date of Service
- Provider and Insurance Company
- Charges Amount, Benefit Amount, Total Transaction Value
- Claim Status, Reconciliation Status, Variance



## 📝 Requirements

```txt
polars>=0.19.0
faker>=20.0.0
```

## 👨‍💻 Author

M - [Your GitHub Profile](https://github.com/matan-tabachnik)

