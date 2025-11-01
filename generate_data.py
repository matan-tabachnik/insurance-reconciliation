
import polars as pl
import random
from datetime import datetime, timedelta
from faker import Faker


fake = Faker()


NUM_PATIENTS = 200
MIN_CLAIMS_PER_PATIENT = 2
MAX_CLAIMS_PER_PATIENT = 20
MIN_INVOICES_PER_CLAIM = 1
MAX_INVOICES_PER_CLAIM = 5

def generate_patients(num_patients):

    patients = []

    
    us_states = [
        'CA', 'TX', 'FL', 'NY', 'PA', 'IL', 'OH', 'GA', 'NC', 'MI',
        'NJ', 'VA', 'WA', 'AZ', 'MA', 'TN', 'IN', 'MO', 'MD', 'WI'
    ]
    
    insurance_plans = ['Gold Plan', 'Silver Plan', 'Bronze Plan', 'PPO', 'HMO', 'EPO']
    
    for i in range(num_patients):
        patient = {
            'patient_id': f'P{i+1:04d}',
            'name': fake.name(),
            'age': random.randint(18, 90),  
            'state': random.choice(us_states), 
            'insurance_plan': random.choice(insurance_plans)  
        }
        patients.append(patient)
    
    return pl.DataFrame(patients)

def generate_claims(patients_df):

    claims = []
    claim_counter = 1
    
    claim_statuses = ['Approved', 'Pending', 'Denied']
    claim_status_weights = [0.70, 0.20, 0.10] 
    
    providers = [
        'City General Hospital', 'Memorial Medical Center', 'St. Mary\'s Hospital',
        'Dr. Sarah Johnson', 'Dr. Michael Chen', 'Dr. Emily Rodriguez',
        'HealthCare Clinic', 'Urgent Care Center', 'Primary Care Associates'
    ]
    
    insurance_companies = [
        'BlueCross BlueShield', 'United Healthcare', 'Aetna', 
        'Cigna', 'Humana', 'Kaiser Permanente'
    ]
    
    # Convert to list of dicts for easier iteration
    patients_list = patients_df.to_dicts()
    
    for patient in patients_list:
        num_claims = random.randint(MIN_CLAIMS_PER_PATIENT, MAX_CLAIMS_PER_PATIENT)
        
        for _ in range(num_claims):

            days_ago = random.randint(0, 730)
            service_date = datetime.now() - timedelta(days=days_ago)
            
            charges_amount = round(random.uniform(100, 5000), 2)
            
            benefit_factor = random.uniform(0.5, 1)
            benefit_amount = round(charges_amount * benefit_factor, 2)
            
            claim = {
                'claim_id': f'C{claim_counter:06d}',
                'patient_id': patient['patient_id'],
                'date_of_service': service_date.strftime('%Y-%m-%d'),
                'charges_amount': charges_amount,
                'benefit_amount': benefit_amount,
                'claim_status': random.choices(claim_statuses, weights=claim_status_weights)[0],
                'provider_name': random.choice(providers),
                'insurance_company': random.choice(insurance_companies)
            }
            claims.append(claim)
            claim_counter += 1
    
    return pl.DataFrame(claims)

def generate_invoices(claims_df):
 
    invoices = []
    invoice_counter = 1
    

    payment_statuses = ['Paid', 'Pending', 'Overdue']
    payment_status_weights = [0.70, 0.20, 0.10]  
    

    payment_methods = ['Check', 'ACH', 'Wire Transfer', 'Credit Card', 'Electronic Payment']
    
    claims_list = claims_df.to_dicts()
    
    for claim in claims_list:
        num_invoices = random.randint(MIN_INVOICES_PER_CLAIM, MAX_INVOICES_PER_CLAIM)
        
        # Get claim's date for invoice date calculation
        claim_date = datetime.strptime(claim['date_of_service'], '%Y-%m-%d')
        
        for _ in range(num_invoices):

            bill_type = random.choice(['fee', 'procedure payment'])
            

            transaction_value = round(random.uniform(-500, 2000), 2)

            if random.random() < 0.05:
                transaction_value = 0
            

            days_after_claim = random.randint(1, 60)
            invoice_date = claim_date + timedelta(days=days_after_claim)
            
            invoice = {
                'invoice_id': f'I{invoice_counter:07d}',
                'claim_id': claim['claim_id'],
                'type_of_bill': bill_type,
                'transaction_value': transaction_value,
                'invoice_date': invoice_date.strftime('%Y-%m-%d'),  
                'payment_status': random.choices(payment_statuses, weights=payment_status_weights)[0], 
                'payment_method': random.choice(payment_methods) 
            }
            invoices.append(invoice)
            invoice_counter += 1
    
    return pl.DataFrame(invoices)

def main():

    patients_df = generate_patients(NUM_PATIENTS)
    claims_df = generate_claims(patients_df)
    invoices_df = generate_invoices(claims_df)

    # Save to CSV
    patients_df.write_csv('data/patients.csv')
    claims_df.write_csv('data/claims.csv')
    invoices_df.write_csv('data/invoices.csv')

    print(f"Files created:")
    print(f"data/patients.csv ({len(patients_df)} rows)")
    print(f"data/claims.csv ({len(claims_df)} rows)")
    print(f"data/invoices.csv ({len(invoices_df)} rows)")
    print(f"Average claims per patient: {len(claims_df)/len(patients_df):.1f}")
    print(f"Average invoices per claim: {len(invoices_df)/len(claims_df):.1f}")

if __name__ == '__main__':
    main()