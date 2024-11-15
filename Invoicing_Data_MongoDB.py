import requests
from requests.auth import HTTPDigestAuth
import pandas as pd
import mysql.connector
import os
import json

# MySQL connection details
mysql_host = os.getenv('db_host_name')
mysql_user = os.getenv('db_admin_user')
mysql_password = os.getenv('db_admin_password')
mysql_database = "mongodb"

# Company configurations
company_configs = [
    {
        "public_key": os.getenv('mongodb_pk'),
        "private_key": os.getenv('mongodb_private_k'),
        "org_id": os.getenv('mongodb_org_id')
    },
    {
        "public_key": os.getenv('mongodb_pk_2'),
        "private_key": os.getenv('mongodb_private_k_2'),
        "org_id": os.getenv('mongodb_org_id_2')
    }
]

# MySQL Date format
mysql_date_format = "%Y-%m-%d %H:%M:%S"

# Function to convert date columns to MySQL datetime format
def convert_to_mysql_datetime(date_str):
    if pd.isnull(date_str):
        return None
    try:
        return pd.to_datetime(date_str).strftime(mysql_date_format)
    except Exception:
        return None

# Function to connect to MySQL and fetch existing invoice IDs
def get_existing_invoice_ids():
    connection = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor = connection.cursor()
    cursor.execute(f"Select distinct(id) from invoices where status_name <> 'PENDING'")
    result = cursor.fetchall()
    existing_invoice_ids = {row[0] for row in result}
    cursor.close()
    connection.close()
    return existing_invoice_ids

# Function to delete existing rows that match the IDs in the DataFrame
def delete_existing_rows(df):
    connection = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor = connection.cursor()

    # Delete rows that match the 'id' in the DataFrame
    for invoice_id in df['id']:
        delete_query = f"DELETE FROM invoices WHERE id = %s"
        cursor.execute(delete_query, (invoice_id,))

    connection.commit()
    cursor.close()
    connection.close()
    print(f"Deleted {len(df)} matching records from the invoices table.")

# Function to insert data into the invoices table
def insert_into_invoices(df):
    connection = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor = connection.cursor()

    # Insert DataFrame records one by one
    insert_query = """
        INSERT INTO invoices (
            id, org_id, created, start_date, end_date, updated, 
            starting_balance_cents, amount_billed_cents, amount_paid_cents,
            credits_cents, subtotal_cents, refunds, sales_tax_cents, status_name
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    for _, row in df.iterrows():
        # Handle 'refunds' column: Convert list to JSON string or null if not applicable
        refunds_value = json.dumps(row['refunds']) if isinstance(row['refunds'], (list, dict)) else row['refunds']
        
        cursor.execute(insert_query, (
            row['id'], row['org_id'], row['created'], row['start_date'], row['end_date'], row['updated'],
            row['starting_balance_cents'], row['amount_billed_cents'], row['amount_paid_cents'], 
            row['credits_cents'], row['subtotal_cents'], refunds_value, row['sales_tax_cents'], row['status_name']
        ))
    
    connection.commit()
    cursor.close()
    connection.close()
    print(f"{len(df)} records inserted into invoices table.")

# Iterate over each company configuration
for config in company_configs:
    public_key = config["public_key"]
    private_key = config["private_key"]
    org_id = config["org_id"]

    # URL to get all invoices for the organization
    invoices_url = f"https://cloud.mongodb.com/api/atlas/v1.0/orgs/{org_id}/invoices"

    # Get existing invoice IDs from MySQL
    existing_invoice_ids = get_existing_invoice_ids()

    # Get all invoices
    response = requests.get(invoices_url, auth=HTTPDigestAuth(public_key, private_key))

    if response.status_code == 200:
        invoices = response.json()['results']
        
        # DataFrame for storing all invoice data without lineItems
        all_invoice_data = []

        # Loop through each invoice and get detailed information
        for invoice in invoices:
            invoice_id = invoice['id']
            
            # Only process invoices that are not already in MySQL
            if invoice_id not in existing_invoice_ids:
                print(f"Processing Invoice ID: {invoice_id} for organization {org_id}")
                
                # URL to get detailed billing info for each invoice
                detailed_url = f"https://cloud.mongodb.com/api/atlas/v1.0/orgs/{org_id}/invoices/{invoice_id}"
                detailed_response = requests.get(detailed_url, auth=HTTPDigestAuth(public_key, private_key))

                if detailed_response.status_code == 200:
                    invoice_details = detailed_response.json()

                    # Exclude the nested fields from the main invoice data
                    invoice_cleaned = {k: v for k, v in invoice_details.items() if k not in ['lineItems', 'payments', 'linkedInvoices', 'links']}
                    
                    # Convert date columns to MySQL format
                    for date_col in ['created', 'endDate', 'startDate', 'updated']:
                        if date_col in invoice_cleaned:
                            invoice_cleaned[date_col] = convert_to_mysql_datetime(invoice_cleaned[date_col])

                    all_invoice_data.append(invoice_cleaned)

        # Convert main invoice data (excluding nested fields) to a DataFrame
        df_main = pd.DataFrame(all_invoice_data)

        # Rename columns in df_main to match the MySQL table
        df_main.rename(columns={
            'id': 'id',
            'orgId': 'org_id',
            'created': 'created',
            'startDate': 'start_date',
            'endDate': 'end_date',
            'updated': 'updated',
            'startingBalanceCents': 'starting_balance_cents',
            'amountBilledCents': 'amount_billed_cents',
            'amountPaidCents': 'amount_paid_cents',
            'creditsCents': 'credits_cents',
            'subtotalCents': 'subtotal_cents',
            'refunds': 'refunds',
            'salesTaxCents': 'sales_tax_cents',
            'statusName': 'status_name'
        }, inplace=True)

        # Drop extra columns not required by MySQL and print them
        extra_columns = set(df_main.columns) - {'id', 'org_id', 'created', 'start_date', 'end_date', 'updated', 'starting_balance_cents',
                                                'amount_billed_cents', 'amount_paid_cents', 'credits_cents', 'subtotal_cents',
                                                'refunds', 'sales_tax_cents', 'status_name'}
        if extra_columns:
            print(f"Dropping the following extra columns: {extra_columns}")
            df_main.drop(columns=extra_columns, inplace=True)

        # Delete existing rows in MySQL table that match the IDs in df_main
        delete_existing_rows(df_main)

        # Insert the cleaned data into MySQL
        insert_into_invoices(df_main)

    else:
        print(f"Error fetching invoices for organization {org_id}: {response.status_code}, {response.text}")
