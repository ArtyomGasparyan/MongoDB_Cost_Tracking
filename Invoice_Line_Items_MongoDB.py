import requests
from requests.auth import HTTPDigestAuth
import pandas as pd
import mysql.connector
import os

# Company configurations (list of dictionaries)
companies = [
    {
        "public_key": os.getenv('mongodb_pk_cognaize'),
        "private_key": os.getenv('mongodb_private_k_cognaize'),
        "org_id": os.getenv('mongodb_org_id_cognaize')
    },
    {
        "public_key": os.getenv('mongodb_pk_cognaize_engineering'),
        "private_key": os.getenv('mongodb_private_k_cognaize_engineering'),
        "org_id": os.getenv('mongodb_org_id_cognaize_engineering')
    }
]

# MySQL connection details
mysql_host = os.getenv('bi_db_host_name')
mysql_user = os.getenv('bi_db_admin_user')
mysql_password = os.getenv('bi_db_admin_password')
mysql_database = "mongodb"

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

# Function to retrieve the latest invoice IDs from the database
def get_latest_invoice_ids(org_id):
    connection = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor = connection.cursor()
    query = f"SELECT id FROM invoices WHERE org_id = '{org_id}' ORDER BY end_date DESC LIMIT 2"
    cursor.execute(query)
    invoice_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.close()
    connection.close()
    
    return invoice_ids

# Function to delete existing rows that match the invoice_id in the invoices_line_items table
def delete_existing_line_items(invoice_ids):
    connection = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor = connection.cursor()

    for invoice_id in invoice_ids:
        delete_query = "DELETE FROM invoices_line_items WHERE invoice_id = %s"
        cursor.execute(delete_query, (invoice_id,))

    connection.commit()
    cursor.close()
    connection.close()
    print("Deleted matching records from the invoices_line_items table.")

# Function to insert data into the invoices_line_items table with NaN handling
def insert_line_items(df, batch_size=3000):
    # Convert NaN values in the DataFrame to None, which translates to NULL in MySQL
    df = df.where(pd.notnull(df), None)
    
    connection = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor = connection.cursor()

    insert_query = """
        INSERT INTO invoices_line_items (
            id, group_id, invoice_id, cluster_name, group_name, sku, created, 
            start_date, end_date, quantity, unit, unit_price_dollars, 
            total_price_cents, stitch_app_name, note, feature, metric_date
        ) VALUES (UUID(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    required_columns = ['group_id', 'invoice_id', 'cluster_name', 'group_name', 'sku', 
                        'created', 'start_date', 'end_date', 'quantity', 'unit', 
                        'unit_price_dollars', 'total_price_cents', 'stitch_app_name', 
                        'note', 'feature', 'metric_date']
    
    for col in required_columns:
        if col not in df.columns:
            df[col] = None
    
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size]
        data = [tuple(row) for row in batch[required_columns].values]
        
        cursor.executemany(insert_query, data)
        connection.commit()
        print(f"Inserted batch {i // batch_size + 1} with {len(data)} records")

    cursor.close()
    connection.close()
    print(f"Total {len(df)} line items inserted into invoices_line_items table.")

# Process data for each company
for company in companies:
    public_key = company["public_key"]
    private_key = company["private_key"]
    org_id = company["org_id"]
    
    # Retrieve invoice IDs from the database for each company
    invoice_ids = get_latest_invoice_ids(org_id)
    
    # DataFrame for storing extracted lineItems
    line_items_data = []
    
    # Loop through each invoice and get detailed information from MongoDB API
    for invoice_id in invoice_ids:
        detailed_url = f"https://cloud.mongodb.com/api/atlas/v1.0/orgs/{org_id}/invoices/{invoice_id}"
        detailed_response = requests.get(detailed_url, auth=HTTPDigestAuth(public_key, private_key))

        if detailed_response.status_code == 200:
            invoice_details = detailed_response.json()

            # Flatten and append lineItems
            if 'lineItems' in invoice_details and isinstance(invoice_details['lineItems'], list):
                for item in invoice_details['lineItems']:
                    item['invoice_id'] = invoice_id

                    # Convert date columns in lineItems to MySQL format
                    for date_col in ['created', 'endDate', 'startDate', 'metric_date']:
                        if date_col in item:
                            item[date_col] = convert_to_mysql_datetime(item[date_col])

                    line_items_data.append(item)

        else:
            print(f"Error fetching details for Invoice ID: {invoice_id}, Status Code: {detailed_response.status_code}")

    # Convert lineItems to DataFrame
    df_line_items = pd.DataFrame(line_items_data)

    # Map DataFrame columns to match SQL table structure
    df_line_items.rename(columns={
        'groupId': 'group_id',
        'invoice_id': 'invoice_id',
        'clusterName': 'cluster_name',
        'groupName': 'group_name',
        'sku': 'sku',
        'created': 'created',
        'startDate': 'start_date',
        'endDate': 'end_date',
        'quantity': 'quantity',
        'unit': 'unit',
        'unitPriceDollars': 'unit_price_dollars',
        'totalPriceCents': 'total_price_cents',
        'stitchAppName': 'stitch_app_name',
        'note': 'note',
        'feature': 'feature',
        'metricDate': 'metric_date'
    }, inplace=True)

    # Delete existing line items in MySQL table that match the invoice IDs in df_line_items
    delete_existing_line_items(df_line_items['invoice_id'].unique())

    # Insert the cleaned data into MySQL
    insert_line_items(df_line_items)

    # Save DataFrame to CSV file
    filename = f'invoices_line_items_{org_id}.csv'
    df_line_items.to_csv(filename, index=False)

    print(f"All data for org_id {org_id} has been saved to {filename}.")
