import pandas as pd
import mysql.connector
from mysql.connector import Error
import yaml


def read_db_config(filename='config.yaml'):
    """Read database configuration from a YAML file."""
    with open(filename, 'r') as f:
        db_config = yaml.safe_load(f)
    return db_config['mysql']

def connect():
    """Connect to MySQL database."""
    try:
        db_config = read_db_config()
        print('Connecting to MySQL database...')
        conn = mysql.connector.connect(**db_config)
        if conn.is_connected():
            print('Connected to MySQL database')
            return conn
    except Error as e:
        print(e)
    return None

def disconnect(conn):
    """Disconnect from MySQL database."""
    if conn is not None and conn.is_connected():
        conn.close()
        print('Disconnected from MySQL database')

# Example usage
column_mapping = {
    'Company-Id': 'company_id',
    'D-U-N-S® Number': 'duns_id',
    'Company Name': 'name',
    'Country-Code': 'country',
    'Verified': 'entity_status', 
}

default_values = {
    'industry': '',
    'entity_type': 'C',
}

# df = pd.read_csv("company_details_s3_updated.csv")
# df.rename(columns={'Company-Id':'company_id', 'D-U-N-S® Number':'duns_id', 'Company Name':'name', 'Country-Code':'country', 'Verified': 'entity_status'}, inplace=True)
# df["industry"] = ''
# df["entity_type"] = 'C'

# df.drop(columns=['Source', 'Conf_Score'], inplace=True)
# print(df.head())

def upload_csv_to_mysql(csv_file, table_name):
    """Upload data from a CSV file to a MySQL table."""
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file)
    df = df.iloc[65000:]

    # Replace NaN values with None (which translates to NULL in MySQL)
    df.fillna('', inplace=True)
    
    # Connect to the database
    conn = connect()
    if conn is None:
        print("Failed to connect to the database.")
        return
    
    cursor = conn.cursor()
    row_count = 0
    
    # Iterate over the DataFrame and insert data into the table
    for index, row in df.iterrows():
        # Prepare SQL query
        sql_columns = ', '.join(row.index)
        placeholders = ', '.join(['%s'] * len(row))
        sql = f"INSERT INTO {table_name} ({sql_columns}) VALUES ({placeholders})"

        if row["company_id"] == '':
            print("Skipping row with missing company_id")
        
        # Execute SQL query
        try:
            cursor.execute(sql, tuple(row))
            print(f"Uploaded {row["company_id"]} successfully")
            row_count += 1

            # Commit every 1000 rows
            if row_count % 1000 == 0:
                conn.commit()
                print(f"Committed {row_count} rows to the database.")
                
        except Error as e:
            print(f"Error: {e}")
            print(f"Failed to upload: {row["company_id"]}")
            conn.commit()
            print("Stopped and comitted")

    # Final commit to save any remaining data
    try:
        conn.commit()
        print(f"Final commit: {row_count} rows uploaded.")
    except Error as e:
        print(f"Error during final commit: {e}")
    
    print(f"Data from {csv_file} uploaded to {table_name} successfully.")

    # Disconnect from the database
    disconnect(conn)

upload_csv_to_mysql("company_details_s3_updated.csv", "d_company_details")


# def print_rows_from_csv(csv_file, start_row):
#     try:
#         # Load the CSV file into a DataFrame
#         df = pd.read_csv(csv_file)
#         df = df.iloc[64999:]

#         print(df.head(10))
        
#         # Check if start_row is within the DataFrame's range
#         if start_row >= len(df):
#             print(f"Start row {start_row} is out of range. The DataFrame has only {len(df)} rows.")
#             return
        
#         # Print rows starting from the specified start_row
#         # for index, row in df.iloc[start_row:].iterrows():
#         #     print(row.to_dict())
    
#     except Exception as e:
#         print(f"Error reading CSV file: {e}")

# # Call the function to print rows starting from the 65000th row
# print_rows_from_csv("company_details_s3_updated.csv", 64990)

>>>>>>> 0825e8a6391d620b194bc7b72c7b5f2faadb4347
