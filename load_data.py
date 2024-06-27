"""
This script loads the NYC Airbnb dataset from a CSV file and stores it in a PostgreSQL database.

The script performs the following steps:
1. Reads the database credentials and CSV file path.
2. Establishes a connection to the PostgreSQL database.
3. Reads the CSV file into a pandas DataFrame.
4. Creates the schema for the raw listings table in the PostgreSQL database.
5. Executes the SQL command to create the table if it does not already exist.
6. Loads the data from the pandas DataFrame into the PostgreSQL table.

Dependencies:
- pandas
- sqlalchemy

Usage:
- Ensure the PostgreSQL database is running and accessible.
- Modify the database credentials and CSV file path as needed.
- Run the script to load the data into the PostgreSQL database.
"""
import pandas as pd
from sqlalchemy import create_engine, text

# Database credentials
db_user = 'postgres'
db_password = 'test'
db_host = 'localhost'
db_port = '5432'
db_name = 'airbnb_nyc'

# CSV file path
csv_file_path = 'AB_NYC_2019.csv'

# Create a connection to the PostgreSQL database
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Load the dataset
df = pd.read_csv(csv_file_path)

# Create table schema
create_table_query = text("""
CREATE TABLE IF NOT EXISTS listings_raw (
    id SERIAL PRIMARY KEY,
    name TEXT,
    host_id INT,
    host_name TEXT,
    neighbourhood_group TEXT,
    neighbourhood TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    room_type TEXT,
    price INT,
    minimum_nights INT,
    number_of_reviews INT,
    last_review DATE,
    reviews_per_month DOUBLE PRECISION,
    calculated_host_listings_count INT,
    availability_365 INT
);
""")

# Execute the SQL command to create the table
with engine.connect() as connection:
    print("Executing SQL Command to create table:")
    connection.execute(create_table_query)
    print("Table created successfully.")

# Load raw data into PostgreSQL table
df.to_sql('listings_raw', engine, if_exists='replace', index=False)
print("Data loaded into listings_raw table successfully.")
