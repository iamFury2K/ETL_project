from metaflow import FlowSpec, step
import pandas as pd
from sqlalchemy import create_engine, text

class ETLFlow(FlowSpec):
    """
    A Metaflow-based ETL pipeline that extracts data from a PostgreSQL database,
    transforms it, and loads the transformed data back into PostgreSQL.

    The pipeline performs the following steps:
    1. Extracts raw Airbnb listings data from a PostgreSQL database.
    2. Transforms the data by normalizing date columns, handling missing values,
       and calculating additional metrics such as the average price per neighborhood.
    3. Loads the transformed data into a new PostgreSQL table.
    """

    @step
    def start(self):
        """
        The start step of the ETL pipeline. Initializes the flow and proceeds to the data extraction step.
        """
        print("Starting the ETL flow.")
        self.next(self.extract_data)

    @step
    def extract_data(self):
        """
        Extracts data from the PostgreSQL database. Connects to the database using the provided
        credentials and retrieves data from the 'listings_raw' table.

        The extracted data is stored in the instance variable `self.df`.
        """
        print("Extracting data from PostgreSQL.")
        # Database credentials
        db_user = 'postgres'
        db_password = 'test'
        db_host = 'localhost'
        db_port = '5432'
        db_name = 'airbnb_nyc'

        # Create a connection to the PostgreSQL database
        engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

        # Extract the data from the PostgreSQL database
        self.df = pd.read_sql('SELECT * FROM listings_raw', engine)
        print("Data extraction successful.")
        self.next(self.transform_data)

    @step
    def transform_data(self):
        """
        Transforms the extracted data. This step involves:
        1. Normalizing date columns.
        2. Handling missing values.
        3. Calculating additional metrics such as the average price per neighborhood.
        4. Merging calculated metrics back into the main dataframe.
        """
        print("Transforming data.")
        # Normalize the data (e.g., separate the date and time into different columns)
        self.df['last_review'] = pd.to_datetime(self.df['last_review'])

        # Handle missing values
        self.df['reviews_per_month'] = self.df['reviews_per_month'].fillna(0)
        self.df['last_review'] = self.df['last_review'].fillna(pd.Timestamp.now())

        # Calculate additional metrics (e.g., average price per neighborhood)
        avg_price_per_neighbourhood = self.df.groupby('neighbourhood')['price'].mean().reset_index()
        avg_price_per_neighbourhood.columns = ['neighbourhood', 'average_price']

        # Merge the average price back to the original dataframe
        self.df = self.df.merge(avg_price_per_neighbourhood, on='neighbourhood', how='left')
        self.next(self.load_transformed_data)

    @step
    def load_transformed_data(self):
        """
        Loads the transformed data back into the PostgreSQL database. This step involves:
        1. Creating a new table schema for the transformed data.
        2. Loading the transformed data into the new table.
        """
        print("Loading transformed data into PostgreSQL.")
        # Database credentials
        db_user = 'postgres'
        db_password = 'test'
        db_host = 'localhost'
        db_port = '5432'
        db_name = 'airbnb_nyc'

        # Create a connection to the PostgreSQL database
        engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

        # Create table schema for transformed data
        create_table_query = text("""
            CREATE TABLE IF NOT EXISTS listings_transformed (
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
                availability_365 INT,
                average_price DOUBLE PRECISION
            );
        """)

        with engine.connect() as connection:
            print("Executing SQL Command: ")
            print(create_table_query)
            connection.execute(create_table_query)
            print("Transformed table created successfully.")

        # Load transformed data into PostgreSQL table
        self.df.to_sql('listings_transformed', engine, if_exists='replace', index=False)
        print("Transformed data loaded into listings_transformed table successfully.")
        self.next(self.end)

    @step
    def end(self):
        """
        The final step of the ETL pipeline. Marks the completion of the ETL process.
        """
        print("ETL process completed successfully!")

if __name__ == '__main__':
    ETLFlow()
