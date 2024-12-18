from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd
import os

load_dotenv()

# PostgreSQL connection details
DB_CONFIG = {
    "host": os.getenv("HOST"),
    "port": os.getenv("PORT"),
    "database": os.getenv("DATABASE"),
    "user": os.getenv("USERNAME"),
    "password": os.getenv("PASSWORD"),
}

# Azure Storage Account details
STORAGE_ACCOUNT_NAME = "cfbdata"
CONTAINER_NAME = "cfb-csv"
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")


def teams_to_csv():
    # Select all records from view in SQL database
    query = "SELECT * FROM teams;"

    # Output CSV file path
    csv_file_path = "teams_blob.csv"

    try:
        # Create SQLAlchemy engine
        print("Creating SQLAlchemy engine...")
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )

        # Save data to Pandas dataframe
        print(f"Executing query: {query}")
        df = pd.read_sql_query(query, con=engine)

        # Save to CSV
        print(f"Saving data to {csv_file_path}")
        df.to_csv(csv_file_path, index=False)

        print("Data exported successfully!")
    except Exception as e:
        print("Error:", e)


def offense_to_csv():
    # Select all records from view in SQL database
    query = f"SELECT * FROM offense_plays;"

    # Output CSV file path
    csv_file_path = "offense_plays_blob.csv"

    try:
        # Create the SQLAlchemy engine
        print("Creating SQLAlchemy engine...")
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )

        # Save data to Pandas dataframe
        print(f"Executing query: {query}")
        df = pd.read_sql_query(query, con=engine)

        # Save to CSV
        print(f"Saving data to {csv_file_path}")
        df.to_csv(csv_file_path, index=False)

        print("Data exported successfully!")
    except Exception as e:
        print("Error:", e)


def defense_to_csv():
    # SQL Query
    query = f"SELECT * FROM defense_plays;"

    # Output CSV file path
    csv_file_path = "defense_plays_blob.csv"

    try:
        # Create the SQLAlchemy engine
        print("Creating SQLAlchemy engine...")
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )

        # Fetch data into a Pandas DataFrame
        print(f"Executing query: {query}")
        df = pd.read_sql_query(query, con=engine)

        # Save to CSV
        print(f"Saving data to {csv_file_path}")
        df.to_csv(csv_file_path, index=False)

        print("Data exported successfully!")
    except Exception as e:
        print("Error:", e)


def teams_to_blob():
    local_csv_path = "teams_blob.csv"
    blob_name = "teams_blob.csv"

    try:
        # Connect to the Blob service
        blob_service_client = BlobServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/",
            credential=STORAGE_ACCOUNT_KEY,
        )

        # Get the Blob client
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)

        # Upload the CSV file
        with open(local_csv_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        print(f"File '{blob_client}' uploaded successfully to container '{CONTAINER_NAME}'.")

    except Exception as e:
        print("Error occurred:", e)


def offense_to_blob():
    local_csv_path = "offense_plays_blob.csv"
    blob_name = "offense_plays_blob.csv"

    try:
        # Connect to the Blob service
        blob_service_client = BlobServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/",
            credential=STORAGE_ACCOUNT_KEY,
        )

        # Get the Blob client
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)

        # Upload the CSV file
        with open(local_csv_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        print(f"File '{blob_client}' uploaded successfully to container '{CONTAINER_NAME}'.")

    except Exception as e:
        print("Error occurred:", e)


def defense_to_blob():
    local_csv_path = "defense_plays_blob.csv"
    blob_name = "defense_plays_blob.csv"

    try:
        # Connect to the Blob service
        blob_service_client = BlobServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/",
            credential=STORAGE_ACCOUNT_KEY,
        )

        # Get the Blob client
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)

        # Upload the CSV file
        with open(local_csv_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        print(f"File '{blob_client}' uploaded successfully to container '{CONTAINER_NAME}'.")

    except Exception as e:
        print("Error occurred:", e)


def main():
    # Run CSV functions
    teams_to_csv()
    offense_to_csv()
    defense_to_csv()

    # Run Blob functions
    teams_to_blob()
    offense_to_blob()
    defense_to_blob()


# Execute main function
if __name__ == "__main__":
    main()

