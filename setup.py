import os
import duckdb
import sqlite3
import argparse


def clean_up(file):
    # Remove the file if it exists
    if os.path.exists(file):
        os.remove(file)
    else:
        print(f"The file {file} does not exist.")


def create_tpch_data(db_file_name, data_size=0.01):
    con = duckdb.connect(
        db_file_name
    )  # Define a .db file to persist the generated tpch data
    con.sql(
        f"INSTALL tpch;LOAD tpch;CALL dbgen(sf = {data_size});"
    )  # generate a 100MB TPCH dataset
    con.commit()
    con.close()  # close connection, since duckdb only allows one connection to tpch.db

def create_sqlite_database(sqlite_db_file):
    conn = sqlite3.connect(sqlite_db_file)
    cursor = conn.cursor()
    
    # Create a table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER
        )
    ''')
    
    # Insert data (push)
    cursor.execute('INSERT INTO users (name, age) VALUES (?, ?)', ('Alice', 30))
    conn.commit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script to create TPCH input data and clean up the existing DB file.")
    parser.add_argument('--db_file', type=str, default="tpch.db", help="The TPCH database file to use. Defaults to 'tpch.db'.")
    parser.add_argument('--sqlite_db_file', type=str, default="example.db", help="The example sqlite3 database file to use. Defaults to 'example.db'.")

    # Parse the command line arguments
    args = parser.parse_args()
    # Get the database file from the parsed arguments
    db_file = args.db_file
    sqlite_db_file = args.sqlite_db_file
    
    print(f"Cleaning up (if any existing) tpch db file {db_file}")
    clean_up(db_file)
    print(f"Creating TPCH input data at {db_file}")
    create_tpch_data(db_file)

    print(f"Cleaning up (if any existing) sqlite3 db file {sqlite_db_file}")
    clean_up(sqlite_db_file)
    print(f"Creating sqlite database file at {sqlite_db_file}")
    create_sqlite_database(sqlite_db_file)