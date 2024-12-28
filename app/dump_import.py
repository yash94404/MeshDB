import psycopg2
import subprocess
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = int(os.getenv("PG_PORT"))

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

PG_DUMP_FILE = "dumps/movies_db.dump"  # Replace with the actual path
NEO4J_DUMP_PATH = "dumps"  # Replace with the actual path
NEO4J_DATABASE_NAME = "neo4j"
'''
def create_postgres_database(db_name):
    conn = psycopg2.connect(
        dbname="postgres", user=PG_USER, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"CREATE DATABASE {db_name};")
    conn.close()
    print(f"PostgreSQL database '{db_name}' created.")
'''

def create_postgres_database(db_name):
    """
    Drops the PostgreSQL database if it exists, then recreates it.
    """
    conn = psycopg2.connect(
        dbname="postgres", user=PG_USER, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        # Drop the database if it exists
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}';")
        if cur.fetchone():
            cur.execute(f"DROP DATABASE {db_name};")
            print(f"Database '{db_name}' dropped.")
        # Create the database
        cur.execute(f"CREATE DATABASE {db_name};")
        print(f"Database '{db_name}' created.")
    conn.close()


def restore_postgres_dump(db_name, dump_file):
    restore_cmd = [
        "pg_restore",
        "-U", PG_USER,
        "-h", PG_HOST,
        "-p", str(PG_PORT),
        "-d", db_name,
        dump_file
    ]
    subprocess.run(restore_cmd, check=True)
    print(f"PostgreSQL dump restored to '{db_name}'.")

def create_neo4j_database(database_name):
    create_cmd = [
        "neo4j-admin",
        "database",
        "create",
        database_name
    ]
    subprocess.run(create_cmd, check=True)
    print(f"Neo4j database '{database_name}' created.")

NEO4J_ADMIN_PATH = "neo4j-admin"  # Ensure this is in your PATH or provide full path

def stop_neo4j():
    """
    Stops the Neo4j server.
    """
    try:
        print("Stopping Neo4j...")
        subprocess.run(["sudo", "systemctl", "stop", "neo4j"], check=True)
        print("Neo4j stopped.")
    except subprocess.CalledProcessError:
        print("Failed to stop Neo4j. Ensure the service is running and you have the necessary permissions.")
        raise

def start_neo4j():
    """
    Starts the Neo4j server.
    """
    try:
        print("Starting Neo4j...")
        subprocess.run(["sudo", "systemctl", "start", "neo4j"], check=True)
        print("Neo4j started.")
    except subprocess.CalledProcessError:
        print("Failed to start Neo4j. Ensure the service is installed and you have the necessary permissions.")
        raise

def restore_neo4j_dump(dump_path, database_name):
    """
    Restores a Neo4j dump into the specified database.
    """
    try:
        print(f"Restoring Neo4j dump from '{dump_path}' into '{database_name}'...")
        restore_cmd = [
            NEO4J_ADMIN_PATH,
            "database",
            "load",
            database_name,
            "--from-path", dump_path,
            "--overwrite-destination=true"
        ]
        subprocess.run(restore_cmd, check=True)
        print("Neo4j dump restored successfully.")
    except subprocess.CalledProcessError:
        print("Failed to restore Neo4j dump.")
        raise
    
def main():
    pg_db_name = PG_DATABASE
    neo4j_db_name = "neo4j_movies_instance"

    try:
        # Step 1: Create PostgreSQL Database
        create_postgres_database(pg_db_name)

        # Step 2: Restore PostgreSQL Dump
        restore_postgres_dump(pg_db_name, PG_DUMP_FILE)

        # Step 3: Create Neo4j Database
        #create_neo4j_database(neo4j_db_name)

        # Step 4: Restore Neo4j Dump
        stop_neo4j()

        # Step 2: Restore the Neo4j dump
        restore_neo4j_dump(NEO4J_DUMP_PATH, NEO4J_DATABASE_NAME)

        # Step 3: Start Neo4j server
        start_neo4j()

        print("Pipeline completed successfully!")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
