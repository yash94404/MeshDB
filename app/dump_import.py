import os
import subprocess
import psycopg2
from neo4j import GraphDatabase
from typing import Dict
from dotenv import load_dotenv
import json


def create_postgres_database(db_name, user, password, host, port):
    conn = psycopg2.connect(
        dbname="postgres",  # Connect to the default database
        user=user,
        password=password,
        host=host,
        port=port
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"CREATE DATABASE {db_name};")
    conn.close()
    print(f"PostgreSQL database '{db_name}' created.")


def create_neo4j_database(database_name):
    subprocess.run(
        ["neo4j-admin", "database", "create", database_name],
        check=True
    )
    print(f"Neo4j database '{database_name}' created.")

def restore_postgres_dump(dump_file: str, db_config: Dict):
    """
    Restore a PostgreSQL dump file.
    """
    print(f"Restoring PostgreSQL dump from {dump_file}...")
    restore_cmd = [
        "pg_restore",
        "--clean",
        "--if-exists",
        "--no-owner",
        "--dbname", f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}",
        dump_file
    ]
    subprocess.run(restore_cmd, check=True)
    print("PostgreSQL restoration completed.")

def infer_postgres_schema(conn):
    """
    Infer schema from the restored PostgreSQL database.
    """
    print("Inferring PostgreSQL schema...")
    schema = {}
    with conn.cursor() as cur:
        # Get tables
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = cur.fetchall()
        for table in tables:
            table_name = table[0]
            schema[table_name] = []
            # Get columns for each table
            cur.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
            """)
            schema[table_name] = cur.fetchall()
    print("PostgreSQL schema inference completed.")
    return schema

def restore_neo4j_dump(dump_file: str, neo4j_config: Dict):
    """
    Restores a Neo4j database from a dump file.
    """
    print(f"Restoring Neo4j database from {dump_file}...")
    restore_cmd = [
        "neo4j-admin",
        "database",
        "load",
        neo4j_config["database"],  # The database name, e.g., 'neo4j'
        "--from-path", dump_file,  # Path to the dump
        "--overwrite-destination=true"
    ]

    try:
        subprocess.run(restore_cmd, check=True)
        print("Neo4j database restored successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error restoring Neo4j database: {e}")


def infer_neo4j_schema(neo4j_driver):
    """
    Infers the schema of the Neo4j database without APOC.
    """
    print("Inferring Neo4j schema...")
    schema = {"nodes": {}, "relationships": {}}
    with neo4j_driver.session() as session:
        # Get node labels and properties
        node_result = session.run("""
            MATCH (n)
            RETURN DISTINCT labels(n) AS labels, keys(n) AS properties
        """)
        for record in node_result:
            labels = tuple(record["labels"])
            properties = record["properties"]
            schema["nodes"][labels] = properties
        
        # Get relationship types and properties
        rel_result = session.run("""
            MATCH ()-[r]->()
            RETURN DISTINCT type(r) AS relationshipType, keys(r) AS properties
        """)
        for record in rel_result:
            rel_type = record["relationshipType"]
            properties = record["properties"]
            schema["relationships"][rel_type] = properties

    print("Neo4j schema inference completed.")
    return schema



def infer_property_type(value):
    if isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "FLOAT"
    elif isinstance(value, str):
        return "STRING"
    elif isinstance(value, bool):
        return "BOOLEAN"
    elif value is None:
        return "NULL"
    else:
        return "UNKNOWN"


def convert_keys_to_str(data):
    """
    Recursively convert dictionary keys to strings for JSON compatibility.
    """
    if isinstance(data, dict):
        return {str(key): convert_keys_to_str(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_keys_to_str(item) for item in data]
    else:
        return data
    
def save_schema_to_file(schema, filename):
    """
    Save the schema dictionary to a JSON file.
    """
    schema_str_keys = convert_keys_to_str(schema)
    with open(filename, 'w') as f:
        json.dump(schema_str_keys, f, indent=4)
    print(f"Schema saved to {filename}")


def main():
    load_dotenv()

    # Configuration
    pg_config = {
        "database": os.getenv("PG_DATABASE"),
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "host": os.getenv("PG_HOST"),
        "port": os.getenv("PG_PORT"),
    }
    neo4j_config = {
        "uri": os.getenv("NEO4J_URI"),
        "user": os.getenv("NEO4J_USER"),
        "password": os.getenv("NEO4J_PASSWORD"),
        "database": "neo4j"  # Default Neo4j database
    }

    # Database connections
    pg_conn = psycopg2.connect(
        dbname=pg_config["database"],
        user=pg_config["user"],
        password=pg_config["password"],
        host=pg_config["host"],
        port=pg_config["port"]
    )

    neo4j_driver = GraphDatabase.driver(
        neo4j_config["uri"],
        auth=(neo4j_config["user"], neo4j_config["password"])
    )

    # Restore databases
    pg_dump_file = "dumps/movies_db.dump"  # Replace with actual dump file path
    neo4j_dump_file = "dumps"  # Replace with actual dump file path

    #restore_postgres_dump(pg_dump_file, pg_config)
    restore_neo4j_dump(neo4j_dump_file, neo4j_config)

    # Infer schemas
    postgres_schema = infer_postgres_schema(pg_conn)
    neo4j_schema = infer_neo4j_schema(neo4j_driver)

    # Output schemas
    print("PostgreSQL Schema:")
    for table, columns in postgres_schema.items():
        print(f"Table: {table}")
        for column, data_type in columns:
            print(f"  {column}: {data_type}")

    print("\nNeo4j Schema:")
    print("Nodes:")
    print(neo4j_schema["nodes"])
    for label, properties in neo4j_schema["nodes"].items():
        print(f"Label: {label}")
        for property_name in properties:
            print(f"  {property_name}")
    print("Relationships:")
    for rel_type, properties in neo4j_schema["relationships"].items():
        print(f"Type: {rel_type}")
        for property_name, data_type in properties:
            print(f"  {property_name}: {data_type}")
    
    combined_schema = {
        "postgres": postgres_schema,
        "neo4j": neo4j_schema
    }

    # Save schemas to a JSON file
    save_schema_to_file(combined_schema, "schemas.json")

    # Close connections
    pg_conn.close()
    neo4j_driver.close()

if __name__ == "__main__":
    main()
