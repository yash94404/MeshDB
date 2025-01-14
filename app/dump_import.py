import os
import subprocess
import psycopg2
from neo4j import GraphDatabase
from typing import Dict
from dotenv import load_dotenv
import json
from pymongo import MongoClient

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


def restore_mongodb_dump(dump_folder: str, mongo_uri: str, database_name: str):
    """
    Restore a MongoDB dump from a folder.
    """
    print(f"Restoring MongoDB dump from {dump_folder}...")

    client = MongoClient(mongo_uri)
    db = client[database_name]
    db.drop_collection("movies")
    
    restore_cmd = [
        "mongorestore",
        "--uri", mongo_uri,
        "--db", database_name,
        dump_folder
    ]
    subprocess.run(restore_cmd, check=True)
    print("MongoDB restoration completed.")

def infer_mongodb_schema(mongo_uri: str, database_name: str):
    """
    Infer the schema of collections in the MongoDB database.
    """
    print("Inferring MongoDB schema...")
    schema = {}
    client = MongoClient(mongo_uri)
    db = client[database_name]

    for collection_name in db.list_collection_names():
        collection = db[collection_name]
        sample_doc = collection.find_one()
        if sample_doc:
            schema[collection_name] = {key: type(value).__name__ for key, value in sample_doc.items()}
        else:
            schema[collection_name] = "Empty Collection"

    client.close()
    print("MongoDB schema inference completed.")
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
    mongo_config = {
        "uri": os.getenv("MONGO_URI"),
        "database": os.getenv("MONGO_DATABASE")
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

    try:
        # Restore PostgreSQL
        pg_dump_file = "dumps/movies_db.dump"  # Replace with actual dump file path
        restore_postgres_dump(pg_dump_file, pg_config)

        # Restore Neo4j
        neo4j_dump_file = "dumps"  # Replace with actual dump file path
        restore_neo4j_dump(neo4j_dump_file, neo4j_config)

        # Restore MongoDB
        mongo_dump_folder = "dumps"  # Replace with actual MongoDB dump folder path
        restore_mongodb_dump(mongo_dump_folder, mongo_config["uri"], mongo_config["database"])

        # Infer schemas
        postgres_schema = infer_postgres_schema(pg_conn)
        neo4j_schema = infer_neo4j_schema(neo4j_driver)
        mongo_schema = infer_mongodb_schema(mongo_config["uri"], mongo_config["database"])

        # Combine schemas
        combined_schema = {
            "postgres": postgres_schema,
            "neo4j": neo4j_schema,
            "mongodb": mongo_schema
        }

        # Output schemas
        print("\nPostgreSQL Schema:")
        for table, columns in postgres_schema.items():
            print(f"Table: {table}")
            for column, data_type in columns:
                print(f"  {column}: {data_type}")

        print("\nNeo4j Schema:")
        print("Nodes:")
        for label, properties in neo4j_schema["nodes"].items():
            print(f"Label: {label}")
            for property_name in properties:
                print(f"  {property_name}")
        print("Relationships:")
        for rel_type, properties in neo4j_schema["relationships"].items():
            print(f"Type: {rel_type}")
            for property_name in properties:
                print(f"  {property_name}")

        print("\nMongoDB Schema:")
        for collection, fields in mongo_schema.items():
            print(f"Collection: {collection}")
            for field, data_type in fields.items():
                print(f"  {field}: {data_type}")

        # Save combined schemas to JSON file
        save_schema_to_file(combined_schema, "schemas.json")

    except Exception as e:
        print(f"Error during processing: {e}")

    finally:
        # Close connections
        pg_conn.close()
        neo4j_driver.close()

if __name__ == "__main__":
    main()

