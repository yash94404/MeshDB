from flask import Blueprint, render_template, request, jsonify
from werkzeug.utils import secure_filename
import os
from .query_handler import NLMDQA
from .dump_import import restore_postgres_dump, restore_neo4j_dump, restore_mongodb_dump, infer_postgres_schema, infer_neo4j_schema, infer_mongodb_schema, save_schema_to_file
import psycopg2
from neo4j import GraphDatabase

main = Blueprint('main', __name__)

# Initialize the NLMDQA system
nlmdqa = NLMDQA()

# Configuration
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

PG_CONFIG = {
    "database": os.getenv("PG_DATABASE"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
}

NEO4J_CONFIG = {
    "uri": os.getenv("NEO4J_URI"),
    "user": os.getenv("NEO4J_USER"),
    "password": os.getenv("NEO4J_PASSWORD"),
    "database": "neo4j",  # Default Neo4j database
}

@main.route('/')
def index():
    return render_template('index.html')  # Renders your HTML file

@main.route('/api/query', methods=['POST'])
async def handle_query():
    data = request.json
    query = data.get('query')

    if not query:
        return jsonify({"error": "Query cannot be empty"}), 400

    try:
        results = await nlmdqa.process_query(query, human_readable=True)
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@main.route('/upload-dumps', methods=['POST'])
def upload_dumps():
    if "pg_dump" not in request.files or "neo4j_dump" not in request.files or "mongo_dump" not in request.files:
        return jsonify({"error": "PostgreSQL, Neo4j, and MongoDB dump files are required."}), 400

    pg_file = request.files["pg_dump"]
    neo4j_file = request.files["neo4j_dump"]
    mongo_file = request.files["mongo_dump"]

    pg_filepath = os.path.join(UPLOAD_FOLDER, secure_filename(pg_file.filename))
    neo4j_filepath = os.path.join(UPLOAD_FOLDER, secure_filename(neo4j_file.filename))
    mongo_filepath = os.path.join(UPLOAD_FOLDER, secure_filename(mongo_file.filename))

    pg_file.save(pg_filepath)
    neo4j_file.save(neo4j_filepath)
    mongo_file.save(mongo_filepath)

    try:
        # Restore PostgreSQL
        restore_postgres_dump(pg_filepath, PG_CONFIG)

        # Restore Neo4j
        restore_neo4j_dump(neo4j_filepath, NEO4J_CONFIG)

        # Restore MongoDB
        restore_mongodb_dump(mongo_filepath, os.getenv("MONGO_URI"), os.getenv("MONGO_DATABASE"))

        # Infer schemas
        pg_conn = psycopg2.connect(**PG_CONFIG)
        neo4j_driver = GraphDatabase.driver(**NEO4J_CONFIG)
        postgres_schema = infer_postgres_schema(pg_conn)
        neo4j_schema = infer_neo4j_schema(neo4j_driver)
        mongo_schema = infer_mongodb_schema(os.getenv("MONGO_URI"), os.getenv("MONGO_DATABASE"))

        combined_schema = {
            "postgres": postgres_schema,
            "neo4j": neo4j_schema,
            "mongodb": mongo_schema
        }
        save_schema_to_file(combined_schema, "schemas.json")

        # Close connections
        pg_conn.close()
        neo4j_driver.close()

        return jsonify({"message": "Dumps restored and schemas saved successfully."}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

