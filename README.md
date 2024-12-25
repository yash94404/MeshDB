How to run the code:

1. Install the requirements:

```
pip install -r requirements.txt
```

2. Setup postgres and neo4j databases, and add data to databases

Add keys in .env file for Postgres and Neo4j connection, as well as the openAI key.

Here were the names/values I used:

PG_DATABASE=movies_db

PG_USER=movies_user

PG_PASSWORD=movies123

PG_HOST=localhost

PG_PORT=5432

NEO4J_URI=bolt://localhost:7687

NEO4J_USER=neo4j

NEO4J_PASSWORD=movies123

Make sure both the postgres and neo4j instances are running and configured properly, and run the following code to create the tables and add the data to the tables from movies.csv:

```
python data_import.py
```

3. Substitute the query in the `main()` function with the query you want to try

4. Run the code:

```
python main.py
```

5. Run the code with human-readable output:

```
python main.py -hr
```
