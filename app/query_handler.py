# Natural Language Multi-Database Query Agent (NLMDQA)
# Required packages:
# pip install openai python-dotenv pymongo psycopg2-binary neo4j redis

import os
from typing import Dict, List, Any, Union
from openai import AsyncOpenAI
from dotenv import load_dotenv
import json
import psycopg2
from pymongo import MongoClient
from neo4j import GraphDatabase
import redis
from concurrent.futures import ThreadPoolExecutor
import logging
import decimal
from json import JSONEncoder
from time import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CustomJSONEncoder(JSONEncoder):
    """Custom JSON encoder to handle Decimal types"""
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super().default(obj)

class DatabaseConfig:
    """Configuration class for database connections"""
    def __init__(self):
        load_dotenv()
        
        # PostgreSQL configuration
        self.pg_config = {
            'dbname': os.getenv('PG_DATABASE'),
            'user': os.getenv('PG_USER'),
            'password': os.getenv('PG_PASSWORD'),
            'host': os.getenv('PG_HOST'),
            'port': os.getenv('PG_PORT')
        }
        
        # MongoDB configuration
        self.mongo_config = {
            'uri': os.getenv('MONGO_URI'),
            'database': os.getenv('MONGO_DATABASE')
        }
        
        # Neo4j configuration
        self.neo4j_config = {
            'uri': os.getenv('NEO4J_URI'),
            'user': os.getenv('NEO4J_USER'),
            'password': os.getenv('NEO4J_PASSWORD')
        }
        
        # Redis configuration
        self.redis_config = {
            'host': os.getenv('REDIS_HOST'),
            'port': os.getenv('REDIS_PORT'),
            'password': os.getenv('REDIS_PASSWORD')
        }
        
        # OpenAI configuration
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
class QueryParser:
    """Handles natural language parsing using GPT-4 to generate database queries directly"""
    def __init__(self, config: DatabaseConfig, schema):
        self.client = AsyncOpenAI(api_key=config.openai_api_key)
        self.postgres_schema = schema["postgres"]
        self.neo4j_schema = schema["neo4j"]
        
    async def parse_query(self, natural_language_query: str, error_feedback: str = "") -> Dict:
        """Generate database-specific queries directly from natural language"""
        try:
            error_context = f"""
            PREVIOUS ERROR FEEDBACK:
            {error_feedback}
            
            Please fix any issues mentioned above in the generated query.
            """ if error_feedback else ""

            prompt = f"""
            {error_context}

            Convert the following natural language query into a pipeline of database queries.
            You can create multi-stage queries where results from one query feed into another.
            
            Natural Language Query: {natural_language_query}
            
            GENERAL RULES FOR QUERY GENERATION:
            1. Only use multiple stages when you need to:
            - Query across different databases
            - Use results from one database to filter in another
            - Join data that exists in different databases
            2. Never use a stage just for ordering or aggregating previous results
            3. Each stage should either:
            - Fetch new data based on previous stage results
            - Join data from a different database
            - Transform data in a way that can't be done in a single query
            4. When passing IDs between stages, always include them in output_keys

            POSTGRESQL RULES:
            1. Use proper SQL JOINs and CTEs (Common Table Expressions) whenever possible
            2. For aggregations and statistics, use SQL GROUP BY, HAVING, and window functions
            3. Include all necessary columns in output_keys, especially IDs needed for joins
            4. Always qualify column names with table aliases (e.g., m.id, g.name)
            5. Use meaningful table aliases (e.g., m for movies, g for genres)
            6. Include ORDER BY in the same query when doing aggregations
            
            NEO4J RULES:
            1. Always use different variable names for relationships and nodes
            2. Never reuse the same variable name in a pattern
            3. Include ORDER BY in the same MATCH clause, never in a separate stage
            4. Never use WITH clauses with previous stage placeholders - instead use MATCH WHERE conditions
            5. For multiple values from previous stages, use:
            Good: WHERE x.id IN [{{previous_stage1.ids}}]
            Bad: WITH [{{previous_stage1.ids}}] as ids ... // Never do this!
            
            NEO4J QUERY PATTERNS:
            1. For counting/aggregation:
            MATCH (node1:Label1)-[r1:REL]->(node2:Label2)
            WHERE node1.property = value
            RETURN node2.property as prop, COUNT(*) as count
            ORDER BY count DESC

            2. For finding related entities:
            MATCH (node1:Label1)-[r1:REL]->(node2:Label2)-[r2:REL]->(node3:Label3)
            WHERE node1.id IN [{{previous_stage1.ids}}]
            RETURN DISTINCT node3.id as id
            
            DATABASE SCHEMA:
            PostgreSQL Tables:
            {self.postgres_schema}
            
            Neo4j Structure:
            {self.neo4j_schema}

            EXAMPLE QUERIES:

            1. "Find all action movies with ratings above 8.0"
            {{
                "pipeline": [
                    {{
                        "stage": 1,
                        "database": "postgresql",
                        "query": {{
                            "postgresql": "
                                SELECT DISTINCT m.id, m.title, m.imdb_rating, m.release_year 
                                FROM movies m
                                JOIN movie_genres mg ON m.id = mg.movie_id
                                JOIN genres g ON g.id = mg.genre_id
                                WHERE g.name = 'Action'
                                AND m.imdb_rating > 8.0
                                ORDER BY m.imdb_rating DESC"
                        }},
                        "output_keys": ["id", "title", "imdb_rating", "release_year"],
                        "description": "Get high-rated action movies"
                    }}
                ]
            }}

            2. "Find directors who have worked with both Tom Hanks and Leonardo DiCaprio"
            {{
                "pipeline": [
                    {{
                        "stage": 1,
                        "database": "neo4j",
                        "query": {{
                            "neo4j": "
                                MATCH (actor1:Person {{name: 'Tom Hanks'}})-[r1:ACTED_IN]->(m1:Movie)<-[d1:DIRECTED]-(director:Person),
                                    (actor2:Person {{name: 'Leonardo DiCaprio'}})-[r2:ACTED_IN]->(m2:Movie)<-[d2:DIRECTED]-(director)
                                RETURN DISTINCT director.name as director_name"
                        }},
                        "output_keys": ["director_name"],
                        "description": "Find directors who worked with both actors"
                    }}
                ]
            }}

            3. "Find high-grossing movies directed by Christopher Nolan"
            {{
                "pipeline": [
                    {{
                        "stage": 1,
                        "database": "neo4j",
                        "query": {{
                            "neo4j": "
                                MATCH (p:Person {{name: 'Christopher Nolan'}})-[r:DIRECTED]->(m:Movie)
                                RETURN m.id as id"
                        }},
                        "output_keys": ["id"],
                        "description": "Get movies directed by Christopher Nolan"
                    }},
                    {{
                        "stage": 2,
                        "database": "postgresql",
                        "query": {{
                            "postgresql": "
                                SELECT m.id, m.title, m.release_year, m.gross
                                FROM movies m
                                WHERE m.id IN {{previous_stage1.id}}
                                AND m.gross > 100000000
                                ORDER BY m.gross DESC"
                        }},
                        "output_keys": ["id", "title", "release_year", "gross"],
                        "description": "Get high-grossing movies from the director"
                    }}
                ]
            }}

            Return ONLY a JSON object containing a pipeline array of stages. Each stage must have:
            - stage: number indicating order
            - database: which database to query ("postgresql" or "neo4j")
            - query: object with database-specific query
            - output_keys: array of column names to pass to next stage
            - description: string explaining what the stage does
            """
            
            response = await self.client.chat.completions.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=2000
            )
            
            content = response.choices[0].message.content.strip()

            # Clean up the content before parsing JSON
            content = content.replace('\n', ' ')  # Replace newlines with spaces
            content = ' '.join(content.split())   # Normalize whitespace
            content = content.replace('\t', ' ')  # Replace tabs
            content = content.replace('": "', '": "')  # Ensure consistent quote formatting
        
            try:
                return json.loads(content)
            except json.JSONDecodeError as json_err:
                logger.error(f"Failed to parse JSON response: {content}")
                logger.error(f"JSON parse error: {str(json_err)}")
                raise RuntimeError("Generated query was not valid JSON") from json_err
        except Exception as e:
            logger.error(f"Error during query parsing: {str(e)}")
            raise RuntimeError(f"Failed to parse natural language query: {str(e)}") from e

class DatabaseConnector:
    """Manages connections to different databases"""
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.redis_client = redis.Redis(
            host=config.redis_config['host'],
            port=config.redis_config['port'],
            password=config.redis_config['password']
        )
    
    def get_postgres_connection(self):
        """Get PostgreSQL connection"""
        return psycopg2.connect(**self.config.pg_config)
    
    def get_mongo_connection(self):
        """Get MongoDB connection"""
        client = MongoClient(self.config.mongo_config['uri'])
        return client[self.config.mongo_config['database']]
    
    def get_neo4j_connection(self):
        """Get Neo4j connection"""
        return GraphDatabase.driver(
            self.config.neo4j_config['uri'],
            auth=(self.config.neo4j_config['user'], 
                  self.config.neo4j_config['password'])
        )

class QueryExecutor:
    """Executes queries across different databases"""
    def __init__(self, connector: DatabaseConnector):
        self.connector = connector
    
    # def execute_postgres_query(self, query: str) -> List[Dict]:
    #     """Execute PostgreSQL query"""
    #     with self.connector.get_postgres_connection() as conn:
    #         with conn.cursor() as cur:
    #             cur.execute(query)
    #             columns = [desc[0] for desc in cur.description]
    #             results = []
    #             for row in cur.fetchall():
    #                 results.append(dict(zip(columns, row)))
    #     return results
    def execute_postgres_query(self, query: str) -> List[Dict]:
        """Execute PostgreSQL query"""
        if "IN ()" in query or "IN ( )" in query:
            return []
        
        with self.connector.get_postgres_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                columns = [desc[0] for desc in cur.description]
                results = []
                for row in cur.fetchall():
                    # Convert Decimal objects to float
                    processed_row = []
                    for value in row:
                        if isinstance(value, decimal.Decimal):
                            processed_row.append(float(value))
                        else:
                            processed_row.append(value)
                    results.append(dict(zip(columns, processed_row)))
        return results
    
    def execute_mongo_query(self, query: Dict) -> List[Dict]:
        """Execute MongoDB query"""
        db = self.connector.get_mongo_connection()
        collection = db[query['collection']]
        return list(collection.find(query['filter']))
    
    def execute_neo4j_query(self, query: str) -> List[Dict]:
        """Execute Neo4j query"""
        with self.connector.get_neo4j_connection() as driver:
            with driver.session(database="neo4j") as session:
                result = session.run(query)
                return [record.data() for record in result]

class DataMerger:
    """Merges results from different databases"""
    def merge_results(self, results: Dict[str, List[Dict]], merge_keys: List[str]) -> List[Dict]:
        """Merge results based on common keys"""
        if not results:
            return []
        
        # Validate that all merge keys are present in the results
        for stage_name, stage_results in results.items():
            if stage_results:  # Check if there are any results
                missing_keys = [key for key in merge_keys if key not in stage_results[0]]
                if missing_keys:
                    logger.warning(f"Stage {stage_name} is missing merge keys: {missing_keys}")
                    return []  # Return empty list if merge keys are missing
        
        # Start with the first database's results
        merged_data = results[list(results.keys())[0]]
        
        # Merge with other databases
        for db_name in list(results.keys())[1:]:
            merged_data = self._merge_two_datasets(
                merged_data,
                results[db_name],
                merge_keys
            )
        
        return merged_data
    
    def _merge_two_datasets(self, data1: List[Dict], data2: List[Dict], 
                           merge_keys: List[str]) -> List[Dict]:
        """Merge two datasets based on common keys"""
        merged = []
        
        # Create dictionaries for faster lookup
        data2_dict = {
            tuple(str(item.get(key)) for key in merge_keys): item 
            for item in data2
        }
        
        for item1 in data1:
            key_tuple = tuple(str(item1.get(key)) for key in merge_keys)
            if key_tuple in data2_dict:
                merged_item = {**item1, **data2_dict[key_tuple]}
                merged.append(merged_item)
        
        return merged

class CacheManager:
    """Manages query caching using an in-memory dictionary"""
    def __init__(self):
        self.cache = {}
        self.cache_ttl = 3600  # 1 hour
        self.timestamps = {}  # Store timestamps for TTL checking
        self.time = time
    
    def get_cached_result(self, query: str) -> Union[List[Dict], None]:
        """Get cached query result"""
        cache_key = self._get_cache_key(query)
        if cache_key in self.cache:
            # Check if cache has expired
            if self.time() - self.timestamps[cache_key] > self.cache_ttl:
                del self.cache[cache_key]
                del self.timestamps[cache_key]
                return None
            return self.cache[cache_key]
        return None
    
    def cache_result(self, query: str, result: List[Dict]):
        """Cache query result"""
        cache_key = self._get_cache_key(query)
        self.cache[cache_key] = result
        self.timestamps[cache_key] = self.time()
    
    def _get_cache_key(self, query: str) -> str:
        """Generate cache key for query"""
        return f"nlmdqa:query:{hash(query)}"

class NLMDQA:
    """Main class for Natural Language Multi-Database Query Agent"""
    def __init__(self):
        self.config = DatabaseConfig()
        self.schema = NLMDQA.load_schema_from_file("schemas.json")
        self.parser = QueryParser(self.config, self.schema)
        self.connector = DatabaseConnector(self.config)
        self.executor = QueryExecutor(self.connector)
        self.merger = DataMerger()
        self.cache_manager = CacheManager()
    
    def load_schema_from_file(filename):
        """
        Load the schema dictionary from a JSON file.
        """
        with open(filename, 'r') as f:
            schema = json.load(f)
        print(f"Schema loaded from {filename}")
        return schema
    
    async def generate_human_response(self, query: str, results: List[Dict]) -> str:
        """Generate a human-readable response using GPT"""
        try:
            response = await self.parser.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that explains database query results in natural language."},
                    {"role": "user", "content": f"""
                        Original question: {query}
                        
                        Query results: {json.dumps(results, cls=CustomJSONEncoder)}
                        
                        Please provide a clear, concise summary of these results in natural language. 
                        Format the response in a reader-friendly way."""}
                ],
                temperature=0.7,
                max_tokens=500
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"Error generating human response: {str(e)}")
            return "Sorry, I couldn't generate a human-readable response for these results."
    
    def format_value_for_query(self, value, database_type):
        """Format values appropriately for different database types"""
        if isinstance(value, list):
            if database_type == "neo4j":
                # For Neo4j, wrap list in square brackets
                formatted_values = []
                for v in value:
                    if isinstance(v, (int, float, decimal.Decimal)):
                        formatted_values.append(str(v))
                    else:
                        formatted_values.append(f"'{v}'")
                
                # print(f"Formatted values: {formatted_values}")
                return f"[{', '.join(formatted_values)}]"
            else:  # postgresql or others
                # For SQL, just join with commas
                formatted_values = []
                for v in value:
                    if isinstance(v, (int, float, decimal.Decimal)):
                        formatted_values.append(str(v))
                    else:
                        formatted_values.append(f"'{v}'")
                return ', '.join(formatted_values)
        else:
            if isinstance(value, (int, float, decimal.Decimal)):
                return str(value)
            return f"'{value}'"
    
    async def process_query(self, natural_language_query: str, human_readable: bool = False, retry_count: int = 0, error_feedback: str = "") -> Union[Dict, str]:
        """Process natural language query and return results"""
        MAX_RETRIES = 3

        try:
            # Check cache first
            cached_result = self.cache_manager.get_cached_result(natural_language_query)
            if cached_result:
                logger.info("Returning cached result")
                return cached_result

            # Get queries directly from GPT-4
            pipeline_result = await self.parser.parse_query(natural_language_query, error_feedback)
            print("Generated pipeline: \n\n", json.dumps(pipeline_result, indent=2))

            # Execute pipeline stages
            stage_results = {}
            final_results = {}
            test_final_results = None
            
            for stage in pipeline_result['pipeline']:
                stage_num = stage['stage']
                database = stage['database']
                query = stage['query'][database]

                print(f"Stage {stage_num} original query: {query}")
                
                # Replace placeholders with previous stage results
                if isinstance(query, str):
                    for prev_stage, prev_results in stage_results.items():
                        for key, value in prev_results.items():
                            formatted_value = self.format_value_for_query(value, database)
                            placeholder = f"{{previous_stage{prev_stage}.{key}}}"
                            query = query.replace(placeholder, formatted_value)
                elif isinstance(query, dict):  # MongoDB query
                    query_str = json.dumps(query)
                    for prev_stage, prev_results in stage_results.items():
                        for key, value in prev_results.items():
                            formatted_value = self.format_value_for_query(value, database)
                            placeholder = f"{{previous_stage{prev_stage}.{key}}}"
                            query = query.replace(placeholder, formatted_value)
                    query = json.loads(query_str)

                print(f"Stage {stage_num} formatted query: {query}")
                
                # Execute query based on database type
                if database == 'postgresql':
                    results = self.executor.execute_postgres_query(query)
                elif database == 'mongodb':
                    results = self.executor.execute_mongo_query(query)
                elif database == 'neo4j':
                    results = self.executor.execute_neo4j_query(query)
                
                # Store results for this stage
                stage_results[stage_num] = {
                    key: [r[key] for r in results] for key in stage['output_keys']
                }
                final_results[f"stage_{stage_num}"] = results
                test_final_results = results
            
            # Cache results
            self.cache_manager.cache_result(natural_language_query, test_final_results)

            if human_readable:
                return await self.generate_human_response(natural_language_query, test_final_results)
            return test_final_results
        except Exception as e:
            error_message = str(e)
            logger.error(f"Error processing query (attempt {retry_count + 1}): {error_message}")
            
            if retry_count < MAX_RETRIES:
                # Modify the parser's prompt to include the error feedback
                error_feedback = f"""
                Previous attempt failed with error: {error_message}
                Please fix the query and try again. Common issues to check:
                - Ensure JSON formatting is correct
                - Verify database names are correct ('postgresql', 'neo4j', 'mongodb')
                - Check that all referenced columns exist
                - Verify syntax for the specific database being queried
                """
                
                # Retry with incremented counter
                logger.info(f"Retrying query (attempt {retry_count + 2})")
                return await self.process_query(natural_language_query, human_readable, retry_count + 1, error_feedback)
            else:
                raise RuntimeError(f"Failed to process query after {MAX_RETRIES} attempts. Last error: {error_message}")

# Example usage
async def main():
    import argparse

    # Set up argument parser
    parser = argparse.ArgumentParser(description='Natural Language Multi-Database Query Agent')
    parser.add_argument('--human-readable', '-hr', action='store_true', 
                       help='Generate a human-readable response instead of raw data')
    args = parser.parse_args()

    # Initialize the NLMDQA system
    nlmdqa = NLMDQA()
    
    # Demo queries
    # query = "Show me all movies with an IMDB rating above 8.5" # Postgres
    # query = "Find all movies where Tom Hanks and Leonardo DiCaprio acted together" # Neo4j
    query = "Find all war movies directed by Steven Spielberg that grossed over $200 million" # Multi-Database
    
    try:
        start_time = time()
        results = await nlmdqa.process_query(query, human_readable=args.human_readable)

        # Calculate elapsed time
        elapsed_time = time() - start_time

        if args.human_readable:
            print("\nHuman-readable response:")
            print(results)
        else:
            print("\nRaw query results:")
            print(json.dumps(results, indent=2, cls=CustomJSONEncoder))

        print(f"\nQuery processing time: {elapsed_time:.2f} seconds")
    except Exception as e:
        logger.error(f"Error processing query: {str(e)}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

"""
1. Simple Queries (Single Database)
"Show me all movies with an IMDB rating above 8.5"
"What are the top 10 highest-grossing movies?"
"List all movies released in 2019"

2. Genre-Based Queries (PostgreSQL + Relationships)
"Find all action movies with an IMDB rating above 8.0"
"What are the most common genres among movies that grossed over $100 million?"
"Show me all drama movies from the last 5 years with high meta scores"

3. Person-Related Queries (Neo4j Relationships)
"Which actors have worked with Christopher Nolan?"
"Find all movies where Tom Hanks and Leonardo DiCaprio acted together"
"Who are the directors that have made the most movies with Robert De Niro?"

4. Complex Multi-Database Queries
"Find all war movies directed by Steven Spielberg that grossed over $200 million"
"Show me all movies where Morgan Freeman acted that have an IMDB rating above 8.0 and are in the drama genre"
"List the top 5 directors who have made the highest-grossing sci-fi movies in the last decade"

5. Analytics-Focused Queries
"What's the average IMDB rating for movies directed by Martin Scorsese?"
"Compare the average gross earnings of action movies vs. drama movies"
"Who are the actors that appear most frequently in movies with meta scores above 80?"

6. Time-Based Analysis
"Show the trend of superhero movie ratings over the last 20 years"
"Which directors have consistently made high-grossing movies in each decade?"
"Find movies from the 1990s that have both high IMDB ratings and high meta scores"

7. Complex Relationship Chains
"Find actors who have both directed and acted in movies with ratings above 8.0"
"Show me directors who have worked with the same actor in more than 3 movies"
"List movies where the director has also acted in another director's film"
"""