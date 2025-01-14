import pandas as pd
import psycopg2
from neo4j import GraphDatabase
from typing import List, Set, Dict
import os
from dotenv import load_dotenv

def create_postgres_tables(conn):
    """Create the PostgreSQL tables"""
    with conn.cursor() as cur:
        # Create movies table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                release_year INTEGER,
                certificate VARCHAR(50),
                runtime INTEGER,
                imdb_rating DECIMAL(3,1),
                meta_score INTEGER,
                overview TEXT,
                gross DECIMAL,
                no_of_votes INTEGER,
                poster_link TEXT
            )
        """)
        
        # Create genres table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS genres (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) UNIQUE
            )
        """)
        
        # Create movie_genres table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS movie_genres (
                movie_id INTEGER REFERENCES movies(id),
                genre_id INTEGER REFERENCES genres(id),
                PRIMARY KEY (movie_id, genre_id)
            )
        """)
        
        conn.commit()

def process_genres(genre_string: str) -> List[str]:
    """Convert genre string to list of genres"""
    if pd.isna(genre_string):
        return []
    return [g.strip() for g in genre_string.split(',')]

def insert_genres(conn, all_genres: Set[str]) -> Dict[str, int]:
    """Insert genres and return mapping of genre names to ids"""
    genre_map = {}
    with conn.cursor() as cur:
        for genre in all_genres:
            cur.execute(
                "INSERT INTO genres (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id",
                (genre,)
            )
            genre_map[genre] = cur.fetchone()[0]
    conn.commit()
    return genre_map

def clean_money_value(value: str) -> float:
    """Convert money string to float"""
    if pd.isna(value):
        return None
    return float(value.replace('$', '').replace(',', ''))

def clear_all_data(pg_conn, neo4j_driver):
    """Clear all data from PostgreSQL and Neo4j databases"""
    # Clear PostgreSQL tables
    with pg_conn.cursor() as cur:
        # Delete in correct order to respect foreign keys
        cur.execute("TRUNCATE movie_genres, genres, movies RESTART IDENTITY CASCADE")
        pg_conn.commit()

    # Clear Neo4j database
    with neo4j_driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")

def main():
    load_dotenv()

    # Database connections
    pg_conn = psycopg2.connect(
        dbname=os.getenv('PG_DATABASE'),
        user=os.getenv('PG_USER'),
        password=os.getenv('PG_PASSWORD'),
        host=os.getenv('PG_HOST'),
        port=os.getenv('PG_PORT')
    )

    neo4j_driver = GraphDatabase.driver(
        os.getenv('NEO4J_URI'),
        auth=(os.getenv('NEO4J_USER'), os.getenv('NEO4J_PASSWORD'))
    )

    # Clear existing data
    clear_all_data(pg_conn, neo4j_driver)

    # Read CSV file
    # df = pd.read_csv('movies.csv')  # Replace with your CSV file path
    # Read CSV with specific data types
    df = pd.read_csv('movies.csv', dtype={
        'Released_Year': str,  # Read as string first
        'Certificate': str,
        'Runtime': str,
        'IMDB_Rating': float,
        'Meta_score': float,
        'No_of_Votes': str,
        'Gross': str
    })

    # Clean and convert data types
    df['Released_Year'] = pd.to_numeric(df['Released_Year'], errors='coerce')
    df['No_of_Votes'] = pd.to_numeric(df['No_of_Votes'].str.replace(',', ''), errors='coerce')
    # df['Gross'] = pd.to_numeric(df['Gross'].str.replace(',', '').str.replace('$', ''), errors='coerce')

    # Create tables
    create_postgres_tables(pg_conn)

    # Process genres
    all_genres = set()
    for genres in df['Genre'].dropna():
        all_genres.update(process_genres(genres))
    
    # Insert genres and get mapping
    genre_map = insert_genres(pg_conn, all_genres)

    # Insert movies and create relationships
    with pg_conn.cursor() as cur:
        for _, row in df.iterrows():
            # Insert movie into PostgreSQL
            cur.execute("""
                INSERT INTO movies (
                    title, release_year, certificate, runtime,
                    imdb_rating, meta_score, overview, gross,
                    no_of_votes, poster_link
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                row['Series_Title'],
                int(row['Released_Year']) if pd.notna(row['Released_Year']) else None,
                row['Certificate'] if pd.notna(row['Certificate']) else None,
                int(row['Runtime'].split()[0]) if pd.notna(row['Runtime']) else None,
                float(row['IMDB_Rating']) if pd.notna(row['IMDB_Rating']) else None,
                float(row['Meta_score']) if pd.notna(row['Meta_score']) else None,
                row['Overview'] if pd.notna(row['Overview']) else None,
                clean_money_value(row['Gross']) if pd.notna(row['Gross']) else None,
                int(row['No_of_Votes']) if pd.notna(row['No_of_Votes']) else None,
                row['Poster_Link'] if pd.notna(row['Poster_Link']) else None
            ))
            
            movie_id = cur.fetchone()[0]

            # Insert movie_genres relationships
            for genre in process_genres(row['Genre']):
                cur.execute("""
                    INSERT INTO movie_genres (movie_id, genre_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (movie_id, genre_map[genre]))

            # Create Neo4j nodes and relationships
            with neo4j_driver.session() as session:
                # Create Movie node
                session.run("""
                    CREATE (m:Movie {id: $id, title: $title})
                """, id=movie_id, title=row['Series_Title'])

                # Create Director node and relationship
                if pd.notna(row['Director']):
                    session.run("""
                        MERGE (p:Person {name: $name})
                        WITH p
                        MATCH (m:Movie {id: $id})
                        MERGE (p)-[:DIRECTED]->(m)
                    """, name=row['Director'], id=movie_id)

                # Create Actor nodes and relationships
                for star_col in ['Star1', 'Star2', 'Star3', 'Star4']:
                    if pd.notna(row[star_col]):
                        session.run("""
                            MERGE (p:Person {name: $name})
                            WITH p
                            MATCH (m:Movie {id: $id})
                            MERGE (p)-[:ACTED_IN]->(m)
                        """, name=row[star_col], id=movie_id)

    # Commit PostgreSQL transactions
    pg_conn.commit()

    # Close connections
    pg_conn.close()
    neo4j_driver.close()

if __name__ == "__main__":
    main()