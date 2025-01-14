import pandas as pd
from pymongo import MongoClient

def clean_money_value(value: str) -> float:
    """Convert money string to float"""
    if pd.isna(value):
        return None
    return float(value.replace('$', '').replace(',', ''))

def process_genres(genre_string: str):
    """Convert genre string to list of genres"""
    if pd.isna(genre_string):
        return []
    return [g.strip() for g in genre_string.split(',')]

def main():
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["movies_db"]  # Create or use the database
    movies_collection = db["movies"]

    # Clear existing data
    #movies_collection.delete_many({})

    # Load movies.csv
    df = pd.read_csv('movies.csv', dtype={
        'Released_Year': str,
        'Certificate': str,
        'Runtime': str,
        'IMDB_Rating': float,
        'Meta_score': float,
        'No_of_Votes': str,
        'Gross': str
    })

    # Clean data
    df['Released_Year'] = pd.to_numeric(df['Released_Year'], errors='coerce')
    df['No_of_Votes'] = pd.to_numeric(df['No_of_Votes'].str.replace(',', ''), errors='coerce')
    df['Gross'] = df['Gross'].apply(clean_money_value)

    # Insert data into MongoDB
    for _, row in df.iterrows():
        movie = {
            "title": row['Series_Title'],
            "release_year": int(row['Released_Year']) if pd.notna(row['Released_Year']) else None,
            "certificate": row['Certificate'] if pd.notna(row['Certificate']) else None,
            "runtime": int(row['Runtime'].split()[0]) if pd.notna(row['Runtime']) else None,
            "imdb_rating": float(row['IMDB_Rating']) if pd.notna(row['IMDB_Rating']) else None,
            "meta_score": int(row['Meta_score']) if pd.notna(row['Meta_score']) else None,
            "overview": row['Overview'] if pd.notna(row['Overview']) else None,
            "gross": row['Gross'],
            "no_of_votes": int(row['No_of_Votes']) if pd.notna(row['No_of_Votes']) else None,
            "poster_link": row['Poster_Link'] if pd.notna(row['Poster_Link']) else None,
            "genres": process_genres(row['Genre']),
            "director": row['Director'] if pd.notna(row['Director']) else None,
            "stars": [row[star_col] for star_col in ['Star1', 'Star2', 'Star3', 'Star4'] if pd.notna(row[star_col])]
        }
        movies_collection.insert_one(movie)

    print("Movies inserted into MongoDB successfully.")

if __name__ == "__main__":
    main()
