import requests
import pandas as pd
import sqlite3
from datetime import datetime
import time
from sklearn.preprocessing import MinMaxScaler
import numpy as np


class MovieRecommendationPipeline:
    def __init__(self, tmdb_api_key):
        self.api_key = tmdb_api_key
        self.base_url = "https://api.themoviedb.org/3"
        self.db_name = "movie_recommendations.db"

    def extract_movie_data(self, num_pages=5):
        """Extract movie data from TMDB API"""
        print("Starting data extraction...")
        movies_list = []

        for page in range(1, num_pages + 1):
            # Get popular movies
            url = f"{self.base_url}/movie/popular"
            params = {
                'api_key': self.api_key,
                'page': page
            }

            response = requests.get(url, params=params)
            if response.status_code == 200:
                movies = response.json()['results']

                for movie in movies:
                    # Get additional movie details
                    movie_id = movie['id']
                    details_url = f"{self.base_url}/movie/{movie_id}"
                    details_params = {
                        'api_key': self.api_key,
                        'append_to_response': 'credits,keywords'
                    }

                    details_response = requests.get(
                        details_url, params=details_params)
                    if details_response.status_code == 200:
                        movie_details = details_response.json()

                        movie_data = {
                            'movie_id': movie_id,
                            'title': movie['title'],
                            'release_date': movie.get('release_date'),
                            'popularity': movie.get('popularity'),
                            'vote_average': movie.get('vote_average'),
                            'vote_count': movie.get('vote_count'),
                            'genres': ','.join([genre['name'] for genre in movie_details.get('genres', [])]),
                            'runtime': movie_details.get('runtime'),
                            'budget': movie_details.get('budget'),
                            'revenue': movie_details.get('revenue'),
                            'director': next((crew['name'] for crew in movie_details.get('credits', {}).get('crew', [])
                                              if crew['job'] == 'Director'), None),
                            'cast': ','.join([cast['name'] for cast in movie_details.get('credits', {}).get('cast', [])[:5]]),
                            'keywords': ','.join([kw['name'] for kw in movie_details.get('keywords', {}).get('keywords', [])])
                        }
                        movies_list.append(movie_data)

                    # Respect API rate limits
                    time.sleep(0.25)

            print(f"Processed page {page}/{num_pages}")

        return pd.DataFrame(movies_list)

    def transform_data(self, df):
        """Transform and preprocess the movie data"""
        print("Starting data transformation...")

        # Clean dates
        df['release_date'] = pd.to_datetime(
            df['release_date'], errors='coerce')
        df['release_year'] = df['release_date'].dt.year

        # Handle missing values
        df['runtime'] = df['runtime'].fillna(df['runtime'].mean())
        df['budget'] = df['budget'].fillna(0)
        df['revenue'] = df['revenue'].fillna(0)

        # Create derived features
        df['roi'] = np.where(
            df['budget'] != 0, (df['revenue'] - df['budget']) / df['budget'], 0)

        # Normalize numerical features
        scaler = MinMaxScaler()
        numerical_cols = ['popularity', 'vote_average',
                          'vote_count', 'runtime', 'roi']
        df[numerical_cols] = scaler.fit_transform(df[numerical_cols])

        # Create genre features (one-hot encoding)
        genres_df = df['genres'].str.get_dummies(sep=',')

        # Combine features for recommendation
        feature_cols = numerical_cols + list(genres_df.columns)
        features_df = pd.concat([df[numerical_cols], genres_df], axis=1)

        return df, features_df

    def load_data(self, movies_df, features_df):
        """Load processed data into SQLite database"""
        print("Starting data loading...")

        conn = sqlite3.connect(self.db_name)

        # Create movies table
        movies_df.to_sql('movies', conn, if_exists='replace', index=False)

        # Create features table
        features_df.to_sql('movie_features', conn,
                           if_exists='replace', index=False)

        # Create some useful views
        conn.execute("""
        CREATE VIEW IF NOT EXISTS top_rated_movies AS
        SELECT movie_id, title, vote_average, vote_count, popularity
        FROM movies
        WHERE vote_count > (SELECT AVG(vote_count) FROM movies)
        ORDER BY vote_average DESC
        LIMIT 100
        """)

        conn.execute("""
        CREATE VIEW IF NOT EXISTS genre_stats AS
        SELECT 
            genres,
            COUNT(*) as movie_count,
            AVG(vote_average) as avg_rating,
            AVG(popularity) as avg_popularity
        FROM movies
        GROUP BY genres
        """)

        conn.commit()
        conn.close()

        print("Data pipeline completed successfully!")


def main():
    # Initialize pipeline
    api_key = "313bfe25f2e7a3efa03ab55ede52edc9"  # Replace with your actual API key
    pipeline = MovieRecommendationPipeline(api_key)

    # Extract data
    movies_df = pipeline.extract_movie_data(
        num_pages=5)  # Adjust number of pages as needed

    # Transform data
    processed_df, features_df = pipeline.transform_data(movies_df)

    # Load data
    pipeline.load_data(processed_df, features_df)


if __name__ == "__main__":
    main()
