# Author Name: Moe Kyal Tun

import os
import requests
import sqlite3
import csv
import time
import matplotlib.pyplot as plt

# Load API Key from a file
try:
    with open('api_key.txt', 'r') as file:
        API_KEY = file.read().strip() # Read the API key from a file and remove whitespace
except FileNotFoundError:
    raise FileNotFoundError("The file 'api_key.txt' was not found. Ensure it contains your API key.")

# Load run_number.txt to keep track of API requests/pages processed
try:
    with open('run_number.txt', 'r') as file1:
        RUN_NUMBER = int(file1.read().strip()) # Read and convert the run number to an integer
except FileNotFoundError:
    raise FileNotFoundError("The file 'run_number.txt' was not found.")

# Increment the run_number.txt file for the next run to fetch new data/pages
try:
    with open('run_number.txt', "w") as file2:
        file2.write(str(RUN_NUMBER+1))
except FileNotFoundError:
    raise FileNotFoundError("The file 'run_number.txt' was not found.")

# API constant
DB_NAME = "outputs/final_project.db"  # SQLite database location
os.makedirs("outputs", exist_ok=True)  # Ensure the output directory exists
BASE_URL = 'https://newsapi.org/v2/everything' # NewsAPI Endpoint

# Database setup
def setup_database():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Create the sources tbale to store unique source names
    cursor.execute('''CREATE TABLE IF NOT EXISTS sources (
                   id INTEGER PRIMARY KEY,
                   name TEXT UNIQUE
                   )''')

    # Create the countries table to store unique country names 
    cursor.execute('''CREATE TABLE IF NOT EXISTS countries (
                        id INTEGER PRIMARY KEY,
                        name TEXT UNIQUE
                      )''')

    # Create the articles table to store articles details
    cursor.execute('''CREATE TABLE IF NOT EXISTS articles (
                        id INTEGER PRIMARY KEY,
                        source_id INTEGER,
                        title TEXT,
                        published_date TEXT,
                        country_id INTEGER,
                        FOREIGN KEY (source_id) REFERENCES sources (id),
                        FOREIGN KEY (country_id) REFERENCES countries (id)
                      )''')

    conn.commit()
    return conn

# Fetch news articles from NewsAPI
def fetch_articles(query, page=RUN_NUMBER):
    params = {
        'q': query, # Search query string
        'apiKey': API_KEY, # API key for authentication
        'pageSize': 5,  # Fetch maximum allowed articles per request
        'page': page # Current page number for pagination
    }
    response = requests.get(BASE_URL, params=params) # Make the API request
    if response.status_code == 200:
        return response.json()['articles'] # Return the list of articles
    else:
        print(f"Error: {response.status_code}, {response.text}") # Log errors if any
        return []

# Insert or get the ID of a unique value in a table
def get_or_create_id(conn, table, column, value):
    cursor = conn.cursor()

    # Check if the value already exists
    cursor.execute(f'''SELECT id FROM {table} WHERE {column} = ?''', (value,))
    row = cursor.fetchone()
    if row:
        return row[0]  # Return the existing ID

    # Insert the new value and return its ID
    cursor.execute(f'''INSERT INTO {table} ({column}) VALUES (?)''', (value,))
    conn.commit()
    return cursor.lastrowid

# Insert data into the database
def insert_data(conn, articles, country_name):
    cursor = conn.cursor()

    # Get or create the country ID
    country_id = get_or_create_id(conn, 'countries', 'name', country_name)

    for article in articles[:5]:  # Limit to 5 rows per run
        source_name = article['source']['name']  # Extract source name
        title = article['title']  # Extract article title
        published_date = article.get('publishedAt', None)  # Extract publish date if available

        # Get or create the source ID
        source_id = get_or_create_id(conn, 'sources', 'name', source_name)

        # Avoid duplicate entries
        cursor.execute('''SELECT id FROM articles WHERE source_id = ? AND title = ?''', (source_id, title))
        if cursor.fetchone():
            continue  # Skip duplicates

        # Insert article into the articles table
        cursor.execute('''INSERT INTO articles (source_id, title, published_date, country_id)
                          VALUES (?, ?, ?, ?)''', (source_id, title, published_date, country_id))

    conn.commit()

# Query data with advanced calculations
def query_advanced_data(conn):
    cursor = conn.cursor()

     # Count articles by country and day of the week
    cursor.execute('''
        SELECT c.name AS country_name, COUNT(a.id) AS article_count, strftime('%w', a.published_date) AS day_of_week
        FROM articles a
        JOIN countries c ON a.country_id = c.id
        JOIN sources s ON a.source_id = s.id
        WHERE a.published_date IS NOT NULL
        GROUP BY c.name, day_of_week
    ''')

    return cursor.fetchall()  # Return query results

# Write processed data to a CSV file
def write_data_to_csv(data, filename):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Country', 'Article Count', 'Day of Week']) # Write CSV headers
        writer.writerows(data) # Write the data rows

# Visualize data with advanced insights
def visualize_data(data):
    countries = {}
    for country, count, day_of_week in data:
        if country not in countries:
            countries[country] = [0] * 7 # Initialize a list for all days of the week
        countries[country][int(day_of_week)] += count # Increment the count for the respective day

    # Plot data for each countr
    for country, counts in countries.items():
        plt.plot(range(7), counts, label=country)

    # Set plot labels and title
    plt.xlabel('Day of the Week (0=Sunday, 6=Saturday)')
    plt.ylabel('Number of Articles')
    plt.title('Article Count by Country and Day of Week')
    plt.legend()
    plt.tight_layout()
    plt.savefig('outputs/published_trends.png')
    plt.show()
    

if __name__ == '__main__':
    # Setup database
    conn = setup_database()

    # Example countries and queries
    country_queries = {
        'USA': 'USA',
        'UK': 'UK',
        'Canada': 'Canada',
        'India': 'India',
        'Australia': 'Australia'
    }

    # Fetch and store articles for each country
    for country_name, query in country_queries.items():
        print(f"Fetching articles for {country_name}...")
        articles = fetch_articles(query) # Fetch articles for the country
        insert_data(conn, articles, country_name) # Insert the articles into the database

        # Delay of 1 second to avoid hitting API rate limits
        time.sleep(1)

    # Query and visualize advanced data
    print("Querying and visualizing advanced data...")
    advanced_data = query_advanced_data(conn)

    # Write data to CSV
    write_data_to_csv(advanced_data, "outputs/processed_data.csv")

    # Visualize the data
    visualize_data(advanced_data)

    # Close the database connection
    conn.close()
