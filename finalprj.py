# Author Name: Murtaza Ahmadi 
# Author Name: Moe Kyal Tun (fixed duplicate string data)

import os  # For managing directories and files
import requests  # For making HTTP requests to APIs
import sqlite3  # For interacting with SQLite databases
import matplotlib.pyplot as plt  # For creating visualizations

# -----------------------------------------------
# API Details
# -----------------------------------------------
WEATHER_API_KEY = "dc7e94ba44e8c15ce96ca03dd4b1d796"  # OpenWeatherMap API key
WEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"  # Weather API endpoint
AIR_QUALITY_BASE_URL = "http://api.openweathermap.org/data/2.5/air_pollution"  # Air Quality API endpoint

# -----------------------------------------------
# Cities (100 largest US cities)
# -----------------------------------------------
CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", 
    "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville", 
    "Fort Worth", "Columbus", "Charlotte", "San Francisco", "Indianapolis", "Seattle", 
    "Denver", "Washington", "Boston", "El Paso", "Nashville", "Detroit", 
    "Oklahoma City", "Portland", "Las Vegas", "Memphis", "Louisville", "Baltimore", 
    "Milwaukee", "Albuquerque", "Tucson", "Fresno", "Sacramento", "Kansas City", 
    "Mesa", "Atlanta", "Omaha", "Colorado Springs", "Raleigh", "Miami", 
    "Long Beach", "Virginia Beach", "Oakland", "Minneapolis", "Tulsa", "Tampa", 
    "Arlington", "New Orleans", "Wichita", "Cleveland", "Bakersfield", "Aurora", 
    "Anaheim", "Honolulu", "Santa Ana", "Riverside", "Corpus Christi", "Lexington", 
    "Stockton", "St. Louis", "Saint Paul", "Henderson", "Pittsburgh", "Cincinnati", 
    "Anchorage", "Greensboro", "Plano", "Lincoln", "Orlando", "Irvine", 
    "Newark", "Durham", "Chula Vista", "Toledo", "Fort Wayne", "St. Petersburg", 
    "Laredo", "Jersey City", "Chandler", "Madison", "Lubbock", "Scottsdale", 
    "Reno", "Buffalo", "Gilbert", "Glendale", "North Las Vegas", "Winston-Salem", 
    "Chesapeake", "Norfolk", "Fremont", "Garland", "Irving", "Hialeah", 
    "Richmond", "Boise", "Spokane", "Baton Rouge"
]

# -----------------------------------------------
# Database and Output Directory
# -----------------------------------------------
DB_NAME = "outputs/final_project.db"  # SQLite database file path
os.makedirs("outputs", exist_ok=True)  # Ensure the outputs directory exists

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

# -----------------------------------------------
# Step 1: Setup the SQLite Database
# -----------------------------------------------
def setup_database():
    """
    Sets up the SQLite database and creates tables for Weather and Air Quality data.
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Create the weather description table to store unique weather descriptions 
    cursor.execute('''CREATE TABLE IF NOT EXISTS WeatherDescriptions (
                        id INTEGER PRIMARY KEY,
                        description TEXT UNIQUE
                      )''')

    # Create Weather table with UNIQUE constraint on city
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT UNIQUE,
            temperature REAL,
            humidity INTEGER,
            wind_speed REAL,
            weather_description_id INTEGER,
            timestamp TEXT,
            FOREIGN KEY (weather_description_id) REFERENCES WeatherDescriptions (id)
        )
    """)

    # Create AirQuality table with UNIQUE constraint on city
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS AirQuality (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT UNIQUE,
            aqi INTEGER,
            pm2_5 REAL,
            pm10 REAL,
            co REAL,
            no2 REAL,
            o3 REAL,
            so2 REAL,
            timestamp TEXT
        )
    """)
    conn.commit()
    conn.close()

# -----------------------------------------------
# Step 2: Fetch Weather Data
# -----------------------------------------------
def fetch_weather_data(city_list):
    """
    Fetches weather data from OpenWeatherMap API for specified cities.
    Uses ON CONFLICT DO UPDATE to prevent duplicate IDs for the same city.
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for city in city_list:
        response = requests.get(WEATHER_BASE_URL, params={
            "q": city, "appid": WEATHER_API_KEY, "units": "metric"
        })
        if response.status_code == 200:
            data = response.json()
            weather_description_id = get_or_create_id(conn, 'WeatherDescriptions', 'description', data["weather"][0]["description"])
            cursor.execute("""
                INSERT INTO Weather (city, temperature, humidity, wind_speed, weather_description_id, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(city) DO UPDATE SET
                    temperature=excluded.temperature,
                    humidity=excluded.humidity,
                    wind_speed=excluded.wind_speed,
                    weather_description_id=excluded.weather_description_id,
                    timestamp=excluded.timestamp
            """, (city, data["main"]["temp"], data["main"]["humidity"], data["wind"]["speed"],
                  weather_description_id, data["dt"]))
            conn.commit()
    conn.close()

# -----------------------------------------------
# Step 3: Fetch Air Quality Data
# -----------------------------------------------
def fetch_air_quality_data(city_list):
    """
    Fetches air quality data using geographic coordinates from the Weather API.
    Uses ON CONFLICT DO UPDATE to prevent duplicate IDs for the same city.
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for city in city_list:
        weather_response = requests.get(WEATHER_BASE_URL, params={
            "q": city, "appid": WEATHER_API_KEY, "units": "metric"
        })
        if weather_response.status_code == 200:
            weather_data = weather_response.json()
            lat = weather_data["coord"]["lat"]
            lon = weather_data["coord"]["lon"]

            air_quality_response = requests.get(AIR_QUALITY_BASE_URL, params={
                "lat": lat, "lon": lon, "appid": WEATHER_API_KEY
            })
            if air_quality_response.status_code == 200:
                air_quality_data = air_quality_response.json()
                main_pollution = air_quality_data["list"][0]["main"]
                components = air_quality_data["list"][0]["components"]
                cursor.execute("""
                    INSERT INTO AirQuality (city, aqi, pm2_5, pm10, co, no2, o3, so2, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(city) DO UPDATE SET
                        aqi=excluded.aqi,
                        pm2_5=excluded.pm2_5,
                        pm10=excluded.pm10,
                        co=excluded.co,
                        no2=excluded.no2,
                        o3=excluded.o3,
                        so2=excluded.so2,
                        timestamp=excluded.timestamp
                """, (city, main_pollution["aqi"], components["pm2_5"], components["pm10"],
                      components["co"], components["no2"], components["o3"], components["so2"],
                      air_quality_data["list"][0]["dt"]))
                conn.commit()
    conn.close()

# -----------------------------------------------
# Step 4: Process Data
# -----------------------------------------------
def process_weather_data():
    """
    Processes Weather data to calculate average temperature and humidity by city.
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT city, AVG(temperature), AVG(humidity) FROM Weather GROUP BY city")
    results = cursor.fetchall()

    # Save results to a text file
    with open("outputs/weather_averages.txt", "w") as file:
        for row in results:
            file.write(f"City: {row[0]}, Avg Temperature: {row[1]:.2f}, Avg Humidity: {row[2]:.2f}\n")

    conn.close()
    return results

def process_air_quality_data():
    """
    Processes Air Quality data to calculate average PM2.5 levels and AQI counts by category.
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT city, AVG(pm2_5), aqi FROM AirQuality GROUP BY city")
    results = cursor.fetchall()

    # Save results to a text file
    with open("outputs/air_quality_pm2_5.txt", "w") as file:
        for row in results:
            file.write(f"City: {row[0]}, Avg PM2.5: {row[1]:.2f}, AQI: {row[2]}\n")

    conn.close()
    return results

# -----------------------------------------------
# Step 5: Visualize Data
# -----------------------------------------------
def visualize_weather_data(weather_results):
    """
    Creates a bar chart for average temperature and humidity by city.
    """
    cities = [row[0] for row in weather_results]
    avg_temps = [row[1] for row in weather_results]
    avg_humidity = [row[2] for row in weather_results]

    plt.figure(figsize=(12, 8))
    plt.bar(cities, avg_temps, color="blue", label="Avg Temperature")
    plt.bar(cities, avg_humidity, color="orange", alpha=0.6, label="Avg Humidity", bottom=avg_temps)
    plt.xticks(rotation=90)
    plt.ylabel("Values")
    plt.title("Average Temperature and Humidity by City")
    plt.legend()
    plt.tight_layout()
    plt.savefig("outputs/weather_temperature_humidity.png")
    plt.show()

def visualize_aqi_categories(air_quality_results):
    """
    Creates a pie chart for AQI categories across cities.
    """
    aqi_labels_map = {1: "Good", 2: "Fair", 3: "Moderate", 4: "Poor", 5: "Very Poor"}
    aqi_categories = [row[2] for row in air_quality_results if row[2] is not None]
    if not aqi_categories:
        print("No AQI data available to visualize.")
        return

    sizes = [aqi_categories.count(key) for key in aqi_labels_map.keys()]
    labels = [aqi_labels_map[key] for key, size in zip(aqi_labels_map.keys(), sizes) if size > 0]
    sizes = [size for size in sizes if size > 0]
    colors = plt.cm.Paired.colors[:len(sizes)]

    plt.figure(figsize=(8, 8))
    plt.pie(sizes, labels=labels, colors=colors, autopct="%1.1f%%", startangle=140)
    plt.title("AQI Category Distribution")
    plt.savefig("outputs/aqi_category_distribution.png")
    plt.show()

# -----------------------------------------------
# Main Execution
# -----------------------------------------------
if __name__ == "__main__":
    setup_database()
    
    # Determine which 25 cities to run this time
    run_count_file = "outputs/run_count.txt"
    if os.path.exists(run_count_file):
        with open(run_count_file, "r") as f:
            run_count = int(f.read().strip())
    else:
        run_count = 0

    # Process 25 cities per run, up to 100 total
    chunk_size = 25
    start_index = run_count * chunk_size
    end_index = start_index + chunk_size
    if start_index >= len(CITIES):
        print("All 100 cities have already been processed.")
        # We can still process, visualize, etc., but no new data will be fetched
        weather_results = process_weather_data()
        air_quality_results = process_air_quality_data()
        visualize_weather_data(weather_results)
        visualize_aqi_categories(air_quality_results)
        print("All tasks completed.")
        exit()

    city_subset = CITIES[start_index:end_index]

    fetch_weather_data(city_subset)
    fetch_air_quality_data(city_subset)
    weather_results = process_weather_data()
    air_quality_results = process_air_quality_data()
    visualize_weather_data(weather_results)
    visualize_aqi_categories(air_quality_results)

    # Increment run_count and save
    run_count += 1
    with open(run_count_file, "w") as f:
        f.write(str(run_count))

    print("All tasks completed.")