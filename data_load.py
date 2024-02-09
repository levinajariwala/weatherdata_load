from sqlalchemy import create_engine
import pandas as pd
import urllib.parse
import psycopg2
from psycopg2 import sql
import uuid

# Encode the password
encoded_password = urllib.parse.quote("WelcomeItc@2022")

# Database connection parameters
db_params = {
            "user": "consultants",
            "password": "WelcomeItc@2022",
            "host": "ec2-3-9-191-104.eu-west-2.compute.amazonaws.com",  # Replace with the remote server's IP or domain name
            "port": "5432",  # Default PostgreSQL port
            "database": "testdb"
    }

# Create a connection
connection = psycopg2.connect(**db_params)

def create_table():
    

    try:
        
        # Create a cursor
        cursor = connection.cursor()

        # SQL statement to create the table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS Location (
            id SERIAL PRIMARY KEY,
            location_name TEXT,
            region TEXT,
            country TEXT,
            lat NUMERIC,
            lon NUMERIC,
            tz_id TEXT,
            event_time TIMESTAMP,
            UNIQUE (location_name,lat,lon)
        )
        """

        # Execute the create table SQL statement
        cursor.execute(create_table_sql)
        connection.commit()

            # SQL statement to create the Daily_Record table with a foreign key reference to Location
        create_daily_record_table_sql = """
        CREATE TABLE IF NOT EXISTS Daily_Record (
            id SERIAL PRIMARY KEY,
            location_id INT REFERENCES Location(id),  -- Foreign key reference to Location table
            dates DATE,
            maxtemp_c NUMERIC,
            maxtemp_f NUMERIC,
            mintemp_c NUMERIC,
            mintemp_f NUMERIC,
            avgtemp_c NUMERIC,
            avgtemp_f NUMERIC,
            maxwind_mph NUMERIC,
            maxwind_kph NUMERIC,
            totalprecip_mm NUMERIC,
            totalprecip_in NUMERIC,
            avgvis_km NUMERIC,
            avgvis_miles NUMERIC,
            avghumidity NUMERIC,
            day_Condition TEXT,
            uv NUMERIC,
            sunrise TIME,
            sunset TIME,
            moonrise TEXT,
            moonset TEXT,
            moon_phase TEXT,
            moon_illumination NUMERIC,
            UNIQUE (location_id,dates)
        )
        """
        # Execute the create table SQL statement
        cursor.execute(create_daily_record_table_sql)
        connection.commit()

        # SQL statement to create the Hourly_Record table with a foreign key reference to Daily_Record
        create_hourly_record_table_sql = """
        CREATE TABLE IF NOT EXISTS Hourly_Record (
            id SERIAL PRIMARY KEY,
            daily_record_id INT REFERENCES Daily_Record(id),  -- Foreign key reference to Daily_Record table
            times TIME,
            temp_c NUMERIC,
            temp_f NUMERIC,
            is_day NUMERIC,
            time_condition TEXT,
            wind_mph NUMERIC,
            wind_kph NUMERIC,
            wind_degree NUMERIC,
            wind_dir TEXT,
            pressure_mb NUMERIC,
            pressure_in NUMERIC,
            precip_mm NUMERIC,
            precip_in NUMERIC,
            humidity NUMERIC,
            cloud NUMERIC,
            feelslike_c NUMERIC,
            feelslike_f NUMERIC,
            windchill_c NUMERIC,
            windchill_f NUMERIC,
            heatindex_c NUMERIC,
            heatindex_f NUMERIC,
            dewpoint_c NUMERIC,
            dewpoint_f NUMERIC,
            will_it_rain NUMERIC,
            chance_of_rain NUMERIC,
            will_it_snow NUMERIC,
            chance_of_snow NUMERIC,
            vis_km NUMERIC,
            vis_miles NUMERIC,
            gust_mph NUMERIC,
            gust_kph NUMERIC,
            time_uv NUMERIC,
            UNIQUE (daily_record_id,times)
        )
        """

        # Execute the create table SQL statement
        cursor.execute(create_hourly_record_table_sql)
        connection.commit()

        # Close the cursor and connection
        cursor.close()
        # connection.close()
    except Exception as e:
        print("Error : ",str(e))
    
try:
    create_table()
    # Replace 'your_json_file.json' with the actual path to your JSON file
    json_file = 'C:\\Users\\levin\\OneDrive\\Desktop\\codes\\vspython\\src\\Project_code\\weather_dt\\weather_dt\\birmingham\\birmingham_01_dec.json'

    # Read the JSON file into a Pandas DataFrame    
    df = pd.read_json(json_file)
    print(df)

    print(df['forecast']['forecastday'])

    cursor = connection.cursor()

    # SQL query to insert data into the Location table, and ignore conflicts
    location_insert_query = sql.SQL(f"""WITH ins AS (
        INSERT INTO Location (location_name, region, country, lat, lon, tz_id, event_time)
        VALUES ('{df['location']['name']}', '{df['location']['region']}', '{df['location']['country']}', {df['location']['lat']}, {df['location']['lon']}, '{df['location']['tz_id']}','{df['location']['localtime']}')
        ON CONFLICT (location_name, lat, lon) DO NOTHING
        RETURNING id)
        SELECT id FROM ins
        UNION ALL
        SELECT id FROM Location WHERE location_name = '{df['location']['name']}' and lat={df['location']['lat']} and lon={df['location']['lon']}
        LIMIT 1;
    """)

    # Execute the query and fetch the id
    cursor.execute(location_insert_query)
    location_id = cursor.fetchone()
    print(location_id[0])


    
    connection.commit()

    # SQL query to insert data into the Location table, and ignore conflicts
    daily_record_insert_query = sql.SQL(f"""WITH ins AS (
        INSERT INTO Daily_Record (location_id, dates, maxtemp_c, maxtemp_f, mintemp_c, mintemp_f, avgtemp_c, avgtemp_f, maxwind_mph, maxwind_kph, totalprecip_mm, totalprecip_in, avgvis_km, avgvis_miles, avghumidity, day_Condition, uv, sunrise, sunset, moonrise, moonset, moon_phase, moon_illumination)
        VALUES ('{location_id[0]}', '{df['forecast']['forecastday'][0]['date']}', {df['forecast']['forecastday'][0]['day']['maxtemp_c']}, {df['forecast']['forecastday'][0]['day']['maxtemp_f']}, {df['forecast']['forecastday'][0]['day']['mintemp_c']}, {df['forecast']['forecastday'][0]['day']['mintemp_f']},{df['forecast']['forecastday'][0]['day']['avgtemp_c']},{df['forecast']['forecastday'][0]['day']['avgtemp_f']},{df['forecast']['forecastday'][0]['day']['maxwind_mph']},{df['forecast']['forecastday'][0]['day']['maxwind_kph']},{df['forecast']['forecastday'][0]['day']['totalprecip_mm']},{df['forecast']['forecastday'][0]['day']['totalprecip_in']},{df['forecast']['forecastday'][0]['day']['avgvis_km']},{df['forecast']['forecastday'][0]['day']['avgvis_miles']},{df['forecast']['forecastday'][0]['day']['avghumidity']},'{df['forecast']['forecastday'][0]['day']['condition']['text']}',{df['forecast']['forecastday'][0]['day']['uv']},'{df['forecast']['forecastday'][0]['astro']['sunrise']}','{df['forecast']['forecastday'][0]['astro']['sunset']}','{df['forecast']['forecastday'][0]['astro']['moonrise']}','{df['forecast']['forecastday'][0]['astro']['moonset']}','{df['forecast']['forecastday'][0]['astro']['moon_phase']}','{df['forecast']['forecastday'][0]['astro']['moon_illumination']}')
        ON CONFLICT (location_id, dates) DO NOTHING
        RETURNING id)
        SELECT id FROM ins
        UNION ALL
        SELECT id FROM Daily_Record WHERE location_id = {location_id[0]} and dates='{df['forecast']['forecastday'][0]['date']}' 
        LIMIT 1;
    """)
    print(daily_record_insert_query)

    # Execute the query and fetch the id
    cursor.execute(daily_record_insert_query)
    daily_record_id = cursor.fetchone()
    print("daily_record_id : ",daily_record_id[0])
    connection.commit()
    daily_record_idd = daily_record_id[0]
    for i in df['forecast']['forecastday'][0]['hour']:
        try:
            # Construct the SQL query
            print("daily_record_id : ",daily_record_idd)
            daily_record_insert_query = """
                INSERT INTO Hourly_Record (daily_record_id, times, temp_c, temp_f, is_day, time_condition, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, will_it_rain, chance_of_rain, will_it_snow, chance_of_snow, vis_km, vis_miles, gust_mph, gust_kph, time_uv)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
                ON CONFLICT (daily_record_id, times) DO NOTHING
                RETURNING id;
            """
            
            # Execute the query with parameters
            cursor.execute(daily_record_insert_query, (
                daily_record_idd, i['time'], i['temp_c'], i['temp_f'], i['is_day'],
                i['condition']['text'], i['wind_mph'], i['wind_kph'], i['wind_degree'],
                i['wind_dir'], i['pressure_mb'], i['pressure_in'], i['precip_mm'], i['precip_in'],
                i['humidity'], i['cloud'], i['feelslike_c'], i['feelslike_f'], i['windchill_c'],
                i['windchill_f'], i['heatindex_c'], i['heatindex_f'], i['dewpoint_c'], i['dewpoint_f'],
                i['will_it_rain'], i['chance_of_rain'], i['will_it_snow'], i['chance_of_snow'],
                i['vis_km'], i['vis_miles'], i['gust_mph'], i['gust_kph'], i['uv']
            ))

            # Fetch the ID and commit the transaction
            daily_record_id = cursor.fetchone()
            connection.commit()
        except Exception as e:
            connection.rollback()
            print("Error:", str(e))


    # for i in df['forecast']['forecastday'][0]['hour']:
    #     try:
    #         # print(i)
    #         # break
    #         # SQL query to insert data into the Location table, and ignore conflicts
    #         daily_record_insert_query = sql.SQL(f"""
    #             INSERT INTO Hourly_Record (daily_record_id, time, temp_c, temp_f, is_day, time_condition, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c,dewpoint_f,will_it_rain,chance_of_rain,will_it_snow,chance_of_snow,vis_km,vis_miles,gust_mph,gust_kph,time_uv)
    #             VALUES ('{daily_record_id[0]}', '{i['time']}', '{i['temp_c']}', '{i['temp_f']}', '{i['is_day']}', '{i['condition']['text']}','{i['wind_mph']}','{i['wind_kph']}','{i['wind_degree']}','{i['wind_dir']}','{i['pressure_mb']}','{i['pressure_in']}','{i['precip_mm']}','{i['precip_in']}','{i['humidity']}','{i['cloud']}','{i['feelslike_c']}','{i['feelslike_f']}','{i['windchill_c']}','{i['windchill_f']}','{i['heatindex_c']}','{i['heatindex_f']}','{i['dewpoint_c']}','{i['dewpoint_f']}','{i['will_it_rain']}','{i['chance_of_rain']}','{i['will_it_snow']}','{i['chance_of_snow']}','{i['vis_km']}','{i['vis_miles']}','{i['gust_mph']}','{i['gust_kph']}','{i['uv']}')
    #             ON CONFLICT (daily_record_id, time) DO NOTHING
    #             RETURNING id;
    #         """)
    #         print(daily_record_insert_query)

    #         # Execute the query and fetch the id
    #         cursor.execute(daily_record_insert_query)
    #         daily_record_id = cursor.fetchone()
    #         print(daily_record_id[0])
    #         connection.commit()
    #     except Exception as e:
    #         print(str(e))

        
    connection.commit()

    # Close the connection
    cursor.close()
    connection.close()

except Exception as e:
    print(str(e))