import datetime
import random
import psycopg2
import pandas as pd
import os
import time 


def connect_to_src():
    try:
        # Establish connection to PostgreSQL
        connection = psycopg2.connect(
            dbname="ams_db",
            user="postgres",
            password="root",
            host="localhost",
            port="5432"
        )
        print("Connection established!")

        # Create a cursor to execute SQL queries
        cursor = connection.cursor()

        connection.set_session(autocommit=True)
        print("autocommit is on")
        return connection, cursor
    except psycopg2.Error as e:
        print("Error: Could not connect to database")
        print(e)
        return None, None

def connect_to_dest():
    try:
        # Establish connection to PostgreSQL
        connection = psycopg2.connect(
            dbname="flight_operations_data_mart",
            user="postgres",
            password="root",
            host="localhost",
            port="5432"
        )
        print("Connection established with destination!")

        # Create a cursor to execute SQL queries
        cursor = connection.cursor()
        connection.set_session(autocommit=True)
        return connection,cursor
    except psycopg2.Error as e:
        print("Error: Could not connect to destination database")
        print(e)



def flashDB(connection, cursor):
    try:
        cursor.execute("DROP TABLE IF EXISTS flight_operations;")
    except psycopg2.Error as e:
        print(e)
    
    try:
        cursor.execute("DROP TABLE IF EXISTS dimAircraft;")
    except psycopg2.Error as e:
        print(e)
    
    try:
        cursor.execute("DROP TABLE IF EXISTS dimFlight;")
    except psycopg2.Error as e:
        print(e)
    
    try:
        cursor.execute("DROP TABLE IF EXISTS dimDateTime;")
    except psycopg2.Error as e:
        print(e)
    
    try:
        cursor.execute("DROP TABLE IF EXISTS dimRoute;")
    except psycopg2.Error as e:
        print(e)
    
    try:
        cursor.execute("DROP TABLE IF EXISTS dimAirport;")
    except psycopg2.Error as e:
        print(e)
    print("loading data from source")
def generate_aircraft_name(aircraft_id):
    # Generate aircraft name based on the suffix of aircraft_id
    suffix = int(aircraft_id) - 65
    return f"Aircraft{suffix}"

# extract

def load_data_from_db(cursor, query):
    try:
        # Execute the SQL query
        cursor.execute(query)
        
        # Fetch all rows from the result
        query_result = cursor.fetchall()
        
        # Load the query result into a DataFrame
        df = pd.DataFrame(query_result, columns=[
            'airport_code', 'route_id', 'aircraftid', 'flightid', 'airport_name', 
            'city', 'country', 'elevation', 'is_international', 'dep_airport_code', 
            'arr_airport_code', 'distance', 'route_duration', 'weather_conditions', 
            'departure_date_time', 'arrival_date_time', 'avbseats', 'status', 
            'aircraft_name', 'totalseats', 'aircraft_status', 'manufacturer'
        ])
        
        return df
    except psycopg2.Error as e:
        print("Error: Could not execute query")
        print(e)
        return None

def clean_data(df):
    # Perform data cleaning here
    # For example, you can handle missing values, data type conversions, etc.
    # Let's say you want to convert departure_date_time and arrival_date_time to datetime objects

    df['departure_date_time'] = pd.to_datetime(df['departure_date_time'])
    df['arrival_date_time'] = pd.to_datetime(df['arrival_date_time'])

    # Fill missing values in departure_date_time and arrival_date_time
    for index, row in df.iterrows():
        if pd.isnull(row['departure_date_time']) and pd.isnull(row['arrival_date_time']):
            # If both departure_date_time and arrival_date_time are missing, generate random times
            random_departure_hour = random.randint(0, 22)
            random_departure_minute = random.randint(0, 59)
            random_departure_second = random.randint(0, 59)
            random_arrival_hour = random.randint(random_departure_hour + 1, 23)  # Ensure arrival time is ahead of departure time
            random_arrival_minute = random.randint(0, 59)
            random_arrival_second = random.randint(0, 59)

            new_departure_time = pd.Timestamp.now().replace(hour=random_departure_hour,
                                                            minute=random_departure_minute,
                                                            second=random_departure_second)
            new_arrival_time = pd.Timestamp.now().replace(hour=random_arrival_hour,
                                                        minute=random_arrival_minute,
                                                        second=random_arrival_second)

            df.at[index, 'departure_date_time'] = new_departure_time
            df.at[index, 'arrival_date_time'] = new_arrival_time
        elif pd.isnull(row['departure_date_time']):
            # If departure_date_time is missing, generate a random time before arrival_date_time
            random_arrival_time = row['arrival_date_time']
            random_departure_time = random_arrival_time - pd.Timedelta(minutes=random.randint(1, 60))
            df.at[index, 'departure_date_time'] = random_departure_time
        elif pd.isnull(row['arrival_date_time']):
            # If arrival_date_time is missing, generate a random time after departure_date_time
            random_departure_time = row['departure_date_time']
            random_arrival_time = random_departure_time + pd.Timedelta(minutes=random.randint(1, 60))
            df.at[index, 'arrival_date_time'] = random_arrival_time



 

     # Fill missing values in aircraft_name based on aircraft_id suffix
    df['aircraft_name'] = df.apply(lambda row: generate_aircraft_name(row['aircraftid']) if pd.isnull(row['aircraft_name']) 
                                   else row['aircraft_name'], axis=1)


     # Find cases where departure time is equal to arrival time
    equal_times = df[df['departure_date_time'] == df['arrival_date_time']]
    
    # Iterate over the rows with equal departure and arrival times
    for index, row in equal_times.iterrows():
        # Generate a random time difference within a reasonable range (e.g., between 30 minutes and 2 hours)
        random_offset = random.randint(30, 120)  # Random offset in minutes
        new_arrival_time = row['departure_date_time'] + pd.Timedelta(minutes=random_offset)
        df.at[index, 'arrival_date_time'] = new_arrival_time

    # Fill missing values in airport_name, city, and country based on airport_code
    for index, row in df.iterrows():
        if pd.isnull(row['airport_name']):
            df.at[index, 'airport_name'] = f"Airport{row['airport_code'][1:]}"
        if pd.isnull(row['city']):
            df.at[index, 'city'] = f"City{row['airport_code'][1:]}"
        if pd.isnull(row['country']):
            df.at[index, 'country'] = f"Country{row['airport_code'][1:]}"


    # Perform linear interpolation on elevation column
    # Interpolate missing values linearly
    df['elevation'] = df['elevation'].interpolate(method='linear')

    # Forward fill any remaining NaNs
    df['elevation'] = df['elevation'].fillna(method='ffill')

    # If there are still NaNs at the beginning of the series, use backward fill
    df['elevation'] = df['elevation'].fillna(0)

    # Round the values and convert to integer
    df['elevation'] = df['elevation'].round().astype(int)
    # Perform linear interpolation on distance column
    df['distance'] = df['distance'].interpolate(method='linear')

    # Ensure there are no remaining missing values in the distance column
    df['distance'].fillna(df['distance'].mean(), inplace=True)
    # Perform linear interpolation on avbseats and totalseats columns
     # Calculate the mean of the column, ignoring NaNs
    median = df['avbseats'].median()
    # Fill NaNs with the mean value
    df['avbseats'] = df['avbseats'].fillna(median)
    median2 = df['totalseats'].median()

    # Fill NaNs with the mean value
    df['totalseats'] = df['totalseats'].fillna(median2)
     # Fill missing values in is_international and weather_conditions with mode
    df['is_international'].fillna(df['is_international'].mode()[0], inplace=True)
    df['weather_conditions'].fillna(df['weather_conditions'].mode()[0], inplace=True)
    df['status'].fillna(df['status'].mode()[0], inplace=True)
    df['manufacturer'].fillna(df['manufacturer'].mode()[0], inplace=True)        
    
    df['route_duration'] = (df['arrival_date_time'] - df['departure_date_time']).dt.total_seconds() / 60  # Convert to minutes
    
    return df

# creating empty dimensions and facts in destination (dwh)
def createDimensions_n_Facts(connection,cursor):
    dimAircraft_query="""
CREATE TABLE dimAircraft (
 aircraftid INT PRIMARY KEY,
    aircraft_name VARCHAR(255),
    totalseats INT,
    aircraft_status VARCHAR(50),
    manufacturer VARCHAR(100)
   
);
"""
    dimRoute_query="""
CREATE TABLE dimRoute (
   route_id INT PRIMARY KEY,
    weather_conditions VARCHAR(100)
 
);


"""
    dimFlight_query="""
CREATE TABLE dimFlight (
    flightid INT PRIMARY KEY,
    avbseats INT,
    status VARCHAR(50)
);
"""
    dimAirport_query="""
CREATE TABLE dimAirport (
    airport_code VARCHAR(100) PRIMARY KEY,
    airport_name VARCHAR(255),
    city VARCHAR(100),
    country VARCHAR(100),
    elevation INT,
    is_international BOOLEAN
);
"""
    dimDateTime_query="""
create table dimDateTime (
date_time_id serial primary key,
date date,
month int,
year int,
time time,
hours int,
minutes int,
meridiem varchar(3)
);"""
    #fact table # temporary i.e. few columns will be dropped later
    flight_operations="""
CREATE TABLE flight_operations (
flight_operation_id serial primary key,
    route_id INT,
    aircraft_id INT,
    departure_airport_code varchar(100),
    arrival_airport_code varchar(100),
    flight_id INT,
    departure_date_time_id INT,
	arrival_date_time_id int,
     avbseats INT,
      distance float,
    route_duration INT,
    route_cost int,
    total_seats INT,
	booked_seats int,
    FOREIGN KEY (route_id) REFERENCES dimRoute(route_id),
    FOREIGN KEY (aircraft_id) REFERENCES dimAircraft(aircraftid),
    FOREIGN KEY (departure_airport_code) REFERENCES dimAirport(airport_code),
    FOREIGN KEY (arrival_airport_code) REFERENCES dimAirport(airport_code),	
    FOREIGN KEY (flight_id) REFERENCES dimFlight(flightid),
    FOREIGN KEY (departure_date_time_id) REFERENCES dimDateTime(date_time_id),
	FOREIGN KEY (arrival_date_time_id) REFERENCES dimDateTime(date_time_id)

);

 """
    cursor.execute(dimAircraft_query)
    print("dimAircraft created")
    cursor.execute(dimAirport_query)
    print("dimAirport created")

    cursor.execute(dimDateTime_query)
    print("dimDateTime created")

    cursor.execute(dimRoute_query)
    print("dimRoute created")

    cursor.execute(dimFlight_query)
    print("dimFlight created")
    cursor.execute(flight_operations)
    print("flight_operations created")

def load_dimensions(connection,cursor,df):
   #++++++++++++++++++++++++++++++++++++++++++++++for dimFlight++++++++++++++++++++++++++++++++++++++++++++++++
    distinct_df = df.drop_duplicates(subset=['flightid'])[['flightid', 'avbseats', 'status']]
    # Convert the DataFrame to a list of tuples for insertion
    dim_flight_data = [tuple(row) for row in distinct_df.values]
    # SQL statement for inserting into dimFlight
    insert_query = """
    INSERT INTO dimFlight (flightid, avbseats, status)
    VALUES (%s, %s, %s)
    """
    try:
        # Insert data into dimFlight table
        cursor.executemany(insert_query, dim_flight_data)
        connection.commit()
        print("Data inserted into dimFlight successfully.")
        print(distinct_df.shape)
    except Exception as e:
        # connection.rollback()
        print(f"Error inserting data into dimFlight: {e}")

       #++++++++++++++++++++++++++++++++++++++++++++++for dimAirport++++++++++++++++++++++++++++++++++++++++++++++++

    # Select rows with unique airport_code values while keeping other attributes
    unique_airport_df = df.drop_duplicates(subset=['airport_code'])[['airport_code', 'airport_name', 'city', 'country', 'elevation', 'is_international']]

    # Convert the DataFrame to a list of tuples for insertion
    dim_airport_data = [tuple(row) for row in unique_airport_df.values]

    # SQL statement for inserting into dimAirport
    insert_query = """
    INSERT INTO dimAirport (airport_code, airport_name, city, country, elevation, is_international)
    VALUES (%s, %s, %s, %s, %s, %s)
    """

    try:
        # Insert data into dimAirport table
        cursor.executemany(insert_query, dim_airport_data)
        connection.commit()
        print("Data inserted into dimAirport successfully.")
        print(unique_airport_df.shape)

    except Exception as e:
        # connection.rollback()
        print(f"Error inserting data into dimAirport: {e}")

           #++++++++++++++++++++++++++++++++++++++++++++++for dimRoute++++++++++++++++++++++++++++++++++++++++++++++++

     # Select rows with unique route_id values while keeping other attributes
    unique_route_df = df.drop_duplicates(subset=['route_id'])[['route_id',  'weather_conditions']]

    # Convert the DataFrame to a list of tuples for insertion
    dim_route_data = [tuple(row) for row in unique_route_df.values]

    # SQL statement for inserting into dimRoute
    insert_query = """
    INSERT INTO dimRoute (route_id,  weather_conditions)
    VALUES (%s, %s)
    """

    try:
        # Insert data into dimRoute table
        cursor.executemany(insert_query, dim_route_data)
        connection.commit()
        print("Data inserted into dimRoute successfully.")
        print(unique_route_df.shape)
    except Exception as e:
        connection.rollback()
        print(f"Error inserting data into dimRoute: {e}")

               #++++++++++++++++++++++++++++++++++++++++++++++for dimAircraft++++++++++++++++++++++++++++++++++++++++++++++++
    
    # Select rows with unique aircraftid values while keeping other attributes
    unique_aircraft_df = df.drop_duplicates(subset=['aircraftid'])[['aircraftid', 'aircraft_name', 'totalseats', 'aircraft_status', 'manufacturer']]

    # Convert the DataFrame to a list of tuples for insertion
    dim_aircraft_data = [tuple(row) for row in unique_aircraft_df.values]

    # SQL statement for inserting into dimAircraft
    insert_query = """
    INSERT INTO dimAircraft (aircraftid, aircraft_name, totalseats, aircraft_status, manufacturer)
    VALUES (%s, %s, %s, %s, %s)
    """

    try:
        # Insert data into dimAircraft table
        cursor.executemany(insert_query, dim_aircraft_data)
        connection.commit()
        print("Data inserted into dimAircraft successfully.")
        print(unique_aircraft_df.shape)
    except Exception as e:
        # connection.rollback()
        print(f"Error inserting data into dimAircraft: {e}")

               #++++++++++++++++++++++++++++++++++++++++++++++for dimDateTime++++++++++++++++++++++++++++++++++++++++++++++++
    dep_df,arr_df=extract_datetime_components(df)
    load_dimDateTime(connection,cursor,dep_df,arr_df)

def extract_datetime_components(df):
    # Extract date, time, and meridian components from departure_date_time
    df['dep_date'] = df['departure_date_time'].dt.date
    df['dep_time'] = df['departure_date_time'].dt.time
    df['dep_hour'] = df['departure_date_time'].dt.hour
    df['dep_minute'] = df['departure_date_time'].dt.minute
    df['dep_meridiem'] = df['departure_date_time'].dt.strftime('%p')
    df['dep_month'] = df['departure_date_time'].dt.month
    df['dep_year'] = df['departure_date_time'].dt.year

    # Extract date, time, and meridian components from arrival_date_time
    df['arr_date'] = df['arrival_date_time'].dt.date
    df['arr_time'] = df['arrival_date_time'].dt.time
    df['arr_hour'] = df['arrival_date_time'].dt.hour
    df['arr_minute'] = df['arrival_date_time'].dt.minute
    df['arr_meridiem'] = df['arrival_date_time'].dt.strftime('%p')
    df['arr_month'] = df['arrival_date_time'].dt.month
    df['arr_year'] = df['arrival_date_time'].dt.year

    # Create new DataFrames for departure and arrival date/time components
    dep_df = df[['dep_date','dep_month',  'dep_year', 'dep_time', 'dep_hour', 'dep_minute', 'dep_meridiem']].drop_duplicates()
    arr_df = df[['arr_date', 'arr_month', 'arr_year','arr_time', 'arr_hour', 'arr_minute', 'arr_meridiem']].drop_duplicates()
    return dep_df,arr_df

def load_dimDateTime(connection, cursor, dep_df, arr_df):
    # Combine departure and arrival DataFrames
    # Vertically concatenate departure and arrival DataFrames
    dep_df.rename(columns={
        'dep_date': 'date',
        'dep_time': 'time',
        'dep_meridiem': 'meridiem',
        'dep_year': 'year',
        'dep_month': 'month',
        'dep_hour': 'hours',
        'dep_minute': 'minutes'
    }, inplace=True)
    arr_df.rename(columns={
        'arr_date': 'date',
        'arr_time': 'time',
        'arr_meridiem': 'meridiem',
        'arr_year': 'year',
        'arr_month': 'month',
        'arr_hour': 'hours',
        'arr_minute': 'minutes'
    }, inplace=True)
    combined_df = pd.concat([dep_df, arr_df]).drop_duplicates()

    # Sort the combined DataFrame by 'date' and 'time' columns in ascending order
    combined_df_sorted = combined_df.sort_values(by=['date', 'time'])    
    # Convert the DataFrame to a list of tuples for insertion
    dim_datetime_data = [tuple(row) for row in combined_df_sorted.values]
    # SQL statement for inserting into dimDateTime
    insert_query = """
    INSERT INTO dimDateTime (date,month, year, time, hours, minutes,meridiem )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    try:
        # Insert data into dimDateTime table
        cursor.executemany(insert_query, dim_datetime_data)
        connection.commit()
        print("Data inserted into dimDateTime successfully.")
        print(combined_df_sorted.shape)
    except Exception as e:
        connection.rollback()
        print(f"Error inserting data into dimDateTime: {e}")


def get_datetime_id(cur,datetime_value):
    # Execute a SQL query to get the ID from dimDateTime
    cur.execute("SELECT date_time_id FROM dimDateTime WHERE date = %s AND time = %s", (datetime_value.date(), datetime_value.time()))
    result = cur.fetchone()
    if result:
        return result[0]  # Return the ID
    else:
        return None  # Handle if ID not found
    
    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++for flight_operations+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    
    # load

def load_facts(connection,cursor,df):
    # Step 1: Extract required columns from the DataFrame
# Assuming df contains the data extracted from the source
# Adjust column names as needed to match your DataFrame
    df_selected = df[['route_id', 'aircraftid', 'flightid', 'dep_airport_code', 
                    'arr_airport_code',  'departure_date_time', 'arrival_date_time',
                    'distance', 'route_duration', 'avbseats', 'totalseats']]
    # Step 2: Calculate route_cost and booked_seats
    df_selected['route_cost'] = df_selected['distance'] * 2
    df_selected['booked_seats'] = df_selected['totalseats'] - df_selected['avbseats']
    
# Step 3: Map departure_date_time_id and arrival_date_time_id using dimDateTime
# Assuming dimDateTime_df contains datetime data and their corresponding IDs
# Adjust column names as needed to match your DataFrame and database table
        # Apply the function to get departure_date_time_id and arrival_date_time_id
    df_selected['departure_date_time_id'] = df_selected['departure_date_time'].apply(lambda dt: get_datetime_id(cursor, dt))
    df_selected['arrival_date_time_id'] = df_selected['arrival_date_time'].apply(lambda dt: get_datetime_id(cursor, dt))

    # Step 4: Drop the original date_time columns
    df_selected.drop(['departure_date_time', 'arrival_date_time'], axis=1, inplace=True)



    # Step 4: Insert data into your database table
    # Assuming you have established a connection 'conn' and a cursor 'cur'
    # Adjust table name and column names as needed
    for index, row in df_selected.iterrows():
        cursor.execute("""
            INSERT INTO flight_operations 
            (route_id, aircraft_id, departure_airport_code, arrival_airport_code, flight_id, departure_date_time_id,
            arrival_date_time_id, avbseats, distance, route_duration, route_cost, total_seats, booked_seats)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (row['route_id'], row['aircraftid'], row['dep_airport_code'], row['arr_airport_code'],
            row['flightid'], row['departure_date_time_id'], row['arrival_date_time_id'], row['avbseats'],
            row['distance'], row['route_duration'], row['route_cost'], row['totalseats'], row['booked_seats']))
        
        # delete duplicates
    cursor.execute("""DELETE FROM flight_operations
WHERE flight_operation_id NOT IN (
    SELECT DISTINCT ON (flight_id) flight_operation_id
    FROM public.flight_operations
    ORDER BY flight_id, flight_operation_id ASC
);
""")
    print("duplicates deleted")
    cursor.execute("""ALTER TABLE public.flight_operations
DROP COLUMN avbseats,
DROP COLUMN total_seats;""")
    print("column dropped")
    print("flight_operations loaded")
    cursor.execute("select * from flight_operations")
    ddf=pd.DataFrame(cursor.fetchall())
    print(ddf.shape)
    
def load_facts_snapshot( cursor):
    try:
        # Execute SQL query to fetch denormalized snapshot data
        cursor.execute("""
           SELECT
                fo.flight_operation_id,
                df.flightid,
				df.avbseats,
				df.status,
                dr.route_id,
                dr.weather_conditions,
                daa.aircraftid,
				daa.aircraft_name,
				daa.totalseats,
				daa.aircraft_status,
				daa.manufacturer,
                da.airport_code AS departure_airport_code,
                da.airport_name AS departure_airport_name,
                da.city AS departure_city,
                da.country AS departure_country,
                da.elevation AS departure_elevation,
                da.is_international AS departure_is_international,
                aa.airport_code AS arrival_airport_code,
                aa.airport_name AS arrival_airport_name,
                aa.city AS arrival_city,
                aa.country AS arrival_country,
                aa.elevation AS arrival_elevation,
                aa.is_international AS arrival_is_international,
                dd.date AS departure_date,
                dd.month AS departure_month,
                dd.year AS departure_year,
                dd.time AS departure_time,
                dd.hours AS departure_hours,
                dd.minutes AS departure_minutes,
                dd.meridiem AS departure_meridiem,
				ad.date AS arrival_date,
                ad.month AS arrival_month,
                ad.year AS arrival_year,
                ad.time AS arrival_time,
                ad.hours AS arrival_hours,
                ad.minutes AS arrival_minutes,
                ad.meridiem AS arrival_meridiem,
				fo.route_duration,
                fo.distance,
                fo.route_cost AS total_route_cost,
                fo.booked_seats
            FROM
                flight_operations fo
            JOIN
                dimAirport da ON fo.departure_airport_code = da.airport_code
            JOIN
                dimAirport aa ON fo.arrival_airport_code = aa.airport_code
            JOIN
                dimDateTime dd ON fo.departure_date_time_id = dd.date_time_id
            join 
                dimDateTime ad ON fo.arrival_date_time_id = ad.date_time_id

            join
                 dimroute dr on fo.route_id=dr.route_id
            join 
                dimflight df on df.flightid=fo.flight_id
            join 
                dimAircraft daa on fo.aircraft_id=daa.aircraftid
				order by flight_operation_id asc;
            
                         
        """)

        # Fetch all rows from the result
        snapshot_data = cursor.fetchall()

        # Create DataFrame from snapshot data
        snapshot_df = pd.DataFrame(snapshot_data,columns = [
    'flight_operation_id',
    'flightid',
    'avbseats',
    'status',
    'route_id',
    'weather_conditions',
    'aircraftid',
    'aircraft_name',
    'totalseats',
    'aircraft_status',
    'manufacturer',
    'departure_airport_code',
    'departure_airport_name',
    'departure_city',
    'departure_country',
    'departure_elevation',
    'departure_is_international',
    'arrival_airport_code',
    'arrival_airport_name',
    'arrival_city',
    'arrival_country',
    'arrival_elevation',
    'arrival_is_international',
    'departure_date',
    'departure_month',
    'departure_year',
    'departure_time',
    'departure_hours',
    'departure_minutes',
    'departure_meridiem',
    'arrival_date',
    'arrival_month',
    'arrival_year',
    'arrival_time',
    'arrival_hours',
    'arrival_minutes',
    'arrival_meridiem',
    'route_duration',
    'distance',
    'route_cost',
    'booked_seats'
]
    )
    # first time when running pipeline, run it without changing dATA IN OLTP. after first time, just change data and execute pipeline to see the changes in dashboard.
    # you may run pipeline second time without changing data but in metrics_change_dashboard, there will be change shown as 0.00 i.e. no change.
    # dashboard for analysis is created using    flight_operations_snapshot_new.csv
    
        if not os.path.exists("flight_operations_snapshot_old.csv"):
            snapshot_df.to_csv("flight_operations_snapshot_old.csv", index=False)
            print("Snapshot data saved to 'flight_operations_snapshot_old.csv'.")
        elif not os.path.exists("flight_operations_snapshot_new.csv"):
            snapshot_df.to_csv("flight_operations_snapshot_new.csv", index=False)
            print("Snapshot data saved to 'flight_operations_snapshot_new.csv'.")
        #     #  both files exist, execute 'metrics.py'
        #     print("Both files exist. Executing 'metrics.py'.")
      
        # elif os.path.exists("flight_operations_snapshot_old.csv") and os.path.exists("flight_operations_snapshot_new.csv"):
        #    os.remove("flight_operations_snapshot_old.csv")
        #    os.remove("flight_operations_snapshot_new.csv")
        #    snapshot_df.to_csv("flight_operations_snapshot_old.csv", index=False)
        #    print("files were existing so we removed both and now created the old version of snapshots")


    except psycopg2.Error as e:
        print(f"Error fetching snapshot data: {e}")

def handle_files():
    old_filename = 'flight_operations_snapshot_new.csv'
    new_filename = 'flight_operations_snapshot_old.csv'

# Check if the old file exists in the current directory
    if os.path.isfile(old_filename):
        try:
            # Rename the file
            os.remove("flight_operations_snapshot_old.csv")
            os.rename(old_filename, new_filename)
            print(f"Renamed '{old_filename}' to '{new_filename}'")
        except OSError as e:
            print(f"Error renaming file: {e}")
    else:
        print(f"File '{old_filename}' not found in the current directory.")

        # master function
def main():
    # Connect to the source database
    connection, cursor = connect_to_src()
    connection_dest,cursor_dest=connect_to_dest()
    
    if connection and cursor:
        # SQL query to extract data
        query = """
        SELECT 
            ai.airport_code, 
            ri.route_id,
            ac.aircraftid,
            f.flightid,
            ai.airport_name, 
            ai.city, 
            ai.country, 
            ai.elevation, 
            ai.is_international,
            ri.dep_airport_code, 
            ri.arr_airport_code, 
            ri.distance, 
            ri.avg_duration as route_duration, 
            ri.weather_conditions,
            f.deptime as departure_date_time, 
            f.arrtime as arrival_date_time, 
            f.avbseats, 
            f.status, 
            ac.aname AS aircraft_name, 
            ac.totalseats, 
            ac.astatus AS aircraft_status, 
            ac.manufacturer
        FROM 
            public.airportinfo ai
        JOIN 
            route_info ri ON ai.airport_code = ri.dep_airport_code OR ai.airport_code = ri.arr_airport_code
        JOIN 
            flight f ON ri.route_id = f.route
        JOIN 
            aircraft ac ON f.aircraftid = ac.aircraftid
        ORDER BY 
            flightid ASC;
        """

        # Load data from database
        df = load_data_from_db(cursor, query)

        # Clean the data
        cleaned_df = clean_data(df)
      
        flashDB(connection_dest,cursor_dest)
        createDimensions_n_Facts(connection_dest,cursor_dest)
        load_dimensions(connection_dest,cursor_dest,cleaned_df)
        load_facts(connection_dest,cursor_dest,cleaned_df)

        # before the snapshot creation we will ensure that previous flight_operations_snapshot_new.csv is renamed to
        #  flight_operations_snapshot_old.csv so that flight_operations_snapshot_new.csv could be created which will correspond to 
        # latest state of oltp we will be using this function:

        handle_files()


        load_facts_snapshot(cursor_dest)
        # Close the database connection
        cursor.close()
        connection.close()
        print("connection and cursor with source closed")
        cursor_dest.close()
        connection_dest.close()
        print("connection and cursor with destination closed")


if __name__ == "__main__":
    main()

