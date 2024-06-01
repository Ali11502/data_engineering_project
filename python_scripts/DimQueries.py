import psycopg2
import pandas as pd

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
        return connection, cursor
    except psycopg2.Error as e:
        print("Error: Could not connect to destination database")
        print(e)
        return None, None

def execute_query(cursor, query):
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except psycopg2.Error as e:
        print(f"Error executing query: {e}")
        return None

def main():
    connection, cursor = connect_to_dest()
    if connection is not None and cursor is not None:
        # Define queries
        queries = {
            "Total Number of Flights by Aircraft": """
                SELECT
                    da.aircraft_name,
                    COUNT(fo.flight_operation_id) AS total_flights
                FROM
                    flight_operations fo
                JOIN
                    dimAircraft da ON fo.aircraft_id = da.aircraftid
                GROUP BY
                    da.aircraft_name
                ORDER BY
                    total_flights DESC;
            """,
            "Average Route Duration by Departure Airport": """
                SELECT
                    dap.airport_name AS departure_airport,
                    AVG(fo.route_duration) AS avg_duration
                FROM
                    flight_operations fo
                JOIN
                    dimAirport dap ON fo.departure_airport_code = dap.airport_code
                GROUP BY
                    dap.airport_name
                ORDER BY
                    avg_duration DESC;
            """,
            "Total Route Cost by Route and Month": """
                SELECT
                    dr.route_id,
                    dd.month,
                    SUM(fo.route_cost) AS total_route_cost
                FROM
                    flight_operations fo
                JOIN
                    dimRoute dr ON fo.route_id = dr.route_id
                JOIN
                    dimDateTime dd ON fo.departure_date_time_id = dd.date_time_id
                GROUP BY
                    dr.route_id, dd.month
                ORDER BY
                    dr.route_id, dd.month;
            """,
            "Booked Seats by Flight Status and Aircraft": """
                SELECT
                    df.status,
                    da.aircraft_name,
                    SUM(fo.booked_seats) AS total_booked_seats
                FROM
                    flight_operations fo
                JOIN
                    dimFlight df ON fo.flight_id = df.flightid
                JOIN
                    dimAircraft da ON fo.aircraft_id = da.aircraftid
                GROUP BY
                    df.status, da.aircraft_name
                ORDER BY
                    total_booked_seats DESC;
            """,
            "International vs. Domestic Flights": """
                SELECT
                    dap.is_international,
                    COUNT(fo.flight_operation_id) AS total_flights
                FROM
                    flight_operations fo
                JOIN
                    dimAirport dap ON fo.departure_airport_code = dap.airport_code
                GROUP BY
                    dap.is_international
                ORDER BY
                    total_flights DESC;
            """
        }

        # Execute queries and print results
        for query_name, query in queries.items():
            print(f"\nExecuting query: {query_name}")
            results = execute_query(cursor, query)
            if results is not None:
                df = pd.DataFrame(results)
                print(df)

        # Close the connection
        cursor.close()
        connection.close()

if __name__ == "__main__":
    main()
