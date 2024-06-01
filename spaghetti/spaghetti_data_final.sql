-- SCHEMA: ams_db

-- DROP SCHEMA IF EXISTS ams_db ;

CREATE SCHEMA IF NOT EXISTS ams_db
    AUTHORIZATION postgres;
	
	
	
	CREATE TABLE Users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    isAdmin BOOLEAN,
    email VARCHAR(255) UNIQUE,
    password VARCHAR(255)
);


CREATE TABLE Aircraft (
    aircraftid serial PRIMARY KEY,
    aname VARCHAR(255),
    totalseats INT,
    astatus VARCHAR(50)
);
ALTER TABLE Aircraft
ADD manufacturer varchar(255);

CREATE TABLE AirportInfo (
    airport_code VARCHAR(10) PRIMARY KEY,
    airport_name VARCHAR(255),
    city VARCHAR(255),
    country VARCHAR(255),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    elevation INT,
    timezone VARCHAR(50),
    is_international BOOLEAN
);

CREATE TABLE route_info (
    route_id serial PRIMARY KEY,
    dep_airport_code VARCHAR(10),
    arr_airport_code VARCHAR(10),
    distance DECIMAL(10,2),
    avg_duration INT,
    frequency INT,
    weather_conditions VARCHAR(50),
    FOREIGN KEY (dep_airport_code) REFERENCES AirportInfo(airport_code),
    FOREIGN KEY (arr_airport_code) REFERENCES AirportInfo(airport_code)
);



CREATE TABLE Flight (
    flightid int PRIMARY KEY,
    aircraftid INT,
    route INT,
    deptime TIMESTAMP,
    arrtime TIMESTAMP,
    avbseats INT,
    date DATE,
    status VARCHAR(50),
    duration INT,
    price int (0,2),
    FOREIGN KEY (aircraftid) REFERENCES Aircraft(aircraftid),
    FOREIGN KEY (route) REFERENCES route_info(route_id)
);



CREATE TABLE Passenger (
    passengerid SERIAL PRIMARY KEY,
    id INT,
    name VARCHAR(255),
    phone VARCHAR(20),
    email VARCHAR(255),
    passport VARCHAR(50),
    dob DATE,
    gender VARCHAR(10),
    FOREIGN KEY (id) REFERENCES Users(id)
);
alter table passenger add age int; 


CREATE TABLE Reservation (
    reservationid SERIAL PRIMARY KEY,
    passengerid INT,
    flightid INT,
    id INT,
    date DATE,
    time TIME,
    status VARCHAR(50),
    FOREIGN KEY (passengerid) REFERENCES Passenger(passengerid),
    FOREIGN KEY (flightid) REFERENCES Flight(flightid),
    FOREIGN KEY (id) REFERENCES users(id)
);

CREATE TABLE Payment (
    paymentid SERIAL PRIMARY KEY,
    reservationid INT,
    id INT,
    status VARCHAR(50),
    amount DECIMAL(10,2),
    date DATE,
    time TIME,
    FOREIGN KEY (reservationid) REFERENCES Reservation(reservationid),
    FOREIGN KEY (id) REFERENCES Users(id)
);


CREATE TABLE Employee (
    employeeid SERIAL PRIMARY KEY,
    managerid INT,
    name VARCHAR(255),
    type VARCHAR(50),
    email VARCHAR(255),
    password VARCHAR(255)
);
alter table employee add gender varchar(255); 

create table salary_crew(
	pk serial primary key,
	paid_date Date,
	employeeid int ,
 	FOREIGN KEY (employeeid) REFERENCES Employee(employeeid)

)

CREATE TABLE TicketInfo (
    ticket_id SERIAL PRIMARY KEY,
    passenger_id INT,
    flight_id INT,
    class_type VARCHAR(50),
    seat_number VARCHAR(10),
    meal_preference VARCHAR(50),
    special_requests VARCHAR(255),
    baggage_allowance INT,
    FOREIGN KEY (passenger_id) REFERENCES Passenger(passengerid),
    FOREIGN KEY (flight_id) REFERENCES Flight(flightid)
);
CREATE TABLE PilotAssignment (
    AssignmentID SERIAL PRIMARY KEY,
    employeeid INT,
    flightid INT,
    FOREIGN KEY (employeeid) REFERENCES Employee(employeeid),
    FOREIGN KEY (flightid) REFERENCES Flight(flightid)
);
CREATE TABLE CrewAssignment (
    AssignmentID SERIAL PRIMARY KEY,
    employeeid INT,
    flightid INT,
    FOREIGN KEY (employeeid) REFERENCES Employee(employeeid),
    FOREIGN KEY (flightid) REFERENCES Flight(flightid)
);
CREATE TABLE EngineerAssignment (
    AssignmentID SERIAL PRIMARY KEY,
    employeeid INT,
    aircraftid INT,
    AssignmentDate DATE,
    CompletionDate DATE,
    AssignmentStatus VARCHAR(50),
    FOREIGN KEY (employeeid) REFERENCES Employee(employeeid),
    FOREIGN KEY (aircraftid) REFERENCES Aircraft(aircraftid)
);
CREATE TABLE MaintenanceSchedule (
    schedule_id SERIAL PRIMARY KEY,
    aircraft_id INT,
    maintenance_type VARCHAR(50),
    start_date DATE,
    end_date DATE,
    estimated_cost DECIMAL(10,2),
    assigned_engineer_id INT,
    status VARCHAR(50),
    FOREIGN KEY (aircraft_id) REFERENCES Aircraft(aircraftid),
    FOREIGN KEY (assigned_engineer_id) REFERENCES Employee(employeeid)
);
CREATE TABLE crew_info (
    crew_id SERIAL PRIMARY KEY,
    employee_id INT,
    name VARCHAR(255),
    type VARCHAR(50),
    email VARCHAR(255),
    phone VARCHAR(20),
    address VARCHAR(255),
    certification VARCHAR(255),
    experience_years INT,
    FOREIGN KEY (employee_id) REFERENCES Employee(employeeid)
);

alter table crew_info add salary int ;

CREATE OR REPLACE PROCEDURE insert_dummy_salary_crew()
LANGUAGE plpgsql
AS $procedure$
DECLARE
    i INT := 1;
    employee_id INT;
    paid_date DATE;
BEGIN
    WHILE i < 3000 LOOP
        SELECT employeeid INTO employee_id 
        FROM employee 
--         WHERE employee.employeeid = crew_info.employee_id
        ORDER BY random() 
        LIMIT 1;

        paid_date := '2024-03-01'::DATE + (random() * (now() - '2024-03-01'::DATE));

        INSERT INTO salary_crew (employeeid, paid_date)
        VALUES (employee_id, paid_date);

        i := i + 1;
    END LOOP;
END;
$procedure$;

call insert_dummy_salary_crew();

CREATE OR REPLACE PROCEDURE insert_dummy_crew_info_data()
LANGUAGE plpgsql
AS $procedure$
DECLARE
    i INT := 1;
    employee_id INT;
    salary INT;
    employee_name VARCHAR(255);
    employee_email VARCHAR(255);
    employee_type VARCHAR(50);
    employee_phone VARCHAR(20);   
    employee_address VARCHAR(255);
    employee_certification VARCHAR(255);
    employee_experience_years INT;
BEGIN
    WHILE i <= 999 LOOP
        SELECT employeeid INTO employee_id FROM employee ORDER BY random() LIMIT 1;
        
        -- Get employee details from the Employee table
        SELECT name, email, type INTO employee_name, employee_email, employee_type
        FROM Employee 
        WHERE employeeid = employee_id;

        salary := floor(random() * 1001);
        employee_phone := '+1234567890' || i;
        employee_address := 'Address ' || i;
        employee_certification := 'Certification ' || i;
        employee_experience_years := floor(random() * 20);

        -- Insert the generated values into the crew_info table
        INSERT INTO crew_info (employee_id, name, email, type, phone, address, certification, experience_years, salary)
        VALUES (
            employee_id,
            CASE WHEN random() <= 0.75 THEN employee_name ELSE NULL END,
            CASE WHEN random() <= 0.75 THEN employee_email ELSE NULL END,
            CASE WHEN random() <= 0.75 THEN employee_type ELSE NULL END,
            CASE WHEN random() <= 0.75 THEN employee_phone ELSE NULL END,
            CASE WHEN random() <= 0.75 THEN employee_address ELSE NULL END,
            CASE WHEN random() <= 0.75 THEN employee_certification ELSE NULL END,
            CASE WHEN random() <= 0.75 THEN employee_experience_years ELSE NULL END,
            CASE WHEN random() <= 0.75 THEN salary ELSE NULL END
        );
        
        i := i + 1;
    END LOOP;

END;
$procedure$;

drop procedure insert_dummy_crew_info_data();
call insert_dummy_crew_info_data();
drop table crew_info;
select * from crew_info;



CREATE OR REPLACE PROCEDURE insert_dummy_maintenance_schedule_data()
LANGUAGE plpgsql
AS $procedure$
DECLARE
    i INT := 1;
	asi_id int;
    aircraft_id INT;
    maintenance_types VARCHAR[] := ARRAY['Routine Check', 'Engine Overhaul', 'Avionics Inspection', 'Interior Refurbishment'];
    start_date DATE;
    end_date DATE;
    estimated_cost DECIMAL(10,2);
    assigned_engineer_id INT;
    status VARCHAR(50);
BEGIN
    WHILE i <= 800 LOOP
        -- Generate random values for each column
        select aircraftid from aircraft into aircraft_id order by random() limit 1;
		select assignmentid from engineerassignment into asi_id where aircraft_id=aircraftid order by random() limit 1;

		 SELECT employeeid INTO assigned_engineer_id FROM engineerassignment WHERE assignmentid=asi_id  ;
select assignmentdate from engineerassignment into start_date where assignmentid=asi_id;
select completiondate from engineerassignment into end_date  where assignmentid=asi_id;
        estimated_cost := (floor(random() * 10000) + 1000.00)::numeric(10, 2);
        -- Get a random engineer's employeeid from the Employee table
        status := CASE WHEN random() < 0.5 THEN 'Completed' ELSE 'Pending' END;

        -- Insert the generated values into the MaintenanceSchedule table
        INSERT INTO MaintenanceSchedule (aircraft_id, maintenance_type, start_date, end_date, estimated_cost, assigned_engineer_id, status)
        VALUES (
		aircraft_id ,
			case when floor(random()*100)+1 <=25 then null else maintenance_types[floor(random() * array_length(maintenance_types, 1)) + 1]end,
			case when floor(random()*100)+1 <=25 then null else start_date end,
				case when floor(random()*100)+1 <=25 then null else end_date end,
				case when floor(random()*100)+1 <=25 then null else estimated_cost end,
assigned_engineer_id,				
			case when floor(random()*100)+1 <=25 then null else status end
		);

        i := i + 1;
    END LOOP;
END;
$procedure$;

call insert_dummy_maintenance_schedule_data();
select * from maintenanceschedule;

CREATE OR REPLACE PROCEDURE insert_dummy_engineer_assignment_data()
LANGUAGE plpgsql
AS $procedure$
DECLARE
    i INT := 1;
    employee_id INT;
    aircraft_id INT;
    assignment_date DATE;
    completion_date DATE;
    assignment_status VARCHAR(50);
BEGIN
    WHILE i <= 999 LOOP
        -- Generate random values for each column
        -- Get a random engineer's employeeid from the Employee table
        SELECT employeeid INTO employee_id FROM Employee WHERE type = 'engineer' ORDER BY random() LIMIT 1;
        select aircraftid from aircraft into aircraft_id order by random() limit 1;
        assignment_date := '2024-03-01'::DATE + (random() * (now() - '2024-03-01'::DATE));
        completion_date := '2024-03-01'::DATE + (random() * (now() - '2024-03-01'::DATE));
        assignment_status := CASE WHEN random() < 0.5 THEN 'Completed' ELSE 'Pending' END;

        -- Insert the generated values into the EngineerAssignment table
        INSERT INTO EngineerAssignment (employeeid, aircraftid, AssignmentDate, CompletionDate, AssignmentStatus)
        VALUES (
			 employee_id ,
aircraft_id,
			case when floor(random()*100)+1 <=25 then null else assignment_date end,
				case when floor(random()*100)+1 <=25 then null else completion_date end,
				case when floor(random()*100)+1 <=25 then null else assignment_status end
		);

        i := i + 1;
    END LOOP;
END;
$procedure$;
call insert_dummy_engineer_assignment_data();
select    * from engineerassignment;



CREATE OR REPLACE PROCEDURE insert_dummy_crew_assignment_data()--cabin crew
LANGUAGE plpgsql
AS $procedure$
DECLARE
    i INT := 1;
    employee_id INT;
    flight_id INT;
BEGIN
    WHILE i <= 999 LOOP
        -- Generate random values for employeeid and flightid
        -- Get a random crew member's (non-pilot) employeeid from the Employee table
        SELECT employeeid INTO employee_id FROM Employee WHERE type = 'cabin crew' ORDER BY random() LIMIT 1;
        select flightid from flight into flight_id order by random() limit 1;

        -- Insert the generated values into the CrewAssignment table
        INSERT INTO CrewAssignment (employeeid, flightid)
		
        VALUES (
		employee_id ,
flight_id
		);

        i := i + 1;
    END LOOP;
END;
$procedure$;
call insert_dummy_crew_assignment_data();
select * from crewassignment;

CREATE OR REPLACE PROCEDURE insert_dummy_pilot_assignment_data()
LANGUAGE plpgsql
AS $procedure$
DECLARE
    i INT := 1;
    employee_id INT;
    flight_id INT;
BEGIN
    WHILE i <= 999 LOOP
        -- Generate random values for employeeid (pilot) and flightid
        -- Get a random pilot id from the Employee table
        SELECT employeeid INTO employee_id FROM Employee WHERE type = 'pilot' ORDER BY random() LIMIT 1;
        select flightid from flight into flight_id order by random() limit 1;

        -- Insert the generated values into the PilotAssignment table
        INSERT INTO PilotAssignment (employeeid, flightid)
		
        VALUES (
			  employee_id ,
flight_id			
		);

        i := i + 1;
    END LOOP;
END;
$procedure$;
call insert_dummy_pilot_assignment_data();
select count(*) from pilotassignment ;

CREATE OR REPLACE PROCEDURE insert_dummy_ticket_info_data()
LANGUAGE plpgsql
AS $procedure$
DECLARE
    i INT := 1;
	res_id int;
    passenger_id INT;
    flight_id INT;
    class_types VARCHAR[] := ARRAY['Economy', 'Business', 'First'];
    seat_letters VARCHAR[] := ARRAY['A', 'B', 'C', 'D', 'E'];
    meal_preferences VARCHAR[] := ARRAY['Vegetarian', 'Non-Vegetarian', 'Vegan', 'No Preference'];
    special_requests VARCHAR[] := ARRAY['Wheelchair assistance', 'Extra legroom', 'Child meal', 'No special requests'];
    baggage_allowance INT;
BEGIN
    WHILE i <= 5000 LOOP
        -- Generate random values for each column
		       select reservationid from reservation into res_id  order by random() limit 1;
       select passengerid from reservation into passenger_id where reservationid=res_id ;
               select flightid from reservation into flight_id where reservationid=res_id ;
        baggage_allowance := floor(random() * 30) + 5; -- Random baggage allowance between 5 and 35 kg

        -- Select random values from arrays
        INSERT INTO TicketInfo (passenger_id, flight_id, class_type, seat_number, meal_preference, special_requests, baggage_allowance)
        VALUES (
			 passenger_id ,
flight_id,
			case when floor(random()*100)+1 <=25 then null else class_types[floor(random() * array_length(class_types, 1)) + 1] end,
			case when floor(random()*100)+1 <=25 then null else seat_letters[floor(random() * array_length(seat_letters, 1)) + 1] || (floor(random() * 20) + 1)::TEXT end,
			case when floor(random()*100)+1 <=25 then null else meal_preferences[floor(random() * array_length(meal_preferences, 1)) + 1] end,
			case when floor(random()*100)+1 <=25 then null else  special_requests[floor(random() * array_length(special_requests, 1)) + 1] end,
			case when floor(random()*100)+1 <=25 then null else baggage_allowance end
        );

        i := i + 1;
    END LOOP;
END;
$procedure$;
 call insert_dummy_ticket_info_data();
 select * from ticketinfo ;

CREATE OR REPLACE PROCEDURE insert_dummy_employee_data()
LANGUAGE plpgsql
AS $procedure$
DECLARE
    i INT := 2;
    manager_id INT;
    employee_name VARCHAR(255);
    employee_type VARCHAR(50);
    employee_email VARCHAR(255);
    employee_password VARCHAR(255);
	employee_gender varchar(255);
BEGIN
    WHILE i <= 1000 LOOP
        -- Generate random values for each column
        manager_id := 1;
        employee_name := 'Employee' || i;
        -- Randomly assign employee types: pilot, cabin crew, or engineer
        CASE
            WHEN random() < 0.3 THEN
                employee_type := 'pilot';
            WHEN random() < 0.6 THEN
                employee_type := 'cabin crew';
            ELSE
                employee_type := 'engineer';
        END CASE;
        employee_email := 'employee' || i || '@example.com';
        employee_password := 'password' || i;

        -- Insert the generated values into the Employee table
        INSERT INTO Employee (managerid, name, type, email, password,gender)
		
		values(
		  manager_id ,
			case when floor(random()*100)+1 <=25 then null else employee_name end,
			case when floor(random()*100)+1 <=25 then null else employee_type end,
			case when floor(random()*100)+1 <=25 then null else employee_email end,
			case when floor(random()*100)+1 <=25 then null else employee_password end,
						case when floor(random()*100)+1 <=25 then null else employee_gender end	
		);
        
        i := i + 1;
    END LOOP;
END;
$procedure$;
UPDATE employee
SET gender = CASE 
    WHEN employeeid >= 800 AND employeeid <= 1499 THEN 
        CASE 
            WHEN random() < 0.5 THEN 'Male'
            ELSE 'Female'
        END
    ELSE NULL -- Handle other cases as needed
END;
call insert_dummy_employee_data();
select * from employee;

CREATE OR REPLACE PROCEDURE insert_dummy_payment_data()
LANGUAGE plpgsql
AS $procedure$
DECLARE
    i INT := 1;
    reservation_id INT;
    user_id INT;
    pstatus VARCHAR(50);
    pamount DECIMAL(10,2);
	flight_id int;
    pd DATE;
    pt TIME;
BEGIN
    WHILE i <= 5000 LOOP
        -- Generate random values for each column
		select id from users into user_id order by random() limit 1;
		select reservationid from reservation into reservation_id where id=user_id order by random() limit 1;
        pstatus := case when  exists (select 1 from reservation where reservation.status='Confirmed' and reservationid=reservation_id)  
		THEN  'done' ELSE  'pending'
		END ;
		select flightid from reservation into flight_id where reservation_id=reservationid ;
        select price from flight into pamount where flightid=flight_id order by random() limit 1;
        pd := '2024-03-01'::DATE + (random() * (now() - '2024-03-01'::DATE));
        pt := '12:00:00'::TIME + (random() * '06:00:00'::interval);

        -- Insert the generated values into the Payment table
        INSERT INTO Payment (reservationid, id, status, amount, date, time)
		
		values(
		reservation_id ,
user_id,
			case when floor(random()*100)+1 <=25 then null else pstatus end,
			case when floor(random()*100)+1 <=25 then null else pamount end,
			case when floor(random()*100)+1 <=25 then null else pd end,
			case when floor(random()*100)+1 <=25 then null else pt end
		
		);
		
       

        i := i + 1;
    END LOOP;
END;
$procedure$;
call insert_dummy_payment_data();
select * from payment;



CREATE OR REPLACE PROCEDURE insert_dummy_reservation_data()
LANGUAGE plpgsql
AS $procedure$
DECLARE
    i INT := 1;
    passenger_id INT;
    flight_id INT;
    user_id INT;
    reservation_date DATE;
    reservation_time TIME;
    reservation_status VARCHAR(50);
BEGIN
    WHILE i <= 5000   loop
        -- Generate random values for each column
		select id from users into user_id order by random() limit 1;
        select passengerid from passenger into passenger_id where id=user_id order by random() limit 1;
		select flightid from flight into flight_id order by random() limit 1;
        reservation_date := '2024-03-01'::DATE + (random() * (now() - '2024-03-01'::DATE));
        reservation_time := '12:00:00'::TIME + (random() * '06:00:00'::interval);
        reservation_status := CASE WHEN random() < 0.5 THEN 'Confirmed' ELSE 'Pending' END;

        -- Insert the generated values into the Reservation table
        INSERT INTO Reservation (passengerid, flightid, id, date, time, status)
		values (
		 passenger_id ,
flight_id,
			 user_id ,
			case when floor(random()*100)+1 <=25 then null else reservation_date end,
			case when floor(random()*100)+1 <=25 then null else reservation_time end,
			case when floor(random()*100)+1 <=25 then null else reservation_status end);
		
				

        i := i + 1;
    END LOOP;
END;
$procedure$;

call insert_dummy_reservation_data();
select * from reservation;

CREATE OR REPLACE PROCEDURE insert_dummy_passenger_data()
LANGUAGE plpgsql
AS $procedure$
DECLARE
    i INT := 1;
    user_id INT;
    passenger_name VARCHAR(255);
    passenger_phone VARCHAR(20);
    passenger_email VARCHAR(255);
    passenger_passport VARCHAR(50);
    passenger_dob DATE;
    passenger_gender VARCHAR(10);
	passenger_age int;
BEGIN
    WHILE i <= 5000  loop
        -- Generate random values for each column
        select id from users into user_id order by random() limit 1;
        passenger_name := 'Passenger' || i;
        passenger_phone := '+1234567890'||i; -- Example phone number
        passenger_email := 'passenger' || i || '@example.com';
        passenger_passport := 'PASS' || LPAD(CAST(i AS TEXT), 4, '0'); -- Example passport number
        passenger_dob := '1980-01-01'::DATE + (random() * (now() - '1980-01-01'::DATE));
        passenger_gender := CASE WHEN random() < 0.5 THEN 'Male' ELSE 'Female' END;
select EXTRACT(YEAR FROM AGE(passenger_dob)) into  passenger_age    ;    
-- Insert the generated values into the Passenger table
        INSERT INTO Passenger (id, name, phone, email, passport, dob, gender,age)
		Values (
		user_id ,
		case when floor(random()*100)+1 <=25 then null else passenger_name end,
		case when floor(random()*100)+1 <=25 then null else passenger_phone end,
		case when floor(random()*100)+1 <=25 then null else passenger_email end,
		case when floor(random()*100)+1 <=25 then null else passenger_passport end,
		case when floor(random()*100)+1 <=25 then null else passenger_dob end,
		case when floor(random()*100)+1 <=25 then null else passenger_gender end,
		case when floor(random()*100)+1 <=25 then null else passenger_age end
			
);

        i := i + 1;
    END LOOP;
END;
$procedure$;

--drop table passenger;
call insert_dummy_passenger_data();
select * from passenger;

CREATE OR REPLACE PROCEDURE insert_dummy_flight_data()
LANGUAGE plpgsql
AS $procedure$
DECLARE
    i INT := 1;
	fid int ;
    aircraft_id INT;
    route_id INT; 
    departure_time TIMESTAMP;
    arrival_time TIMESTAMP;
    available_seats INT;
    flight_date DATE;
    flight_status VARCHAR(50);
    flight_duration INT;
    flight_price DECIMAL(10,2);
BEGIN
    WHILE i <= 1000 LOOP
        -- Generate random values for each column
		select max(flightid)+1  into fid from flight;
        select get_active_aircraft_id() into aircraft_id;
        SELECT route_info.route_id FROM route_info into route_id ORDER BY random() LIMIT 1;
        departure_time := '2024-03-01'::timestamp + (random() * (now() - '2024-03-01'::timestamp));
        arrival_time := departure_time + (floor(random() * 6) || ' hours')::interval;
    select totalseats from aircraft into available_seats where aircraftid=aircraft_id;
        flight_date := departure_time::date;
        flight_status := CASE WHEN random() < 0.5 THEN 'Scheduled' ELSE 'Delayed' END;
        flight_duration := floor(random() * 300) + 60;
        flight_price := (floor(random() * 500) + 50.00)::numeric(10, 2);

        -- Insert the generated values into the Flight table
        INSERT INTO Flight (flightid,aircraftid, route, deptime, arrtime, avbseats, date, status, duration, price)
        VALUES ( fid,
				aircraft_id ,
				route_id ,
				case when floor(random()*100)+1 <=25 then null else departure_time end,
				case when floor(random()*100)+1 <=25 then null else arrival_time end,
				case when floor(random()*100)+1 <=25 then null else floor(random()*available_seats)+1 end,
				case when floor(random()*100)+1 <=25 then null else flight_date end,
				case when floor(random()*100)+1 <=25 then null else flight_status end,
				case when floor(random()*100)+1 <=25 then null else flight_duration end,
				case when floor(random()*100)+1 <=25 then null else flight_price end);
        i := i + 1;
    END LOOP;
END;
$procedure$;

CREATE OR REPLACE FUNCTION get_active_aircraft_id() RETURNS INTEGER AS $$
DECLARE
    random_id INTEGER;
BEGIN
    
        select aircraftid from aircraft into random_id where astatus ='Active' order by random() limit 1;
		return random_id;
END;
$$ LANGUAGE PLPGSQL;

drop procedure insert_dummy_flight_data();
call insert_dummy_flight_data();
select * from flight;

CREATE OR REPLACE PROCEDURE insert_dummy_route_info()
LANGUAGE plpgsql
AS $procedure$
DECLARE
    i INT := 1;
    missing_chance INT;
	 missing_columns INT[];
    dep_airport_code VARCHAR(5);
	arr_airport_code varchar(5);
BEGIN
    WHILE i <= 100 LOOP
        -- Generate missing chance (0-100) for each column
        missing_chance := floor(random() * 100) + 1;
dep_airport_code:='A'||floor(random() * 500) + 1;
arr_airport_code:='A'||floor(random() * 500) + 1;
        -- Generate random values for each column
        -- If missing_chance is less than or equal to 25, introduce missing value for individual columns
        -- Primary and foreign key columns should not be null
        --CASE 
           -- WHEN missing_chance <= 25 THEN
                INSERT INTO route_info (dep_airport_code, arr_airport_code, distance, avg_duration, frequency, weather_conditions)
                VALUES (
                     dep_airport_code ,
                    arr_airport_code ,
                    CASE WHEN floor(random() * 100) + 1 <= 25 THEN NULL ELSE floor(random() * 5000) + 100.00 END,
                    CASE WHEN floor(random() * 100) + 1 <= 25 THEN NULL ELSE floor(random() * 480) + 20 END,
                    CASE WHEN floor(random() * 100) + 1 <= 25 THEN NULL ELSE floor(random() * 10) + 1 END,
                    CASE WHEN floor(random() * 100) + 1 <= 25 THEN NULL ELSE CASE WHEN random() < 0.5 THEN 'Good' ELSE 'Bad' END END
                );
         

        i := i + 1;
    END LOOP;
END;
$procedure$;
drop procedure insert_dummy_route_info();
call insert_dummy_route_info();
select * from route_info;



CREATE OR REPLACE PROCEDURE insert_dummy_airport_info()
LANGUAGE plpgsql
AS $procedure$
DECLARE
    i INT := 1;
    missing_chance INT;
    missing_columns INT[];
BEGIN
    WHILE i <= 500 LOOP
        -- Initialize the array for missing columns
        missing_columns := ARRAY[0, 0, 0, 0, 0, 0, 0, 0, 0];
        
        -- Determine which columns will have missing values
        FOR j IN 1..9 LOOP
            -- Generate missing chance (0-100) for each column
            missing_chance := floor(random() * 100) + 1;
            IF missing_chance <= 25 THEN
                missing_columns[j] := 1;
            END IF;
        END LOOP;

        -- Generate random values for each column
        INSERT INTO AirportInfo (
            airport_code, 
            airport_name, 
            city, 
            country, 
            latitude, 
            longitude, 
            elevation, 
            timezone, 
            is_international
        )
        VALUES (
            'A'||i,
            CASE WHEN missing_columns[2] = 1 THEN NULL ELSE 'Airport' || i END,
            CASE WHEN missing_columns[3] = 1 THEN NULL ELSE 'City' || i END,
            CASE WHEN missing_columns[4] = 1 THEN NULL ELSE 'Country' || i END,
            CASE WHEN missing_columns[5] = 1 THEN NULL ELSE (random() * 180::DECIMAL - 90)::DECIMAL(10,6) END,
            CASE WHEN missing_columns[6] = 1 THEN NULL ELSE (random() * 360::DECIMAL - 180)::DECIMAL(10,6) END,
            CASE WHEN missing_columns[7] = 1 THEN NULL ELSE floor(random() * 5000) + 1 END,
            CASE WHEN missing_columns[8] = 1 THEN NULL ELSE 'Timezone' || i END,
            CASE WHEN missing_columns[9] = 1 THEN NULL ELSE (random() < 0.5) END
        );

        i := i + 1;
    END LOOP;
END;
$procedure$;


drop procedure insert_dummy_airport_info()

        call insert_dummy_airport_info(); -- Call the function
        
select * from airportinfo;



CREATE OR REPLACE FUNCTION insert_dummy_aircraft()
RETURNS VOID AS
$$
DECLARE
    i INT := 1;
    missing_chance INT;
    column_to_null INT;
BEGIN
    WHILE i <= 50 LOOP
        -- Generate missing chance (0-100)
        missing_chance := floor(random() * 20) + 1;
        -- Generate random column number to set to NULL (1-4)
        column_to_null := floor(random() * 4) + 1;

        -- Generate random values for each column
        CASE 
            WHEN missing_chance <= 5 THEN
                -- Set a random column to NULL
                INSERT INTO Aircraft (aname, totalseats, astatus, manufacturer)
                VALUES (
                    CASE WHEN column_to_null = 1 THEN NULL ELSE 'Aircraft' || i END,
                    CASE WHEN column_to_null = 2 THEN NULL ELSE floor(random() * 300) + 100 END,
                    CASE WHEN column_to_null = 3 THEN NULL ELSE CASE WHEN random() < 0.5 THEN 'Active' ELSE 'Inactive' END END,
                    CASE WHEN column_to_null = 4 THEN NULL ELSE CASE WHEN random() < 0.5 THEN 'Airbus' ELSE 'Boeing' END END
                );
            WHEN missing_chance <= 10 THEN
                -- Duplicate a row in the Aircraft table
                INSERT INTO Aircraft (aname, totalseats, astatus, manufacturer)
                SELECT aname, totalseats, astatus, manufacturer FROM Aircraft WHERE aircraftid = i;
            ELSE
                -- Generate random values for all columns
                INSERT INTO Aircraft (aname, totalseats, astatus, manufacturer)
                VALUES ('Aircraft' || i, floor(random() * 300) + 100,
                        CASE WHEN random() < 0.5 THEN 'Active' ELSE 'Inactive' END, 
                        CASE WHEN random() < 0.5 THEN 'Airbus' ELSE 'Boeing' END);
        END CASE;

        i := i + 1;
    END LOOP;
END;
$$
LANGUAGE plpgsql;


select insert_dummy_aircraft();
select * from aircraft;


CREATE OR REPLACE PROCEDURE insert_dummy_users()
LANGUAGE plpgsql
AS $procedure$
DECLARE
    i INT := 1;
BEGIN
    WHILE i <= 1000 LOOP
        INSERT INTO Users (name, isAdmin, email, password)
        VALUES ('User' || i, FALSE, 'user' || i || '@example.com', 'password' || i);
        i := i + 1;
    END LOOP;
END;
$procedure$;
--drop procedure insert_dummy_users();
--select * from Users;

call insert_dummy_users();


