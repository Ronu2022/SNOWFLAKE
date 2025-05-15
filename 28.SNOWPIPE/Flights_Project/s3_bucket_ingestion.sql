CREATE OR REPLACE DATABASE flights_Delays_Cancellation;
CREATE OR REPLACE SCHEMA s3_bucket;
CREATE OR REPLACE SCHEMA FILE_FOMRAT_SCHEMA;
CREATE OR REPLACE SCHEMA STAGE_SCHEMA;
CREATE OR REPLACE SCHEMA TABLE_SCHEMA;
CREATE OR REPLACE SCHEMA pipe_schema;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------

// Storage Integration
CREATE OR REPLACE STORAGE INTEGRATION s3_int_flight
TYPE = external_stage
storage_provider = s3
ENABLED = TRUE
storage_aws_role_arn = 'arn:aws:iam::481665128591:role/flightsdata'
storage_allowed_locations = ('s3://flightscancellanddelays/airlines/','s3://flightscancellanddelays/airpots/','s3://flightscancellanddelays/flights/')
comment = 'Storage_integration_creation';

// DESC

DESC  integration s3_int_flight; -- arn:aws:iam::077380691495:user/47wz0000-s -- Policy Updated.
--arn:aws:iam::077380691495:user/47wz0000-s

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// FILE FORMAT

CREATE OR REPLACE FILE FORMAT flights_Delays_Cancellation.FILE_FOMRAT_SCHEMA.flight_csv_format
TYPE = 'csv'
Field_delimiter = ','
skip_header = 1
empty_field_as_null = True;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// STAGING

-- For flights
CREATE OR REPLACE STAGE flights_Delays_Cancellation.STAGE_SCHEMA.stage_flights
URL = 's3://flightscancellanddelays/flights/'
STORAGE_INTEGRATION = s3_int_flight
FILE_FORMAT = flights_Delays_Cancellation.FILE_FOMRAT_SCHEMA.flight_csv_format;

LIST @flights_Delays_Cancellation.STAGE_SCHEMA.stage_flights;

-- For airlines:

CREATE OR REPLACE STAGE flights_Delays_Cancellation.STAGE_SCHEMA.stage_airlines
URL = 's3://flightscancellanddelays/airlines/'
STORAGE_INTEGRATION = s3_int_flight
FILE_FORMAT = flights_Delays_Cancellation.FILE_FOMRAT_SCHEMA.flight_csv_format;

LIST @flights_Delays_Cancellation.STAGE_SCHEMA.stage_airlines;

-- For airports:

CREATE OR REPLACE STAGE flights_Delays_Cancellation.STAGE_SCHEMA.stage_airports
URL = 's3://flightscancellanddelays/airpots/'
STORAGE_INTEGRATION = s3_int_flight
FILE_FORMAT = flights_Delays_Cancellation.FILE_FOMRAT_SCHEMA.flight_csv_format;

LIST @flights_Delays_Cancellation.STAGE_SCHEMA.stage_airports;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------

//  Checking for Files:

SELECT $1, $2
FROM @flights_Delays_Cancellation.STAGE_SCHEMA.stage_flights/flights.csv limit 10
(FILE_FORMAT =>  flights_Delays_Cancellation.FILE_FOMRAT_SCHEMA.flight_csv_format); -- OKAY


SELECT $1, $2
FROM @flights_Delays_Cancellation.STAGE_SCHEMA.stage_airlines/airlines.csv
(FILE_FORMAT =>  flights_Delays_Cancellation.FILE_FOMRAT_SCHEMA.flight_csv_format) LIMIT 10; -- okay


SELECT $1, $2, $3
FROM @flights_Delays_Cancellation.STAGE_SCHEMA.stage_airports/airports.csv
(FILE_FORMAT =>  flights_Delays_Cancellation.FILE_FOMRAT_SCHEMA.flight_csv_format) LIMIT 10; -- Okay


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
// TABLES CREATION:
-- FLights:

CREATE OR REPLACE TABLE flights_Delays_Cancellation.TABLE_SCHEMA.flights 
(
    year INT,
    month INT,
    day INT,
    day_of_week STRING,
    airline STRING,
    flight_number STRING,
    tail_number STRING,
    origin_airport STRING,
    destination_airport STRING,
    scheduled_departure STRING,
    departure_time STRING,
    departure_delay INT,
    taxi_out INT,
    wheels_off STRING,
    scheduled_time INT,
    elapsed_time INT,
    air_time INT,
    distance INT,
    wheels_on STRING,
    taxi_in INT,
    scheduled_arrival STRING,
    arrival_time STRING,
    arrival_delay INT,
    diverted INT,
    cancelled INT,
    cancellation_reason STRING,
    air_system_delay INT,
    security_delay INT,
    airline_delay INT,
    late_aircraft_delay INT,
    weather_delay INT
);


SELECT * FROM flights_Delays_Cancellation.TABLE_SCHEMA.flights; -- 0 records


-- airlines
CREATE OR REPLACE TABLE flights_Delays_Cancellation.TABLE_SCHEMA.airlines
(
    IATA_CODE STRING,
    AIRLINE STRING  

);


-- Airport

CREATE OR REPLACE TABLE flights_Delays_Cancellation.TABLE_SCHEMA.airport
(
    IATA_CODE STRING,
    AIRPORT	 STRING,
    CITY	STRING,
    STATE	STRING,
    COUNTRY	STRING,
    LATITUDE	FLOAT,
    LONGITUDE FLOAT
);

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// SNOWPIPIPE - FLIGHT:

CREATE OR REPLACE PIPE flights_Delays_Cancellation.pipe_schema.pipe_flight
AUTO_INGEST = TRUE
AS
COPY INTO flights_Delays_Cancellation.TABLE_SCHEMA.flights
FROM @flights_Delays_Cancellation.STAGE_SCHEMA.stage_flights
PATTERN = '.*flight.*';

DESC  PIPE flights_Delays_Cancellation.pipe_schema.pipe_flight;
-- notification channel : arn:aws:sqs:ap-south-1:077380691495:sf-snowpipe-AIDAREBB7UYT7MVPOGYP7-Z5W3ICfSzjFJu_9rGTKkXA

SELECT SYSTEM$PIPE_STATUS ('flights_Delays_Cancellation.pipe_schema.pipe_flight');


SELECT * FROM flights_Delays_Cancellation.TABLE_SCHEMA.flights; -- Inserted


// SNOWPIPE - airlines:

CREATE OR REPLACE PIPE flights_Delays_Cancellation.pipe_schema.pipe_airlines
AUTO_INGEST = TRUE
AS
COPY INTO flights_Delays_Cancellation.TABLE_SCHEMA.airlines
FROM @flights_Delays_Cancellation.STAGE_SCHEMA.stage_airlines
PATTERN = '.*airlines.*';

DESC PIPE flights_Delays_Cancellation.pipe_schema.pipe_airlines; -- arn:aws:sqs:ap-south-1:077380691495:sf-snowpipe-AIDAREBB7UYT7MVPOGYP7-Z5W3ICfSzjFJu_9rGTKkXA


SELECT * FROM flights_Delays_Cancellation.TABLE_SCHEMA.airlines; -- 0 records

// SNOWPIPE - airports

CREATE OR REPLACE PIPE flights_Delays_Cancellation.pipe_schema.pipe_airports
AUTO_INGEST = TRUE
AS
COPY INTO flights_Delays_Cancellation.TABLE_SCHEMA.airport
FROM @flights_Delays_Cancellation.STAGE_SCHEMA.stage_airports
PATTERN = '.*airports.*';

SELECT * FROM flights_Delays_Cancellation.TABLE_SCHEMA.airport; -- 0 records

DESC PIPE flights_Delays_Cancellation.pipe_schema.pipe_airports; -- arn:aws:sqs:ap-south-1:077380691495:sf-snowpipe-AIDAREBB7UYT7MVPOGYP7-Z5W3ICfSzjFJu_9rGTKkXA


-------------------------------------------------------------------------------------------------------------------------------------------------------------------------

// uploading file in s3 Bucket

-- Uploaded 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// Checking Pipe Status

SELECT SYSTEM$PIPE_STATUS ('flights_Delays_Cancellation.pipe_schema.pipe_airlines'); -- Okay
SELECT SYSTEM$PIPE_STATUS ('flights_Delays_Cancellation.pipe_schema.pipe_airports'); -- Okay

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// Check for Records:

SELECT * FROM flights_Delays_Cancellation.TABLE_SCHEMA.flights;  -- Done
SELECT * FROM flights_Delays_Cancellation.TABLE_SCHEMA.airlines; -- Done
SELECT * FROM flights_Delays_Cancellation.TABLE_SCHEMA.airport; -- Done

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------


// FINAL TABLES:
SELECT * FROM flights_Delays_Cancellation.TABLE_SCHEMA.flights;
SELECT * FROM flights_Delays_Cancellation.TABLE_SCHEMA.airlines; 
SELECT * FROM flights_Delays_Cancellation.TABLE_SCHEMA.airport; 

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------


DESCRIBE TABLE flights_Delays_Cancellation.TABLE_SCHEMA.airport;






