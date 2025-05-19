-- CREATE SCHEMA
CREATE SCHEMA TRAINING_WORKSHOP.SPAIN_WEATHER;

-- CREATE TABLE FOR SPAIN BEACH MASTER REFERENCE DATA
CREATE TABLE TRAINING_WORKSHOP.SPAIN_WEATHER.RAW_BEACH_LOOKUP (
    ID_PLAYA NUMBER(38,0),
    NOMBRE_PLAYA VARCHAR ,
    ID_PROVINCIA NUMBER(38,0),
    NOMBRE_PROVINCIA VARCHAR,
    ID_MUNICIPIO NUMBER(38,0),
    NOMBRE_MUNICIPIO VARCHAR,
    LATITUD VARCHAR,
    LONGITUD VARCHAR
);

-- CREATE FILE FORMAT TO LOAD DATA TO THE TABLE
CREATE OR REPLACE FILE FORMAT TRAINING_WORKSHOP.SPAIN_WEATHER.BEACH_CSV
	TYPE=CSV
    FIELD_DELIMITER=';'
    TRIM_SPACE=TRUE
    SKIP_HEADER = 1
    PARSE_HEADER = FALSE
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE
    REPLACE_INVALID_CHARACTERS=TRUE
    DATE_FORMAT=AUTO
    TIME_FORMAT=AUTO
    TIMESTAMP_FORMAT=AUTO
    ENCODING = 'ISO88591';

-- CREATE A STAGE TO LOAD THE FILE INTO
CREATE OR REPLACE STAGE TRAINING_WORKSHOP.SPAIN_WEATHER.AEMET_FILES
    FILE_FORMAT = (FORMAT_NAME = 'TRAINING_WORKSHOP.SPAIN_WEATHER.BEACH_CSV');

LOAD INTO

-- TEST DROP AND UNDROP
DROP TABLE TRAINING_WORKSHOP.SPAIN_WEATHER.RAW_BEACH_LOOKUP;
UNDROP TABLE TRAINING_WORKSHOP.SPAIN_WEATHER.RAW_BEACH_LOOKUP;

-- USING EXCLUDE
SELECT * EXCLUDE (BEACH_ID, MUNICIPALITY_ID) FROM TRAINING_WORKSHOP.SPAIN_WEATHER.RAW_BEACH_LOOKUP;

-- TEST SNOWFLAKE CORTEX COMPLETE FUNCTIONS
SELECT
    *,
    SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', CONCAT('calculate and return the numeric latitude based on the following latitude field in degress, minutes and seconds. Remember, I dont need any explanation nor text. Just the final numeric latitude in your response: ', raw_lat, ', for context, the point is located in Spain, in the province of ',province, ' and the municipality of ', municipality)) as generated_lat,
    SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', CONCAT('calculate and return the numeric longitude based on the following longitude field in degress, minutes and seconds. Remember, I dont need any explanation nor text. Just the final numeric longitude in your response: ', raw_long, ', for context, the point is located in Spain, in the province of ',province, ' and the municipality of ', municipality)) as generated_long
FROM TRAINING_WORKSHOP.SPAIN_WEATHER.RAW_BEACH_LOOKUP LIMIT 10;

-- CREATE A TABLE USING THOSE FUNCTIONS AS NEW COLUMNS
CREATE OR REPLACE TABLE TRAINING_WORKSHOP.SPAIN_WEATHER.RAW_BEACH_LAT_LONG
AS SELECT
       BEACH_ID,
       SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', CONCAT('calculate and return the numeric latitude based on the following latitude field in degress, minutes and seconds. Remember, I dont need any explanation nor text. Just the final numeric latitude in your response: ', raw_lat, ', for context, the point is located in Spain, in the province of ',province, ' and the municipality of ', municipality)) as generated_lat,
       SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', CONCAT('calculate and return the numeric longitude based on the following longitude field in degress, minutes and seconds. Remember, I dont need any explanation nor text. Just the final numeric longitude in your response: ', raw_long, ', for context, the point is located in Spain, in the province of ',province, ' and the municipality of ', municipality)) as generated_long
FROM TRAINING_WORKSHOP.SPAIN_WEATHER.RAW_BEACH_LOOKUP;

-- CHECK DATA
SELECT * FROM TRAINING_WORKSHOP.SPAIN_WEATHER.RAW_BEACH_LAT_LONG;

/*
CREATE A DYNAMIC TABLE JOINING BOTH THE RAW_BEACH_LOOKUP AND
THE PREVIOUSLY CREATED RAW_BEACH_LAT_LONG
 */

CREATE OR REPLACE DYNAMIC TABLE TIL_WORKSHOP.SPAIN_WEATHER.SILVER_BEACH_LOOKUP
    WAREHOUSE = COMPUTE_XS
    INITIALIZE = ON_CREATE
    TARGET_LAG = 'DOWNSTREAM'
    AS SELECT
        t1.*, t2.generated_lat, t2.generated_long
        FROM TRAINING_WORKSHOP.SPAIN_WEATHER.RAW_BEACH_LOOKUP t1
        LEFT JOIN TRAINING_WORKSHOP.SPAIN_WEATHER.RAW_BEACH_LAT_LONG t2
        ON t1.beach_id = t2.beach_id
;

/*
CREATE A SECOND DYNAMIC TABLE CLEANING THE PREVIOUS ONE
TO BE USED AS A GOLD MASTER TABLE FOR BEACH REFERENCE AND LOOKUP
 */

CREATE OR REPLACE DYNAMIC TABLE TRAINING_WORKSHOP.SPAIN_WEATHER.GOLD_BEACH_MASTER_TABLE
    WAREHOUSE = COMPUTE_XS
    INITIALIZE = ON_CREATE
    TARGET_LAG = '30 days'
    AS SELECT
        BEACH_ID,
        BEACH_NAME,
        PROVINCE_ID,
        PROVINCE,
        MUNICIPALITY_ID,
        MUNICIPALITY,
        TO_DOUBLE(GENERATED_LAT) AS LATITUDE,
        TO_DOUBLE(GENERATED_LONG) AS LONGITUDE,
        ST_MAKEPOINT(TO_DOUBLE(GENERATED_LONG),TO_DOUBLE(GENERATED_LAT)) AS POINT
        FROM TRAINING_WORKSHOP.SPAIN_WEATHER.SILVER_BEACH_LOOKUP
;

-- REVIEW DATA
SELECT * EXCLUDE (POINT) FROM TIL_WORKSHOP.SPAIN_WEATHER.GOLD_BEACH_MASTER_TABLE;

-- REVIEW CORTEX SEARCH USAGE AND CREDITS
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY ORDER BY START_TIME DESC;;
