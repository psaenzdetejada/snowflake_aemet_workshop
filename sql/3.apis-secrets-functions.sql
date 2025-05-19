-- LET'S START BY SETTING UP THE CONTEXT
USE WAREHOUSE COMPUTE_XS;
USE ROLE SYSADMIN;
USE SCHEMA TRAINING_WORKSHOP.SPAIN_WEATHER;

-- NOW LET'S CREATE A NETWORK RULE TO BE ABLE TO USE AN EXTERNAL API
CREATE OR REPLACE NETWORK RULE AEMET_API_ACCESS
    TYPE = HOST_PORT
    VALUE_LIST = ('opendata.aemet.es')
    MODE = EGRESS
    COMMENT = 'NETWORK RULE TO ALLOW SNOWFLAKE TO CONNECT TO THE SPANISH NATIONAL WEATHER API';

-- NOW WE NEED TO CONNECT TO THE AEMET WEBSITE TO GET AN API TOKEN

-- AFTER THAT, LET'S SAVE THE API TOKEN AS A SECRET SO IT'S SECURELY STORED IN SNOWFLAKE
CREATE OR REPLACE SECRET AEMET_API_SECRET
    TYPE = GENERIC_STRING
    SECRET_STRING = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJJTkZPQFBBQkxPU0FFTlpERVRFSkFEQS5DT00iLCJqdGkiOiI3NDBmOGQ1YS04NjJhLTQ0ZTAtYWZlYS03YjUxNGFlOGM0MjEiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTc0NzM5Mzc3OSwidXNlcklkIjoiNzQwZjhkNWEtODYyYS00NGUwLWFmZWEtN2I1MTRhZThjNDIxIiwicm9sZSI6IiJ9.u5XdtjU5iR2qtbUEOkNmqr5Fo22F-BimGru_fO8ya0k';

-- NOW, CREATE AN EXTERNAL ACCESS INTEGRATION THAT USES THE SECRET AND NETWORK RULE
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION AEMET_API_INTEGRATION
    ALLOWED_NETWORK_RULES = (AEMET_API_ACCESS)
    ALLOWED_AUTHENTICATION_SECRETS = (AEMET_API_SECRET)
    ENABLED = TRUE
    COMMENT = 'ALLOW TO USE THE AEMET_API_ACCESS NETWORK RULE AND THE AEMET_API_SECRET SECRET TO ACCESS AND QUERY DATA FROM AEMET API';

-- SPECIFY CONTEXT AGAIN
USE ROLE SYSADMIN;

-- CREATE A FUNCTION TO QUERY THE BEACH PREDICTION DATA
CREATE OR REPLACE FUNCTION PREDICT_BEACH_WEATHER(id int)
    RETURNS VARIANT
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    HANDLER = 'AEMET'
    EXTERNAL_ACCESS_INTEGRATIONS = (AEMET_API_INTEGRATION)
    PACKAGES = ('requests')
    SECRETS = ( 'api_key' = AEMET_API_SECRET)
    AS
        $$
        import requests
        import json
        import _snowflake

        def AEMET(id):
            # Use security integration
            try:
                key = _snowflake.get_generic_secret_string('api_key')

                query = "https://opendata.aemet.es/opendata/api/prediccion/especifica/playa/"+ str(id) +"?api_key=" + key
                headers = {
                    'Accept': "application/json"
                }

                # First API call to get the data URL
                response = requests.get(query, headers=headers)
                if response.status_code != 200:
                    return {"error": f"First API call failed with status code: {response.status_code}"}

                json_response = response.json()
                data_url = json_response.get('datos')

                if not data_url:
                    return {"error": "No data URL found in response"}

                # Second API call to get the actual data
                data_response = requests.get(data_url, headers=headers)
                if data_response.status_code != 200:
                    return {"error": f"Second API call failed with status code: {data_response.status_code}"}

                return data_response.json()

            except Exception as e:
                return {"error": f"Function failed with error: {str(e)}"}
        $$;

-- TEST THE FUNCTION CREATED WITH ONE OF THE BEACH_IDs FROM THE GOLD TABLE CREATED EARLIER
SELECT PREDICT_BEACH_WEATHER(4808502);

-- SET CONTEXT
USE SCHEMA TRAINING_WORKSHOP.SPAIN_WEATHER;

-- CHECK USAGE OF FUNCTION
WITH BEACH_PREDICTIONS AS
    (SELECT
         beach_id,
         PREDICT_BEACH_WEATHER(beach_id) AS JSON,
         TO_TIMESTAMP_NTZ(DATEADD('hours',7,current_timestamp)) AS TIMESTAMP FROM TRAINING_WORKSHOP.SPAIN_WEATHER.GOLD_BEACH_MASTER_TABLE LIMIT 2)
SELECT * FROM BEACH_PREDICTIONS
;


-- CREATE A TABLE TO STORE THE DATA QUERIED FROM API
CREATE TABLE TRAINING_WORKSHOP.SPAIN_WEATHER.RAW_BEACH_PREDICTION (
    BEACH_ID INTEGER,
    JSON VARIANT,
    TIMESTAMP TIMESTAMP
);

/*
INSERT DATA INTO THE TABLE USING THE FUNCTION
WE WILL LIMIT TO A CONCRETE PROVINCE AS THE API HAS A LIMIT OF 40 CALLS PER MINUTE
REMEMBER THAT THE FUNCTION IS DOING 2 CALLS EVERY TIME IT'S USED
*/
INSERT INTO TRAINING_WORKSHOP.SPAIN_WEATHER.RAW_BEACH_PREDICTION
    SELECT
        BEACH_ID,
        PREDICT_BEACH_WEATHER(BEACH_ID) AS JSON,
        TO_TIMESTAMP_NTZ(DATEADD('hours',7,current_timestamp)) AS TIMESTAMP
    FROM TRAINING_WORKSHOP.SPAIN_WEATHER.GOLD_BEACH_MASTER_TABLE
    WHERE TRAINING_WORKSHOP.SPAIN_WEATHER.GOLD_BEACH_MASTER_TABLE.PROVINCE = 'Málaga' limit 20;

-- CHECK DATA
SELECT * FROM TRAINING_WORKSHOP.SPAIN_WEATHER.RAW_BEACH_PREDICTION;

-- QUERY SEMI-STRUCTURED DATA TO TEST
SELECT
    p.value:elaborado::VARCHAR AS date,
    p.value:id::VARCHAR AS beach_id,
    p.value:prediccion:dia::VARIANT AS prediccion_dia,
    p.value:prediccion.dia[0].fecha::INT AS fecha1,
    p.value:prediccion.dia[1].fecha::INT AS fecha2
FROM TRAINING_WORKSHOP.SPAIN_WEATHER.RAW_BEACH_PREDICTION t,
LATERAL FLATTEN (INPUT => t.json) p;

-- CREATE A DYNAMIC TABLE
CREATE OR REPLACE DYNAMIC TABLE TRAINING_WORKSHOP.SPAIN_WEATHER.SILVER_BEACH_PREDICTION
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = COMPUTE_XS
    INITIALIZE = ON_CREATE
    AS
        SELECT
            p.value:elaborado::TIMESTAMP as date,
            p.value:id::VARCHAR as beach_id,
            to_date(q.value:fecha::VARCHAR, 'YYYYMMDD') as prediction_date,
            q.value:estadoCielo:descripcion1::VARCHAR as sky_11,
            q.value:estadoCielo:descripcion2::VARCHAR as sky_17,
            q.value:oleaje:descripcion1::VARCHAR as swell_11,
            q.value:oleaje:descripcion2::VARCHAR as swell_17,
            q.value:viento:descripcion1::VARCHAR as wind_11,
            q.value:viento:descripcion2::VARCHAR as wind_17,
            q.value:sTermica:descripcion1::VARCHAR as feeling,
            q.value:tAgua:valor1::INT as water_temperature,
            q.value:tMaxima:valor1::INT as max_temperature,
            q.value:uvMax:valor1::INT as uv_index
        FROM TRAINING_WORKSHOP.SPAIN_WEATHER.RAW_BEACH_PREDICTION t,
        LATERAL FLATTEN (INPUT => t.json) p,
        LATERAL FLATTEN (INPUT => p.value:prediccion:dia) q;

-- CREATE A SECOND DYNAMIC TABLE
CREATE OR REPLACE DYNAMIC TABLE GOLD_BEACH_PREDICTION
    TARGET_LAG = '1 HOUR'
    WAREHOUSE = COMPUTE_XS
    INITIALIZE = ON_CREATE
    AS
        WITH LATEST_RECORD AS (
            SELECT
                MAX(DATE) AS RECORD_DATE,
                BEACH_ID, PREDICTION_DATE
            FROM TRAINING_WORKSHOP.SPAIN_WEATHER.SILVER_BEACH_PREDICTION
            GROUP BY 2,3
            ORDER BY 2,3
        ),
        PREDICTION AS (
            SELECT
                date,
                beach_id,
                prediction_date,
                sky_11,
                sky_17,
                swell_11,
                swell_17,
                wind_11,
                wind_17,
                feeling,
                water_temperature,
                max_temperature,
                uv_index
            FROM TRAINING_WORKSHOP.SPAIN_WEATHER.SILVER_BEACH_PREDICTION
        )
        SELECT
            t1.RECORD_DATE,
            t1.BEACH_ID,
            t1.PREDICTION_DATE,
            t2.sky_11,
            t2.sky_17,
            t2.swell_11,
            t2.swell_17,
            t2.wind_11,
            t2.wind_17,
            t2.feeling,
            t2.water_temperature,
            t2.max_temperature,
            t2.uv_index
        FROM LATEST_RECORD t1
        LEFT JOIN PREDICTION t2
        ON t1.beach_id = t2.beach_id AND t1.record_date = t2.date AND t1.prediction_date = t2.prediction_date;

-- CHECK DATA
SELECT * FROM TRAINING_WORKSHOP.SPAIN_WEATHER.GOLD_BEACH_PREDICTION;