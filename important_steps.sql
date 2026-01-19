-- Step 1: Use an admin role
USE ROLE ACCOUNTADMIN;

-- Step 2: Create the `transform` role and assign it to ACCOUNTADMIN
CREATE ROLE IF NOT EXISTS transform;
GRANT ROLE transform TO ROLE ACCOUNTADMIN;

-- Step 3: Create a default warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE transform;

-- Step 4: Create the `dbt` user and assign to the transform role
CREATE USER IF NOT EXISTS dbt
  PASSWORD='**********'
  LOGIN_NAME='**********'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE=TRANSFORM
  DEFAULT_NAMESPACE='netflix.landing'
  COMMENT='DBT user used for data transformation';

ALTER USER dbt SET TYPE = LEGACY_SERVICE;
GRANT ROLE transform TO USER dbt;

-- Step 5: Create a database and schema for the MovieLens project
CREATE DATABASE IF NOT EXISTS netflix;
CREATE SCHEMA IF NOT EXISTS netflix.landing;

-- Step 6: Grant permissions to the `transform` role
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE transform;
GRANT ALL ON DATABASE netflix TO ROLE transform;
GRANT ALL ON ALL SCHEMAS IN DATABASE netflix TO ROLE transform;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE netflix TO ROLE transform;
GRANT ALL ON ALL TABLES IN SCHEMA netflix.landing TO ROLE transform;
GRANT ALL ON FUTURE TABLES IN SCHEMA netflix.landing TO ROLE transform;


-- Set Context
USE WAREHOUSE COMPUTE_WH;
USE DATABASE netflix;
USE SCHEMA landing;


-- Step #: Create Storage Integration between Snowflake and Azure. 
-- Create a storage integration object 
CREATE OR REPLACE STORAGE INTEGRATION azure_blob_int 
TYPE = EXTERNAL_STAGE 
STORAGE_PROVIDER = AZURE 
ENABLED = TRUE 
AZURE_TENANT_ID = '************************************' 
STORAGE_ALLOWED_LOCATIONS = ('************************************'); 

DESC INTEGRATION azure_blob_int; 

-- Step #: Retrieve Snowflake’s identity and grant access in Azure. 
    -- Grant admin consent for Azure Integration with Snowflake. 
    -- Assign RBAC role to Snowflake in Azure.

-- Step #: Create File format in Snowflake. 
CREATE OR REPLACE FILE FORMAT netflix_ff 
TYPE = CSV 
FIELD_DELIMITER = ',' 
SKIP_HEADER = 1 
EMPTY_FIELD_AS_NULL = TRUE; 

-- Step #: Creating an External Stage in Snowflake to access Blob storage. 
-- Create stage object using integration and file format 
CREATE OR REPLACE STAGE netflix_ext_stg 
STORAGE_INTEGRATION = azure_blob_int
URL = '************************************' 
FILE_FORMAT = netflix_ff;

LIST @netflix_ext_stg; 
-- Step #: Load data into Snowflake tables using COPY commands. 
-- 1. Load raw_movies
-- Step A: Create the target table 
CREATE OR REPLACE TABLE raw_movies ( 
movieId INTEGER, 
title STRING, 
genres STRING 
); 
-- Step B: Load data into the table from the Azure Blob Storage using COPY command 
COPY INTO raw_movies
FROM @netflix_ext_stg/movies.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');
-- Step C: Query the table to verify the data load 
SELECT * FROM raw_movies;

-- 2. Load raw_ratings
CREATE OR REPLACE TABLE raw_ratings (
  userId INTEGER,
  movieId INTEGER,
  rating FLOAT,
  timestamp BIGINT
);

COPY INTO raw_ratings
FROM @netflix_ext_stg/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- 3. Load raw_tags
CREATE OR REPLACE TABLE raw_tags (
  userId INTEGER,
  movieId INTEGER,
  tag STRING,
  timestamp BIGINT
);

COPY INTO raw_tags
FROM @netflix_ext_stg/tags.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

-- 4. Load raw_genome_scores
CREATE OR REPLACE TABLE raw_genome_scores (
  movieId INTEGER,
  tagId INTEGER,
  relevance FLOAT
);

COPY INTO raw_genome_scores
FROM @netflix_ext_stg/genome-scores.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- 5. Load raw_genome_tags
CREATE OR REPLACE TABLE raw_genome_tags (
  tagId INTEGER,
  tag STRING
);

COPY INTO raw_genome_tags
FROM @netflix_ext_stg/genome-tags.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- 6. Load raw_links
CREATE OR REPLACE TABLE raw_links (
  movieId INTEGER,
  imdbId INTEGER,
  tmdbId INTEGER
);

COPY INTO raw_links
FROM @netflix_ext_stg/links.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');


-- Query the table to verify the data load 
SELECT * FROM raw_movies;
SELECT * FROM raw_genome_tags;
SELECT * FROM raw_genome_scores;
SELECT * FROM raw_links;
SELECT * FROM raw_ratings;
SELECT * FROM raw_tags;


-- Windows
virtualenv venv
venv\Scripts\activate

-- MAC
mkdir course
cd course
virtualenv venv
. venv/bin/activate

-- install dbt snowflake
pip install dbt-snowflake==1.9.0

-- create dbt profile
-- mac
mkdir ~/.dbt

-- windows 
mkdir %userprofile%\.dbt

#initiate dbt project 
dbt init <projectname>

-- Add staging tables -> all will be views
-- Update dbt_project.yaml file to support of tables
dim:
  +materialized: table
--   +schema: dim
-- fct:
--   +materialized: incremental
--   +schema: fct
-- mart:
--   +materialized: table
--   +schema: mart
-- snapshots:
--   +schema: snapshots
