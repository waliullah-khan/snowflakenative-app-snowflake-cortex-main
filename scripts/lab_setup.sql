--Step 3.1 - Create NACTX role and Grant Privileges
use role accountadmin;
create role if not exists nactx_role;
grant role nactx_role to role accountadmin;
grant create warehouse on account to role nactx_role;
grant create database on account to role nactx_role;
grant create application package on account to role nactx_role;
grant create application on account to role nactx_role with grant option;

--Step 3.2 - Create CORTEX_APP Database to Store Application Files 
use role nactx_role;
create database if not exists cortex_app;
create schema if not exists cortex_app.napp;
create stage if not exists cortex_app.napp.app_stage;
create warehouse if not exists wh_nap with warehouse_size='xsmall';

-- Step 4.1 - Create NAC role and Grant Privileges
use role accountadmin;
create role if not exists nac;
grant role nac to role accountadmin;
grant create warehouse on account to role nac;
grant create database on account to role nac;
grant create application on account to role nac;


--Step 4.2 - Create Consumer Test Data Database and Load Data
use role nac;
create warehouse if not exists wh_nac with warehouse_size='medium';
create database if not exists movies;
create schema if not exists movies.data;
use schema movies.data;

CREATE STAGE MOVIES.DATA.MY_STAGE
URL='s3://sfquickstarts/vhol_build_2024_native_app_cortex_search/movies_metadata.csv'
DIRECTORY = (
ENABLE = true
AUTO_REFRESH = true
);

CREATE FILE FORMAT MOVIES.DATA.CSV_FILE_FORMAT
	TYPE=CSV
    SKIP_HEADER=1
    FIELD_DELIMITER=','
    TRIM_SPACE=TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY='"'
    REPLACE_INVALID_CHARACTERS=TRUE
    DATE_FORMAT=AUTO
    TIME_FORMAT=AUTO
    TIMESTAMP_FORMAT=AUTO; 

CREATE TABLE movies.data.movies_metadata (
    adult BOOLEAN,
    belongs_to_collection VARCHAR,
    budget NUMBER(38, 0),
    genres VARCHAR,
    homepage VARCHAR,
    id NUMBER(38, 0),
    imdb_id VARCHAR,
    original_language VARCHAR,
    original_title VARCHAR,
    overview VARCHAR,
    popularity VARCHAR,
    poster_path VARCHAR,
    production_companies VARCHAR,
    production_countries VARCHAR,
    release_date DATE,
    revenue NUMBER(38, 0),
    runtime NUMBER(38, 1),
    spoken_languages VARCHAR,
    status VARCHAR,
    tagline VARCHAR,
    title VARCHAR,
    video BOOLEAN,
    vote_average NUMBER(38, 1),
    vote_count NUMBER(38, 0)
);

COPY INTO MOVIES.DATA.MOVIES_METADATA
FROM  @my_stage FILE_FORMAT = (FORMAT_NAME = CSV_FILE_FORMAT) ON_ERROR = CONTINUE;

create or replace table movies_raw as 
select title, budget::string as budget, overview, popularity, release_date::string as release_date, 
runtime::string as runtime 
from movies.data.movies_metadata;

--Step 5.1 - Create Application Package and Grant Consumer Role Privileges
use role nactx_role;
create application package cortex_app_pkg;

--Step 5.2 - Upload Native App Code
--Upload the code from the App and src files into the Cortex Database App Stage

--Step 5.3 - Create Application Package
alter application package cortex_app_pkg add version v1 using @cortex_app.napp.app_stage;
grant install, develop on application package cortex_app_pkg to role nac;

--Step 6.1 - Install App as the Consumer
use role nac;
create application cortex_app_instance from application package cortex_app_pkg using version v1;

grant all on database movies to application cortex_app_instance;
grant all on schema movies.data to application cortex_app_instance;
grant all on table movies.data.movies_raw to application cortex_app_instance;
grant usage on warehouse wh_nac to application cortex_app_instance;

--This is a special step required to allow Native Apps to utilized Cortex Functions in a consumer database 
use role accountadmin;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION cortex_app_instance;



--Call the chunker sproc which uses the text chunk function
use role nac;
call CORTEX_APP_INSTANCE.CORE.TABLE_CHUNKER();
call CORTEX_APP_INSTANCE.CORE.CREATE_CORTEX_SEARCH();


--Step 7.1 - Clean Up
--clean up consumer objects
use role NAC;
drop application cortex_app_instance cascade;
drop warehouse wh_nac;
drop database movies;

--clean up provider objects
use role nactx_role;
drop application package cortex_app_pkg;
drop database cortex_app;
drop warehouse wh_nap;

--clean up prep objects
use role accountadmin;
drop role nactx_role;
drop role nac;
