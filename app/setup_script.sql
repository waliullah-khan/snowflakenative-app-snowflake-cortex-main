-- This is the setup script that runs while installing a Snowflake Native App in a consumer account.
-- For more information on how to create setup file, visit https://docs.snowflake.com/en/developer-guide/native-apps/creating-setup-script

-- A general guideline to building this script looks like:
-- 1. Create application roles
CREATE APPLICATION ROLE IF NOT EXISTS app_public;

-- 2. Create a versioned schema to hold those UDFs/Stored Procedures
CREATE OR ALTER VERSIONED SCHEMA core;
GRANT USAGE ON SCHEMA core TO APPLICATION ROLE app_public;

-- 3. Create UDFs and Stored Procedures using the python code you wrote in src/module-add, as shown below.

CREATE OR REPLACE FUNCTION core.text_chunk(
    overview string, title string, budget string, popularity string, release_date string, runtime string
)
    returns table (chunk string, title string, budget string, popularity string, release_date string, runtime string)
    language python
    runtime_version = '3.9'
    handler = 'text_chunker'
    packages = ('snowflake-snowpark-python','langchain')
    as
$$
from langchain.text_splitter import RecursiveCharacterTextSplitter
import copy
from typing import Optional

class text_chunker:

    def process(self, overview: Optional[str], title: str, budget: str, popularity: str, release_date: str, runtime: str):
        # Handle None values for all input strings
        overview = overview or ""
        title = title or ""
        budget = budget or ""
        popularity = popularity or ""
        release_date = release_date or ""
        runtime = runtime or ""

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size = 2000,
            chunk_overlap  = 300,
            length_function = len
        )
        chunks = text_splitter.split_text(overview)
        for chunk in chunks:
            yield (title + "\n" + budget + "\n" + popularity + "\n" + runtime + "\n" + chunk, title, budget, popularity, release_date, runtime) # always chunk with title
$$;

CREATE OR REPLACE PROCEDURE core.table_chunker()
    RETURNS string
    LANGUAGE sql
    AS $$
BEGIN
    EXECUTE IMMEDIATE 'CREATE or replace TABLE movies.data.movies_metadata_chunked AS (
    SELECT
        movies.*,
        t.CHUNK as CHUNK
    FROM movies.data.movies_raw movies,
        TABLE(core.text_chunk(movies.overview, movies.title, movies.budget, movies.popularity, movies.release_date, movies.runtime))t)';
GRANT SELECT ON TABLE MOVIES.DATA.MOVIES_METADATA_CHUNKED TO application role app_public;
RETURN 'Table Chunked!';
END;
$$;

CREATE OR REPLACE PROCEDURE core.create_cortex_search()
    RETURNS string
    LANGUAGE sql
    AS $$
BEGIN
        EXECUTE IMMEDIATE 'CREATE CORTEX SEARCH SERVICE movies.data.movies_recommendation_service
        ON CHUNK
        WAREHOUSE = wh_nac
        TARGET_LAG = \'1 hour\'
        AS (
            SELECT *
            FROM movies.data.movies_metadata_chunked
    )';
GRANT ALL ON CORTEX SEARCH SERVICE MOVIES.DATA.MOVIES_RECOMMENDATION_SERVICE TO application role app_public;
RETURN 'Search Created go to Streamlit App';
END;
$$;

CREATE OR REPLACE PROCEDURE core.register_single_callback(ref_name STRING, operation STRING, ref_or_alias STRING)
 RETURNS STRING
 LANGUAGE SQL
 AS $$
      BEGIN
      CASE (operation)
         WHEN 'ADD' THEN
            SELECT system$set_reference(:ref_name, :ref_or_alias);
         WHEN 'REMOVE' THEN
            SELECT system$remove_reference(:ref_name);
         WHEN 'CLEAR' THEN
            SELECT system$remove_reference(:ref_name);
         ELSE
            RETURN 'Unknown operation: ' || operation;
      END CASE;
      RETURN 'Operation ' || operation || ' succeeds.';
      END;
   $$;


-- 4. Grant appropriate privileges over these objects to your application roles.
GRANT USAGE ON FUNCTION core.text_chunk( STRING,  STRING,  STRING, STRING, STRING, STRING) TO APPLICATION ROLE app_public;
GRANT USAGE ON PROCEDURE core.table_chunker() TO APPLICATION ROLE app_public;
GRANT USAGE ON PROCEDURE core.create_cortex_search() TO APPLICATION ROLE app_public;
GRANT USAGE ON PROCEDURE core.register_single_callback( STRING,  STRING,  STRING) TO APPLICATION ROLE app_public;


-- 5. Create a streamlit object using the code you wrote in you wrote in src/module-ui, as shown below.
-- The `from` value is derived from the stage path described in snowflake.yml
CREATE OR REPLACE STREAMLIT core.ui
     FROM '/src/'
     MAIN_FILE = 'ui.py';

-- 6. Grant appropriate privileges over these objects to your application roles.
GRANT USAGE ON STREAMLIT core.ui TO APPLICATION ROLE app_public;

-- A detailed explanation can be found at https://docs.snowflake.com/en/developer-guide/native-apps/adding-streamlit
