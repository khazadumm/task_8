USE ROLE ACCOUNTADMIN;

--DB
CREATE OR REPLACE DATABASE TASK_8;

--SCHEMAS
CREATE OR REPLACE SCHEMA TASK_8.STAGING_SCHEMA;
CREATE OR REPLACE SCHEMA TASK_8.CLEANCING_SCHEMA;
CREATE OR REPLACE SCHEMA TASK_8.PROD_SCHEMA;
CREATE OR REPLACE SCHEMA TASK_8.TECH_SCHEMA;

-- LOGGING

CREATE OR REPLACE PROCEDURE TASK_8.TECH_SCHEMA.EVENT_PROC(message VARCHAR)
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
AS '
BEGIN
    SYSTEM$LOG_INFO(''Processed '' || :message);
END';

CREATE OR REPLACE PROCEDURE TASK_8.TECH_SCHEMA.CREATE_EVENT_TABLE()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
AS '
BEGIN
    CREATE OR REPLACE EVENT TABLE TASK_8.TECH_SCHEMA.DB_EVENTS;
    ALTER ACCOUNT SET EVENT_TABLE = TASK_8.TECH_SCHEMA.DB_EVENTS;
    ALTER PROCEDURE EVENT_PROC(VARCHAR) SET LOG_LEVEL = INFO;
END';

CALL TASK_8.TECH_SCHEMA.CREATE_EVENT_TABLE();
CALL TASK_8.TECH_SCHEMA.EVENT_PROC('TASK_8.TECH_SCHEMA.CREATE_EVENT_TABLE() FINISHED');

--ROLES

CREATE OR REPLACE DATABASE ROLE DEVELOPER1;

GRANT ROLE DEVELOPER TO ROLE USERADMIN ;

GRANT OPERATE ON WAREHOUSE WH TO ROLE DEVELOPER;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE TASK_8 TO ROLE DEVELOPER;
GRANT SELECT ON ALL TABLES IN SCHEMA TASK_8.PROD_SCHEMA TO ROLE DEVELOPER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE TASK_8 TO ROLE DEVELOPER;
GRANT SELECT ON ALL VIEWS IN DATABASE TASK_8 TO ROLE DEVELOPER;

CREATE OR REPLACE ROW ACCESS POLICY viewers_fct_view
AS (cur_role_ number) RETURNS BOOLEAN ->
  CURRENT_ROLE() IN ('DEVELOPER')
;

-- STAGING

CREATE OR REPLACE TABLE TASK_8.STAGING_SCHEMA.SOURCE_DATASET (
    ID VARCHAR,
    PASSENGERID VARCHAR,
    FIRSTNAME VARCHAR,
    LASTNAME VARCHAR,
    GENDER VARCHAR,
    AGE VARCHAR,
    NATIONALITY VARCHAR,
    AIRPORTNAME VARCHAR,
    AIRPORTCOUNTRYCODE VARCHAR,
    COUNTRYNAME VARCHAR,
    AIRPORTCONTINENT VARCHAR,
    CONTINENTS VARCHAR,
    DEPARTUREDATE VARCHAR,
    ARRIVALAIRPORT VARCHAR,
    PILOTNAME VARCHAR,
    FLIGHTSTATUS VARCHAR,
    TICKETTYPE VARCHAR,
    PASSENGERSTATUS VARCHAR
);

CREATE OR REPLACE FILE FORMAT TASK_8.STAGING_SCHEMA.CSV_DEFAULF
    TYPE = CSV
    SKIP_HEADER = 1
    RECORD_DELIMITER = '\n'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null')
    FIELD_OPTIONALLY_ENCLOSED_BY = '0x22'
    EMPTY_FIELD_AS_NULL = TRUE
    ENCODING = 'iso-8859-1';

CREATE OR REPLACE STAGE TASK_8.STAGING_SCHEMA.SOURCE_CSV_STAGE
    COPY_OPTIONS = (ON_ERROR='skip_file');
        
CREATE OR REPLACE PROCEDURE TASK_8.STAGING_SCHEMA.DROP_DATASETS_AFTER_LOAD()
    RETURNS VARIANT
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
AS 
$$
    var my_sql_command = "REMOVE @TASK_8.STAGING_SCHEMA.SOURCE_CSV_STAGE PATTERN='.*.csv'" ;
    var statement1 = snowflake.createStatement( {sqlText: my_sql_command} );
    var result_set1 = statement1.execute();
    return 'All files removed from stage';
$$
;

CREATE OR REPLACE PROCEDURE TASK_8.STAGING_SCHEMA.LOAD_DATASETS()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
AS 'BEGIN
    COPY INTO TASK_8.STAGING_SCHEMA.SOURCE_DATASET
    FROM @TASK_8.STAGING_SCHEMA.SOURCE_CSV_STAGE
    FILE_FORMAT = (FORMAT_NAME = TASK_8.STAGING_SCHEMA.CSV_DEFAULF)
    FORCE=TRUE
    PURGE=FALSE
    ON_ERROR = ''skip_file'';
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''COPY INTO TASK_8.STAGING_SCHEMA.SOURCE_DATASET FROM @TASK_8.STAGING_SCHEMA.SOURCE_CSV_STAGE'');
    RETURN ''DONE!'';
 END';

CREATE OR REPLACE PROCEDURE TASK_8.STAGING_SCHEMA.LOAD_SOURCE_DATA_TO_STAGING()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS CALLER
AS 'DEClARE
res1 VARCHAR;
res2 VARCHAR;
BEGIN
    CALL TASK_8.STAGING_SCHEMA.LOAD_DATASETS() into res1;
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''TASK_8.STAGING_SCHEMA.LOAD_DATASETS()'');
    CALL TASK_8.STAGING_SCHEMA.DROP_DATASETS_AFTER_LOAD() into res2;  
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''TASK_8.STAGING_SCHEMA.DROP_DATASETS_AFTER_LOAD()'');
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''TASK_8.STAGING_SCHEMA.LOAD_SOURCE_DATA_TO_STAGING()'');
    RETURN res1 || ''; '' || res2;
END';

-- CLEANCING

CREATE OR REPLACE TABLE TASK_8.CLEANCING_SCHEMA.CLEANCING_SOURCE_DATASET (
	ID NUMBER,
	PASSENGERID VARCHAR,
	FIRSTNAME VARCHAR,
	LASTNAME VARCHAR,
	GENDER VARCHAR,
	AGE NUMBER,
	NATIONALITY VARCHAR,
	AIRPORTNAME VARCHAR,
	AIRPORTCOUNTRYCODE VARCHAR,
	COUNTRYNAME VARCHAR,
	AIRPORTCONTINENT VARCHAR,
	CONTINENTS VARCHAR,
	DEPARTUREDATE DATE,
	ARRIVALAIRPORT VARCHAR,
	PILOTNAME VARCHAR,
	FLIGHTSTATUS VARCHAR,
	TICKETTYPE VARCHAR,
	PASSENGERSTATUS VARCHAR
);

CREATE OR REPLACE PROCEDURE TASK_8.CLEANCING_SCHEMA.CREATE_WRK_TABLES()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS 
'BEGIN
    CREATE OR REPLACE TABLE TASK_8.CLEANCING_SCHEMA.WRK_PASSENGERS
    AS
    SELECT 
        PASSENGERID, FIRSTNAME,LASTNAME,GENDER,AGE,NATIONALITY
    FROM 
        TASK_8.CLEANCING_SCHEMA.CLEANCING_SOURCE_DATASET 
    WHERE 1=2;
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''CLEANCING_SCHEMA.WRK_PASSENGERS CREATED'');
    --------
    CREATE OR REPLACE TABLE TASK_8.CLEANCING_SCHEMA.WRK_FLIGHTSTATUSES
    AS
    SELECT 
        FLIGHTSTATUS
    FROM 
        TASK_8.CLEANCING_SCHEMA.CLEANCING_SOURCE_DATASET 
    WHERE 1=2;
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''CLEANCING_SCHEMA.WRK_FLIGHTSTATUSES CREATED'');
    --------
    CREATE OR REPLACE TABLE TASK_8.CLEANCING_SCHEMA.WRK_TICKETTYPES
    AS
    SELECT 
        TICKETTYPE
    FROM 
        TASK_8.CLEANCING_SCHEMA.CLEANCING_SOURCE_DATASET 
    WHERE 1=2;
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''CLEANCING_SCHEMA.WRK_TICKETTYPES CREATED'');
    --------
    CREATE OR REPLACE TABLE TASK_8.CLEANCING_SCHEMA.WRK_PASSENGERSTATUSES
    AS
    SELECT 
        PASSENGERSTATUS
    FROM 
        TASK_8.CLEANCING_SCHEMA.CLEANCING_SOURCE_DATASET 
    WHERE 1=2;
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''CLEANCING_SCHEMA.WRK_PASSENGERSTATUSES CREATED'');
    --------
    CREATE OR REPLACE TABLE TASK_8.CLEANCING_SCHEMA.WRK_PILOTS
    AS
    SELECT 
        PILOTNAME
    FROM 
        TASK_8.CLEANCING_SCHEMA.CLEANCING_SOURCE_DATASET 
    WHERE 1=2; 
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''CLEANCING_SCHEMA.WRK_PILOTS CREATED'');
    --------
    CREATE OR REPLACE TABLE TASK_8.CLEANCING_SCHEMA.WRK_ARRIVALAIRPORTS
    AS
    SELECT  
        ARRIVALAIRPORT
    FROM 
        TASK_8.CLEANCING_SCHEMA.CLEANCING_SOURCE_DATASET 
    WHERE 1=2; 
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''CLEANCING_SCHEMA.WRK_ARRIVALAIRPORTS CREATED'');
    --------
    CREATE OR REPLACE TABLE TASK_8.CLEANCING_SCHEMA.WRK_AIRPORTS
    AS
    SELECT 
        AIRPORTNAME,AIRPORTCOUNTRYCODE,COUNTRYNAME,AIRPORTCONTINENT,CONTINENTS
    FROM 
        TASK_8.CLEANCING_SCHEMA.CLEANCING_SOURCE_DATASET 
    WHERE 1=2; 
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''CLEANCING_SCHEMA.WRK_AIRPORTS CREATED'');
    --------
    RETURN ''DONE!'';
 END';

CALL TASK_8.CLEANCING_SCHEMA.CREATE_WRK_TABLES();

CREATE OR REPLACE PROCEDURE TASK_8.CLEANCING_SCHEMA.LOAD_FCT_TABLES()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
AS 
'BEGIN
    INSERT INTO TASK_8.PROD_SCHEMA.FCT_FLIES
        (PASSENGER_ID,AIRPORT_ID,DEPARTUREDATE,ARRIVALAIRPORT_ID,PILOTNAME_ID,FLIGHTSTATUS_ID,TICKETTYPE_ID,PASSENGERSTATUS_ID)
    SELECT 
        P.ID AS PASSENGER_ID 
        , A.ID AS AIRPORT_ID
        , S.DEPARTUREDATE
        , AA.ID AS ARRIVALAIRPORT_ID
        , PIL.ID AS PILOT_ID
        , F.ID AS FLIGHTSTATUS_ID
        , T.ID AS TICKETTYPE_ID
        , PS.ID AS PASSENGERSTATUS_ID    
    FROM 
        TASK_8.CLEANCING_SCHEMA.CLEANCING_SOURCE_DATASET AS  S
        JOIN TASK_8.PROD_SCHEMA.DIM_AIRPORTS AS A ON (S.AIRPORTNAME = A.AIRPORTNAME)
        JOIN TASK_8.PROD_SCHEMA.DIM_ARRIVALAIRPORTS AS AA ON(S.ARRIVALAIRPORT = AA.ARRIVALAIRPORT)
        JOIN TASK_8.PROD_SCHEMA.DIM_FLIGHTSTATUSES AS F ON (S.FLIGHTSTATUS = F.FLIGHTSTATUS)
        JOIN TASK_8.PROD_SCHEMA.DIM_PASSENGERS AS P ON (S.PASSENGERID = P.PASSENGERID)
        JOIN TASK_8.PROD_SCHEMA.DIM_PASSENGERSTATUSES AS PS ON (S.PASSENGERSTATUS = PS.PASSENGERSTATUS)
        JOIN TASK_8.PROD_SCHEMA.DIM_PILOTS AS PIL ON (S.PILOTNAME = PIL.PILOTNAME)
        JOIN TASK_8.PROD_SCHEMA.DIM_TICKETTYPES AS T ON (S.TICKETTYPE = T.TICKETTYPE);
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''TASK_8.PROD_SCHEMA.FCT_FLIES INSERTED '' || :SQLROWCOUNT || '' ROWS'');
    RETURN ''DONE!'';
 END';

CREATE OR REPLACE PROCEDURE TASK_8.CLEANCING_SCHEMA.LOAD_SOURCE_TO_CLEANCING_LAYER()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
AS 
'BEGIN
    INSERT  INTO TASK_8.CLEANCING_SCHEMA.CLEANCING_SOURCE_DATASET (
        ID ,
        PASSENGERID ,
        FIRSTNAME ,
        LASTNAME ,
        GENDER ,
        AGE ,
        NATIONALITY ,
        AIRPORTNAME ,
        AIRPORTCOUNTRYCODE ,
        COUNTRYNAME ,
        AIRPORTCONTINENT ,
        CONTINENTS ,
        DEPARTUREDATE ,
        ARRIVALAIRPORT ,
        PILOTNAME ,
        FLIGHTSTATUS ,
        TICKETTYPE ,
        PASSENGERSTATUS 
    )
    SELECT
        CAST(ID AS NUMBER)  ,
        PASSENGERID ,
        FIRSTNAME ,
        LASTNAME ,
        GENDER ,
        CAST(AGE AS NUMBER) ,
        NATIONALITY ,
        AIRPORTNAME ,
        AIRPORTCOUNTRYCODE ,
        COUNTRYNAME ,
        AIRPORTCONTINENT ,
        CONTINENTS ,
        CAST(DEPARTUREDATE AS DATE)  ,
        ARRIVALAIRPORT ,
        PILOTNAME ,
        FLIGHTSTATUS ,
        TICKETTYPE ,
        PASSENGERSTATUS  
    FROM TASK_8.STAGING_SCHEMA.SOURCE_DATASET;
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''TASK_8.CLEANCING_SCHEMA.LOAD_SOURCE_TO_CLEANCING_LAYER(), Rows inserted: '' || :SQLROWCOUNT);
    RETURN ''DONE!'';
END';

CREATE OR REPLACE PROCEDURE TASK_8.CLEANCING_SCHEMA.LOAD_WRK_TABLES()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
AS 
'DEClARE
    res VARCHAR := ''DONE!'';
BEGIN
    INSERT OVERWRITE  INTO TASK_8.CLEANCING_SCHEMA.WRK_PASSENGERS
        (PASSENGERID, FIRSTNAME,LASTNAME,GENDER,AGE,NATIONALITY)
    SELECT 
        DISTINCT PASSENGERID, FIRSTNAME,LASTNAME,GENDER,AGE,NATIONALITY
    FROM 
        TASK_8.CLEANCING_SCHEMA.CLEANCING_SOURCE_DATASET;
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''TASK_8.CLEANCING_SCHEMA.LOAD_WRK_TABLES(),CLEANCING_SCHEMA.WRK_PASSENGERS, Rows inserted: '' || :SQLROWCOUNT);
    --------
    INSERT OVERWRITE  INTO TASK_8.CLEANCING_SCHEMA.WRK_FLIGHTSTATUSES
        (FLIGHTSTATUS)
    SELECT 
        DISTINCT FLIGHTSTATUS
    FROM 
        TASK_8.CLEANCING_SCHEMA.CLEANCING_SOURCE_DATASET;
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''TASK_8.CLEANCING_SCHEMA.LOAD_WRK_TABLES(),CLEANCING_SCHEMA.WRK_FLIGHTSTATUSES, Rows inserted: '' || :SQLROWCOUNT);
    --------
    INSERT OVERWRITE  INTO TASK_8.CLEANCING_SCHEMA.WRK_TICKETTYPES
        (TICKETTYPE)
    SELECT 
        DISTINCT TICKETTYPE
    FROM 
        TASK_8.CLEANCING_SCHEMA.CLEANCING_SOURCE_DATASET;
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''TASK_8.CLEANCING_SCHEMA.LOAD_WRK_TABLES(),CLEANCING_SCHEMA.WRK_TICKETTYPES, Rows inserted: '' || :SQLROWCOUNT);
    --------
    INSERT OVERWRITE  INTO TASK_8.CLEANCING_SCHEMA.WRK_PASSENGERSTATUSES
        (PASSENGERSTATUS)
    SELECT 
        DISTINCT PASSENGERSTATUS
    FROM 
        TASK_8.CLEANCING_SCHEMA.CLEANCING_SOURCE_DATASET;
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''TASK_8.CLEANCING_SCHEMA.LOAD_WRK_TABLES(),CLEANCING_SCHEMA.WRK_PASSENGERSTATUSES, Rows inserted: '' || :SQLROWCOUNT);
    --------
    INSERT OVERWRITE  INTO TASK_8.CLEANCING_SCHEMA.WRK_PILOTS
        (PILOTNAME)
    SELECT 
        DISTINCT PILOTNAME
    FROM 
        TASK_8.CLEANCING_SCHEMA.CLEANCING_SOURCE_DATASET;
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''TASK_8.CLEANCING_SCHEMA.LOAD_WRK_TABLES(),CLEANCING_SCHEMA.WRK_PILOTS, Rows inserted: '' || :SQLROWCOUNT);
    --------
    INSERT OVERWRITE  INTO TASK_8.CLEANCING_SCHEMA.WRK_ARRIVALAIRPORTS
        (ARRIVALAIRPORT)
    SELECT 
        DISTINCT ARRIVALAIRPORT
    FROM 
        TASK_8.CLEANCING_SCHEMA.CLEANCING_SOURCE_DATASET; 
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''TASK_8.CLEANCING_SCHEMA.LOAD_WRK_TABLES(),CLEANCING_SCHEMA.WRK_ARRIVALAIRPORTS, Rows inserted: '' || :SQLROWCOUNT);
    --------
    INSERT OVERWRITE  INTO TASK_8.CLEANCING_SCHEMA.WRK_AIRPORTS
        (AIRPORTNAME,AIRPORTCOUNTRYCODE,COUNTRYNAME,AIRPORTCONTINENT,CONTINENTS)
    SELECT 
        DISTINCT AIRPORTNAME,AIRPORTCOUNTRYCODE,COUNTRYNAME,AIRPORTCONTINENT,CONTINENTS
    FROM 
        TASK_8.CLEANCING_SCHEMA.CLEANCING_SOURCE_DATASET; 
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''TASK_8.CLEANCING_SCHEMA.LOAD_WRK_TABLES(),CLEANCING_SCHEMA.WRK_AIRPORTS, Rows inserted: '' || :SQLROWCOUNT);
    --------
    RETURN ''DONE!'';
 END';

CREATE OR REPLACE PROCEDURE TASK_8.CLEANCING_SCHEMA.UPDATE_DIM_TABLES()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DEClARE
    res_tmp RESULTSET;
    query VARCHAR := '''';
    tmp_str VARCHAR := '''';
    cnt INTEGER := 0;
    ins_str VARCHAR := '''';
    key_str VARCHAR := '''';
BEGIN
    LET list_tables RESULTSET DEFAULT (
        SELECT 
            $1 AS from_table, $2 AS to_table, $3 AS id_field , $4 AS all_filds 
        FROM (
            VALUES 
                (''WRK_AIRPORTS'', ''DIM_AIRPORTS'', ''AIRPORTNAME,AIRPORTCOUNTRYCODE'',''AIRPORTNAME,AIRPORTCOUNTRYCODE,COUNTRYNAME,AIRPORTCONTINENT,CONTINENTS''), 
                (''WRK_ARRIVALAIRPORTS'', ''DIM_ARRIVALAIRPORTS'', ''ARRIVALAIRPORT'', ''ARRIVALAIRPORT''),
                (''WRK_FLIGHTSTATUSES'', ''DIM_FLIGHTSTATUSES'', ''FLIGHTSTATUS'',''FLIGHTSTATUS''),
                (''WRK_PASSENGERS'', ''DIM_PASSENGERS'', ''PASSENGERID'', ''PASSENGERID,FIRSTNAME,LASTNAME,GENDER,AGE,NATIONALITY''),
                (''WRK_PASSENGERSTATUSES'', ''DIM_PASSENGERSTATUSES'', ''PASSENGERSTATUS'',''PASSENGERSTATUS''),
                (''WRK_PILOTS'', ''DIM_PILOTS'', ''PILOTNAME'', ''PILOTNAME''),
                (''WRK_TICKETTYPES'', ''DIM_TICKETTYPES'', ''TICKETTYPE'', ''TICKETTYPE'')
        )
    );
    LET cur1 CURSOR FOR list_tables;
    FOR row_variable IN cur1 DO
        tmp_str := ''SELECT t.value FROM TABLE(SPLIT_TO_TABLE(\\'''' || row_variable.all_filds || ''\\'', \\'',\\'')) as t'';
        res_tmp := (EXECUTE IMMEDIATE :tmp_str);
        LET cur2 CURSOR FOR res_tmp;
        tmp_str := ''SELECT t.value FROM TABLE(SPLIT_TO_TABLE(\\'''' || row_variable.id_field || ''\\'', \\'',\\'')) as t'';
        res_tmp := (EXECUTE IMMEDIATE :tmp_str);
        LET cur3 CURSOR FOR res_tmp;   
        key_str := '''';        
        cnt := 0;
        
        FOR k IN cur3 DO            
            IF ( not cnt = 0) THEN
                key_str := key_str || '' AND '';
            ELSE
                cnt := 2;
            END IF;
            key_str := key_str ||'' t1.'' || k.value || '' = t2.'' || k.value;  
        END FOR;
        
        cnt := 0;
        query:= ''MERGE INTO TASK_8.PROD_SCHEMA.'' || row_variable.to_table || '' as t1 USING TASK_8.CLEANCING_SCHEMA.'' || row_variable.from_table || 
            '' as t2 ON '' || key_str || '' WHEN MATCHED THEN UPDATE SET '';
        ins_str := '''';
        
        FOR i IN cur2 DO
            IF ( not cnt = 0) THEN
                query:= query || '' , '';
                ins_str := ins_str || '' , '';
            ELSE
                cnt := 2;
            END IF;
            query:= query || '' t1.'' || i.value || '' = t2.'' || i.value ;
            ins_str := ins_str || '' t2.'' || i.value;      
        END FOR;
        
        query:= query || '' WHEN NOT MATCHED THEN INSERT ('' || row_variable.all_filds || '') VALUES ('' || ins_str || '')'';        
        EXECUTE IMMEDIATE :query; 
        tmp_str := row_variable.to_table;
        CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''Merge INTO '' || :tmp_str);
    END FOR;
    RETURN ''DONE!'';
 END';

 --PROD

CREATE OR REPLACE PROCEDURE TASK_8.PROD_SCHEMA.CREATE_PROD_SEQUENCES_AND_TABLES()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
AS 
'BEGIN
    CREATE OR REPLACE SEQUENCE TASK_8.PROD_SCHEMA.SEC_DIM_PASSENGERS;
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.SEC_DIM_PASSENGERS CREATED'');
    CREATE OR REPLACE TABLE TASK_8.PROD_SCHEMA.DIM_PASSENGERS(
        ID NUMBER DEFAULT TASK_8.PROD_SCHEMA.SEC_DIM_PASSENGERS.NEXTVAL,
    	PASSENGERID VARCHAR,
    	FIRSTNAME VARCHAR,
    	LASTNAME VARCHAR,
    	GENDER VARCHAR,
    	AGE NUMBER,
    	NATIONALITY VARCHAR
    ) DATA_RETENTION_TIME_IN_DAYS=30;    
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.DIM_PASSENGERS CREATED'');
    --------
    CREATE OR REPLACE SEQUENCE TASK_8.PROD_SCHEMA.SEC_DIM_FLIGHTSTATUSES;
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.SEC_DIM_FLIGHTSTATUSES CREATED'');
    CREATE OR REPLACE TABLE TASK_8.PROD_SCHEMA.DIM_FLIGHTSTATUSES(
        ID NUMBER DEFAULT TASK_8.PROD_SCHEMA.SEC_DIM_FLIGHTSTATUSES.NEXTVAL,
    	FLIGHTSTATUS VARCHAR
    ) DATA_RETENTION_TIME_IN_DAYS=30;        
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.DIM_FLIGHTSTATUSES CREATED'');
    --------
    CREATE OR REPLACE SEQUENCE TASK_8.PROD_SCHEMA.SEC_DIM_TICKETTYPES;
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.SEC_DIM_TICKETTYPES CREATED'');
    CREATE OR REPLACE TABLE TASK_8.PROD_SCHEMA.DIM_TICKETTYPES(
        ID NUMBER DEFAULT TASK_8.PROD_SCHEMA.SEC_DIM_TICKETTYPES.NEXTVAL,
    	TICKETTYPE VARCHAR
    ) DATA_RETENTION_TIME_IN_DAYS=30;    
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.DIM_TICKETTYPES CREATED'');
    --------
    CREATE OR REPLACE SEQUENCE TASK_8.PROD_SCHEMA.SEC_DIM_PASSENGERSTATUSES;
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.SEC_DIM_PASSENGERSTATUSES CREATED'');
    CREATE OR REPLACE TABLE TASK_8.PROD_SCHEMA.DIM_PASSENGERSTATUSES(
        ID NUMBER DEFAULT TASK_8.PROD_SCHEMA.SEC_DIM_PASSENGERSTATUSES.NEXTVAL,
    	PASSENGERSTATUS VARCHAR
    ) DATA_RETENTION_TIME_IN_DAYS=30;    
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.DIM_PASSENGERSTATUSES CREATED'');
    --------
    CREATE OR REPLACE SEQUENCE TASK_8.PROD_SCHEMA.SEC_DIM_PILOTS;
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.SEC_DIM_PILOTS CREATED'');
    CREATE OR REPLACE TABLE TASK_8.PROD_SCHEMA.DIM_PILOTS(
        ID NUMBER DEFAULT TASK_8.PROD_SCHEMA.SEC_DIM_PILOTS.NEXTVAL,
    	PILOTNAME VARCHAR
    ) DATA_RETENTION_TIME_IN_DAYS=30;        
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.DIM_PILOTS CREATED'');
    --------
    CREATE OR REPLACE SEQUENCE TASK_8.PROD_SCHEMA.SEC_DIM_ARRIVALAIRPORTS;
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.SEC_DIM_ARRIVALAIRPORTS CREATED'');
    CREATE OR REPLACE TABLE TASK_8.PROD_SCHEMA.DIM_ARRIVALAIRPORTS(
        ID NUMBER DEFAULT TASK_8.PROD_SCHEMA.SEC_DIM_ARRIVALAIRPORTS.NEXTVAL,
    	ARRIVALAIRPORT VARCHAR
    ) DATA_RETENTION_TIME_IN_DAYS=30;    
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.DIM_ARRIVALAIRPORTS CREATED'');
    --------
    CREATE OR REPLACE SEQUENCE TASK_8.PROD_SCHEMA.SEC_DIM_AIRPORTS;
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.SEC_DIM_AIRPORTS CREATED'');
    CREATE OR REPLACE TABLE TASK_8.PROD_SCHEMA.DIM_AIRPORTS(
        ID NUMBER DEFAULT TASK_8.PROD_SCHEMA.SEC_DIM_AIRPORTS.NEXTVAL,
    	AIRPORTNAME VARCHAR,
    	AIRPORTCOUNTRYCODE VARCHAR,
    	COUNTRYNAME VARCHAR,
    	AIRPORTCONTINENT VARCHAR,
    	CONTINENTS VARCHAR
    ) DATA_RETENTION_TIME_IN_DAYS=30;    
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.DIM_AIRPORTS CREATED'');
    --------
    CREATE OR REPLACE SEQUENCE TASK_8.PROD_SCHEMA.SEC_FCT;
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.SEC_FCT CREATED'');
    CREATE OR REPLACE TABLE TASK_8.PROD_SCHEMA.FCT_FLIES (
    	ID NUMBER DEFAULT TASK_8.PROD_SCHEMA.SEC_FCT.NEXTVAL,
    	PASSENGER_ID NUMBER,
    	AIRPORT_ID NUMBER,
    	DEPARTUREDATE DATE,
    	ARRIVALAIRPORT_ID NUMBER,
    	PILOTNAME_ID NUMBER,
    	FLIGHTSTATUS_ID NUMBER,
    	TICKETTYPE_ID NUMBER,
    	PASSENGERSTATUS_ID NUMBER
    ) DATA_RETENTION_TIME_IN_DAYS=30;
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.FCT_FLIES CREATED'');
    
    RETURN ''DONE!'';
 END';

CALL TASK_8.PROD_SCHEMA.CREATE_PROD_SEQUENCES_AND_TABLES();

CREATE OR REPLACE PROCEDURE TASK_8.PROD_SCHEMA.CREATE_STATES_OF_TABLES_1_HOURS_AGO()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
AS 
'BEGIN
    CREATE OR REPLACE VIEW TASK_8.PROD_SCHEMA.HIST_DIM_PASSENGERS
    AS
    SELECT 
        ID,PASSENGERID,FIRSTNAME,LASTNAME,GENDER,AGE,NATIONALITY
    FROM 
        TASK_8.PROD_SCHEMA.DIM_PASSENGERS
    AT(OFFSET => -60*60);
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.HIST_DIM_PASSENGERS RELOAD'');
    --------
    CREATE OR REPLACE VIEW TASK_8.PROD_SCHEMA.HIST_DIM_FLIGHTSTATUSES
    AS
    SELECT 
        ID,FLIGHTSTATUS
    FROM 
        TASK_8.PROD_SCHEMA.DIM_FLIGHTSTATUSES
    AT(OFFSET => -60*60);
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.HIST_DIM_FLIGHTSTATUSES RELOAD'');
    --------
    CREATE OR REPLACE VIEW TASK_8.PROD_SCHEMA.HIST_DIM_TICKETTYPES
    AS
    SELECT 
        ID,TICKETTYPE
    FROM 
        TASK_8.PROD_SCHEMA.DIM_TICKETTYPES
    AT(OFFSET => -60*60);
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.HIST_DIM_TICKETTYPES RELOAD'');
    --------
    CREATE OR REPLACE VIEW TASK_8.PROD_SCHEMA.HIST_DIM_PASSENGERSTATUSES
    AS
    SELECT 
        ID,PASSENGERSTATUS
    FROM 
        TASK_8.PROD_SCHEMA.DIM_PASSENGERSTATUSES
    AT(OFFSET => -60*60);
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.HIST_DIM_PASSENGERSTATUSES RELOAD'');
    --------   
    CREATE OR REPLACE VIEW TASK_8.PROD_SCHEMA.HIST_DIM_PILOTS
    AS
    SELECT 
        ID,PILOTNAME
    FROM 
        TASK_8.PROD_SCHEMA.DIM_PILOTS
    AT(OFFSET => -60*60);
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.HIST_DIM_PILOTS RELOAD'');
    --------
    CREATE OR REPLACE VIEW TASK_8.PROD_SCHEMA.HIST_DIM_ARRIVALAIRPORTS
    AS
    SELECT 
        ID,ARRIVALAIRPORT
    FROM 
        TASK_8.PROD_SCHEMA.DIM_ARRIVALAIRPORTS
    AT(OFFSET => -60*60);
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.HIST_DIM_ARRIVALAIRPORTS RELOAD'');
    --------
    CREATE OR REPLACE VIEW TASK_8.PROD_SCHEMA.HIST_DIM_AIRPORTS
    AS
    SELECT 
        ID,AIRPORTNAME,AIRPORTCOUNTRYCODE,COUNTRYNAME,AIRPORTCONTINENT,CONTINENTS
    FROM 
        TASK_8.PROD_SCHEMA.DIM_AIRPORTS
    AT(OFFSET => -60*60);
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.HIST_DIM_AIRPORTS RELOAD'');
    --------
    CREATE OR REPLACE VIEW TASK_8.PROD_SCHEMA.HIST_FCT_FLIES
    AS
    SELECT 
        ID,PASSENGER_ID,AIRPORT_ID,DEPARTUREDATE,ARRIVALAIRPORT_ID,PILOTNAME_ID,FLIGHTSTATUS_ID,TICKETTYPE_ID,PASSENGERSTATUS_ID
    FROM 
        TASK_8.PROD_SCHEMA.FCT_FLIES
    AT(OFFSET => -60*60);
    CALL TASK_8.TECH_SCHEMA.EVENT_PROC(''PROD_SCHEMA.FCT_FLIES RELOAD'');
    
    RETURN ''DONE!'';
 END';
 
CALL TASK_8.PROD_SCHEMA.CREATE_STATES_OF_TABLES_1_HOURS_AGO();

CREATE OR REPLACE PROCEDURE TASK_8.PROD_SCHEMA.CREATE_PROD_VIEW_FCT()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
AS 
'BEGIN 
    CREATE OR REPLACE VIEW TASK_8.PROD_SCHEMA.VIEW_FCT
    WITH ROW ACCESS POLICY TASK_8.TECH_SCHEMA.viewers_fct_view ON (ID)
    AS 
    SELECT * 
    FROM TASK_8.PROD_SCHEMA.FCT_FLIES;
    RETURN ''DONE!'';
END';

CALL TASK_8.PROD_SCHEMA.CREATE_PROD_VIEW_FCT();


