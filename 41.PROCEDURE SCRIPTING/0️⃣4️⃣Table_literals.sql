create or replace database proc_table_lietrals_db;
create or replace schema practise;

/*TABLE LITERALS */ 

-- when we pass the table name as parameter
/* 
 lets say create or replace stored procedure (table_name)
 ---
 ---
 select * from table(:Table_name); -- this is table literal;

 -- Syntax:
 TABLE(string_literal | session_variable | bind_Variable ) -> String literal = Fully Qualified Table Name
 it could be any name; 
-------------------------------------------------------------------------------------------------------------------------------------------------------------

-- EXAMPLES:

select * from table('table_name');
select * from table('db_name.schema_name.table_name');

set myvar = 'my_table'; -- recall my var is a session variable
select * from table($myvar) -- $myvar => hence $myvar


select * from table(?);
select * from table(:bind_variable);

--------------------------------------------------------------------------------------------------------------------------------------------------------------

// IDENTIFIERS:

-- for table name parameters we can use table literals
-- but for other objects like database name , schema name etc, we woudl need identifiers.
-- Tabel literals are used only in from caluse, but identifiers can be used in any part of teh statement.
-- IDENTIFIER(string_lietral| Session_variable | bind_variable | sql variable)
-- examples:
    create or replace database itendifier('my_db');
    set schema_name = 'my_db.my_schema';
    use schema identifier($schema_name);
    use schema identifier($schema_name);  -- yoiu cant simply say use schema $schema_name => all though thats a session variable 
    you can use it through identifier
*/


/*
✅ When to use IDENTIFIER() in Snowflake Scripting
Use Case	                                                                     Use IDENTIFIER(...)?	        Example
--------------------------------------------------------------------------------------------------------------------------------------------------------
DDL / DML object names (used in FROM, CREATE, DROP, INSERT INTO, MERGE INTO)	     ✅ Yes	                 FROM IDENTIFIER(:table_name)
CREATE TABLE IDENTIFIER(:full_table_name)

used in filtering (WHERE, SELECT) or values	                                         ❌ No	                WHERE table_catalog = :db_name
                                                                                                            SELECT :col_name

*/
    
-------------------------------------------------------------------------------------------------------------------------------------------------
select * from EMP.hrdata.employees;
select * from EMP.hrdata.departments ;
select * from EMP.hrdata.locations ;
select * from EMP.hrdata.countries ;
select * from EMP.hrdata.regions ;
select * from EMP.hrdata.jobs ;
select * from EMP.hrdata.job_history ;



SET DB = 'EMP';
SET TBL_SCHEMA = 'hrdata';
SET SCHEMA_DUMMY = 'TRIAL_SCHEMA';

USE DATABASE $DB; -- NOT CORRECT; 
USE DATABASE IDENTIFIER($DB); -- CORRECT; 

CREATE OR REPLACE SCHEMA $SCEHMA_DUMMY; -- NOT CORRECT;

SET FULL_SCHEMA_NAME = $DB||'.'||$SCHEMA_DUMMY; -- STEP 1
CREATE OR REPLACE SCHEMA IDENTIFIER($FULL_SCHEMA_NAME); -- STEP 2 -- CORRECT!!


USE SCHEMA IDENTIFIER($TBL_SCHEMA);


-- WRITE A PROCEDURE TO FIND THE ROW COUNTS OF TABL;ES IN EMP DATABASE

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
WHERE TABLE_CATALOG = 'EMP';

select TABLE_NAME , ROW_count FROM 
SNOWFLAKE.ACCOUNT_USAGE.TABLES WHERE TABLE_CATALOG = 'EMP';



CREATE OR REPLACE PROCEDURE SHOW_TABLE_ROWS(DB_NAME VARCHAR)
RETURNS TABLE(table_name VARCHAR, row_count NUMBER)
LANGUAGE SQL
AS
$$
BEGIN
    DECLARE 
        query_string STRING;
        rs RESULTSET;
   begin
        set  query_string := 'SELECT table_name, row_count FROM snowflake.account_usage.tables WHERE table_catalog = ''' || :DB_NAME || '''';

        rs := (EXECUTE IMMEDIATE :query_string);
        RETURN TABLE(rs);
  end;
END;
$$;

CALL SHOW_TABLE_ROWS('EMP');


----------------------------------------------------------------------------------------------------------------------------------
    
    
CREATE OR REPLACE PROCEDURE SHOW_TABLE_ROWS(DB_NAME VARCHAR)
RETURNS TABLE(table_name VARCHAR, row_count NUMBER)
LANGUAGE SQL
AS
BEGIN
    DECLARE query_string STRING;
    DECLARE rs RESULTSET;
    SET query_string := 'SELECT table_name, row_count FROM snowflake.account_usage.tables WHERE table_catalog = ''' || :DB_NAME || '''';
    rs := (EXECUTE IMMEDIATE :query_string);
    RETURN TABLE(rs);
END;

CALL SHOW_TABLE_ROWS('EMP');


