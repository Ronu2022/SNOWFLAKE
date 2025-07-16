CREATE OR REPLACE DATABASE PROC_EXCEPTIONS;
CREATE OR REPLACE SCHEMA CLASS;

-- EXCEPTIONS PRVENETS NEXT LINES OF CODE FROM EXECUTING
-- WE CAN HANDLE EXCEPTIONS USING HANDLERS
-- WE CAN RAISE OUR EXCEPTIONS AS WELL/
-- WE CAN DECLARE EXCEPTIONS IN DECLARATION SECTION
    -- declare exception_name exception(error_code, 'error_message');
    -- here code should be between -20999 to -2001


select * from EMP.hrdata.employees;
select * from EMP.hrdata.departments ;
select * from EMP.hrdata.locations ;
select * from EMP.hrdata.countries ;
select * from EMP.hrdata.regions ;
select * from EMP.hrdata.jobs ;
select * from EMP.hrdata.job_history ;



// WRITE TEH PROICEDURE THAT GETS YOU TOTAL COUNT OF EMPLOYEES IN A PARTICULARC COUNTRY
CREATE OR REPLACE PROCEDURE GET_MANAGER_COUNT(CON_CODE VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER AS
$$
DECLARE
    NO_DATA_FOUND EXCEPTION (-20101,'No Data Found in the EMployee Table.');
    CNT INT;

BEGIN
    SELECT COUNT(*) INTO :CNT FROM  EMP.hrdata.employees;
    IF(:CNT = 0) THEN
        RAISE NO_DATA_FOUND; 
    END IF; 

    SELECT COUNT(*) INTO :CNT FROM
    (
        SELECT E.EMPLOYEE_ID,E.DEPT_ID,D.DEPARTMENT_NAME,D.LOCATION_ID,L.COUNTRY_ID,C.COUNTRY_NAME, E.FIRST_NAME,E.LAST_NAME,
        FROM EMP.hrdata.employees AS E
        JOIN EMP.hrdata.departments AS D ON E.DEPT_ID = D.DEPT_ID
        JOIN EMP.hrdata.locations AS L ON D.LOCATION_ID = L.LOCATION_ID
        JOIN EMP.hrdata.countries  AS C ON L.COUNTRY_ID = C.COUNTRY_ID
        WHERE UPPER(L.COUNTRY_ID) = UPPER(:CON_CODE)
    ) AS ALIAS_NAME;

    RETURN 'NO OF EMPLOYEES IN THE GIVEN COUNTRY: '|| CNT;

    EXCEPTION
    WHEN statement_error THEN
    RETURN OBJECT_CONSTRUCT('ERROR TYPE','statement error',
                            'SQLCODE', sqlcode,
                            'SQLERRM',sqlerrm,
                            'SQLSTATE',sqlstate);
    WHEN expression_error THEN
    RETURN OBJECT_CONSTRUCT('ERROR TYPE', 'EXPRESSION ERROR',
                            'SQLCODE',sqlcode,
                            'SQLERRM',sqlerrm,
                            'SQLSTATE',sqlstate);

    WHEN no_data_found THEN
    RETURN OBJECT_CONSTRUCT('ERROR TYPE', 'user defined exception',
                            'SQL CODE', sqlcode,
                            'SQLERRM',sqlerrm,
                            'SQLSTATE',sqlstate);
                            

END; 
$$;




CALL GET_MANAGER_COUNT('RON');


select * from EMP.hrdata.employees;
select * from EMP.hrdata.departments ;
select * from EMP.hrdata.locations ;
select * from EMP.hrdata.countries ;
select * from EMP.hrdata.regions ;
select * from EMP.hrdata.jobs ;
select * from EMP.hrdata.job_history ;




// WRITE A PROCEDURE THAT GIVES YOU THE TOTAL COUNT OF EMPLOYEES IN A PARTICULAR COUNTRY AND MAINTAIN A LOG TABLE

create or replace sequence  EMP.hrdata.seq
start = 1
increment = 1;

CREATE OR REPLACE TABLE EMP.hrdata.procedure_log
(
   id int default EMP.hrdata.seq.nextval,
   date_time timestamp_ntz default current_timestamp,
   procedure_name varchar default 'GET_MANAGER_COUNT',
   error_type varchar,
   error_code varchar,
   error_message varchar,
   sql_state varchar
) Comment = 'This is a log table for the procedure get_manager_count.';


create or replace procedure PROC_EXCEPTIONS.CLASS.get_manager_count(country_code VARCHAR)
returns varchar
language sql
execute as caller as 
$$
declare
    no_data_found EXCEPTION(-20101,'No Data Found in the EMployee Table');
    cnt int;
    rec_count int;

begin
    select count(*) into cnt from EMP.hrdata.employees;
    if (:cnt = 0) THEN 
    raise no_data_found;
    end if; 
    
    SELECT COUNT(*) INTO :rec_count FROM
        (
            SELECT E.EMPLOYEE_ID,E.DEPT_ID,D.DEPARTMENT_NAME,D.LOCATION_ID,L.COUNTRY_ID,C.COUNTRY_NAME, E.FIRST_NAME,E.LAST_NAME,
            FROM EMP.hrdata.employees_2 AS E -- tested with employees_2 to check the results in lo, actually this shiould have been only EMP.hrdata.employees
            JOIN EMP.hrdata.departments AS D ON E.DEPT_ID = D.DEPT_ID
            JOIN EMP.hrdata.locations AS L ON D.LOCATION_ID = L.LOCATION_ID
            JOIN EMP.hrdata.countries  AS C ON L.COUNTRY_ID = C.COUNTRY_ID
            WHERE UPPER(L.COUNTRY_ID) = UPPER(:country_code)
        ) AS ALIAS_NAME;
    return  'The Count of employees: ' || rec_count;

EXCEPTION 
    WHEN  statement_error THEN 
        INSERT INTO EMP.hrdata.procedure_log(error_type,error_code,error_message,sql_state)
        values('Statement Error',:sqlcode,:sqlerrm,:sqlstate);

    WHEN  expression_error THEN
        INSERT INTO  EMP.hrdata.procedure_log(error_type,error_code,error_message,sql_state)
        values('Expression_Error',:sqlcode,:sqlerrm,:sqlstate);

    WHEN no_data_found THEN
        INSERT INTO EMP.hrdata.procedure_log(error_type,error_code,error_message,sql_state)
        VALUES('User Defined Exception',:sqlcode,:sqlerrm,:sqlstate);

end;
$$;


select * from EMP.hrdata.procedure_log; -- 0 records.

CALL PROC_EXCEPTIONS.CLASS.get_manager_count('RAJA'); -- no results.

select * from EMP.hrdata.procedure_log;  -- Look at teh log -- Statement Error came in. becasuse error in statement

/* 
ID	DATE_TIME	                  PROCEDURE_NAME	                  ERROR_TYPE	    ERROR_CODE	 ERROR_MESSAGE	                                                                           SQL_STATE
1	2025-07-15 23:13:15.234	     GET_MANAGER_COUNT	                  Statement Error	2003	     SQL compilation error:Object 'EMP.HRDATA.EMPLOYEES_2' does not exist or not authorized.    42S02
	
*/






        
