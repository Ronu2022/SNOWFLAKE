create or replace database practise;
create or replace schema practise.pra_schema;

create or replace table practise.pra_schema.dummy_table
(
    cx_id int,
    date_track timestamp_ntz default current_timestamp,
    name varchar,
    qty int
    
);

CREATE OR REPLACE TABLE practise.pra_schema.SP_ERROR_LOGS
(
    TIME_STAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    PROC_NAME VARCHAR,
    ERROR_TYPE VARCHAR, 
    ERROR_CODE VARCHAR,
    ERROR_MESSAGE VARCHAR, 
    SQL_STATE VARCHAR
    
);

create or replace procedure practise.pra_schema.insert_proc(t_name varchar,c int,n varchar, q int)
returns varchar
language sql
execute as caller as
    declare
        qs varchar;
    begin
        qs := 'INSERT INTO '|| t_name ||' (cx_id,name,qty) values(' ||c||','||''''||n||''''||','||q||')';
        -- 'INSERT INTO '|| t_name||' (cx_id,name,qty) values('||c||','||n ||','||q||')';(error)
            BEGIN
                execute immediate :qs;
                COMMIT;
            EXCEPTION

                when statement_error then
                    ROLLBACK;
                    INSERT INTO practise.pra_schema.SP_ERROR_LOGS(PROC_NAME, ERROR_TYPE, ERROR_CODE, ERROR_MESSAGE, SQL_STATE)
                             values('practise.pra_schema.insert_proc','STATEMENT_ERROR',:SQLCODE,:SQLERRM,:SQLSTATE);
                    
                    Return 'Statement_Error';

                 WHEN expression_error then
                    ROLLBACK;
                    INSERT INTO practise.pra_schema.SP_ERROR_LOGS(PROC_NAME, ERROR_TYPE, ERROR_CODE, ERROR_MESSAGE, SQL_STATE)
                             values ('practise.pra_schema.insert_proc','EXPRESSION_ERROR',:SQLCODE,:SQLERRM,:SQLSTATE);
                    Return 'Expression_Error';
                
                WHEN other then
                    ROLLBACK;
                    INSERT INTO practise.pra_schema.SP_ERROR_LOGS(PROC_NAME, ERROR_TYPE, ERROR_CODE, ERROR_MESSAGE, SQL_STATE)
                                VALUES('practise.pra_schema.insert_proc','OTHERS',:sqlcode,:SQLERRM,:SQLSTATE);
                    RETURN 'ERROR HENCE ROLLED BACK';
            END;
    RETURN 'SUCCESS';
    END;


CALL practise.pra_schema.insert_proc('practise.pra_schema.dummy_table',111,'Raju',120); -- gives statement error

select * from practise.pra_schema.SP_ERROR_LOGS; -- error_message 
/* SQL compilation error: error line 1 at position 71
invalid identifier 'RAJU'*/ -- observe we have given the name idf

-- observe
/*
qs := 'INSERT INTO '|| t_name||' (cx_id,name,qty) values('||c||','||n ||','||q||')'
in this line the name param is not quoted
correct should be  
qs := 'INSERT INTO '|| t_name||' (cx_id,name,qty) values('||c ||','''||n'' ||','||q||')'
repl,aced to qs := 'INSERT INTO '|| t_name ||' (cx_id,name,qty) values(' ||c||','||''''||n||''''||','||q||')';
*/

CALL practise.pra_schema.insert_proc('practise.pra_schema.dummy_table',111,'Raju',120); -- SUCCESS; 
