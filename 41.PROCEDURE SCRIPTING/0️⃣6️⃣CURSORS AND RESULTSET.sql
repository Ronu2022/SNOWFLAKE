CREATE OR REPLACE DATABASE CURSORS; 
CREATE OR REPLACE SCHEMA CURSOR_SCHEMA;

/* CURSOR :

- used to iterate through the rows of a table or a view or a resultset onbe row at a time. 
- mostly we use cursors to do row by row processing of data.
*/

/* EXAMPLE:

1. simple cursor:
declare c1 cursor for select emp_id,salary from employees; 
(OR)
begin
let c1 cursor for select empid, salary from employees;

2. opening cursor at teh run time: 

declare
    cur cursor for select empid, salary from table(?) where dept = ?; 
    deptid integer default 10; 
begin
    open cur using('employees', :deptid);
    -- statements..
    CLOSE cur; 
    return; 
end; 
*/
select * from EMP.hrdata.employees;
select * from EMP.hrdata.departments ;
select * from EMP.hrdata.locations ;
select * from EMP.hrdata.countries ;
select * from EMP.hrdata.regions ;
select * from EMP.hrdata.jobs ;
select * from EMP.hrdata.job_history ;



create or replace procedure total_sales()
returns varchar
language sql
execute as caller as
$$
Declare
    cur CURSOR for select e.employee_id, e.first_name, e.last_name,e.salary, d.department_name
                    from  EMP.hrdata.employees as e
                    join  EMP.hrdata.departments as d on e.dept_id = d.dept_id where upper(d.DEPARTMENT_NAME) = 'SALES' ;   
                    
    total_sal integer default 0;
    sal integer;
begin 
    for rec in cur 
    do
        sal := rec.SALARY; 
        total_sal := total_sal + sal; 
    end for; 
return 'The Total Salary is '|| :total_sal; 
end;
$$;


CALL total_sales();

-- fIND THE SALARY FOR ANY GIVEN DEPT:

CREATE OR REPLACE PROCEDURE PROC_NAME_SALARY(DEPT VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER AS
$$
DECLARE
    C1 CURSOR FOR select e.employee_id, e.first_name, e.last_name,e.salary, d.department_name
                    from  EMP.hrdata.employees as e
                    join  EMP.hrdata.departments as d on e.dept_id = d.dept_id where upper(d.DEPARTMENT_NAME) = UPPER(?);

    DNAME VARCHAR;
    SAL INT;
    TOTAL_SAL INT DEFAULT 0; 
    

BEGIN
    OPEN C1 USING(:DEPT);

    FOR REC IN C1 DO
       
        SAL := REC.SALARY;
        TOTAL_SAL := TOTAL_SAL + SAL;
    END FOR;
    CLOSE C1;
RETURN 'THE TOTAL SALARY IS: '|| :TOTAL_SAL;
END;
$$;

CALL PROC_NAME_SALARY('salES');



------------------------------------------------------------------------------------------------------------------------------------------------

/* RESULT SET */

-- SQL DATATYPE -> POINT TOWARDS TEH RESULTS OF A QUERY.
-- WE CAN DO EITHER USE TABLE(RESULT_SET) TO RETRIEVE TEH RESULTS AS A QUERY.
-- ITERATE OVER THE RESULTSET WITH A CURSOR;
-- TO RETURN A RESULT SET YU NEED TO CONVERT IT INTO A TABLE AT FIRST
    --- RETURN TABLE(RESULT_SET_NAME);
    --IF WE WANTT TO CONVERT A CURSOR TO A RESULTSET WE FIRST HAVE TO CONVERT IT TO A RESULTSET AND THEN USE TABLE LITERAL LIKE SHOWN BELOW.
    


-- FIND TOP N SALARIED PERSON
select * from EMP.hrdata.employees;

CREATE OR REPLACE PROCEDURE top_n_salary(DEPT VARCHAR, N INT)
RETURNS TABLE(NAME VARCHAR, DEPT_NAME VARCHAR, SALARY INT, D_RANK INT)
LANGUAGE SQL
EXECUTE AS CALLER AS
    DECLARE
        Q VARCHAR;
        RES RESULTSET;
    BEGIN
        Q := '
        SELECT CONCAT(e.first_name, '' '', e.last_name) AS name,
               d.department_name      AS dept_name,
               e.salary,
               DENSE_RANK() OVER (ORDER BY e.salary DESC) AS d_rank
        FROM  EMP.hrdata.employees   e
        JOIN  EMP.hrdata.departments d
              ON e.dept_id = d.dept_id
        WHERE UPPER(d.department_name) = UPPER(?)          -- 1st bind
        QUALIFY DENSE_RANK() OVER (ORDER BY e.salary DESC) <= ?  -- 2nd bind
    ';
 
                  
    RES := (EXECUTE IMMEDIATE :Q USING (DEPT,N));
    RETURN TABLE(RES);
    END;


CALL top_n_salary('SALES',2);
    



--------------------------------------------------------------------------------------------------------------------------------------

            







