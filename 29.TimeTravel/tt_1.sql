CREATE OR REPLACE DATABASE LEARNINGS;
CREATE OR REPLACE SCHEMA dummy;

CREATE OR REPLACE TABLE t_1
(
    id integer
);



CREATE OR REPLACE TABLE t_2 CLONE t_1;

CREATE OR REPLACE TABLE t_3 CLONE t_2;

CREATE OR REPLACE TABLE t_4 CLONE t_3;


SELECT CURRENT_DATABASE();


SELECT * FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
WHERE TABLE_CATALOG = CURRENT_DATABASE()
AND TABLE_NAME = 'T_1'; -- for clone => Clone_grp_id << id;
-- also active_bytes for main table t1 at present = 0 

-- Lets insert: 

INSERT INTO t_1 (ID)
VALUES 
  (1), (2), (3), (4), (5),
  (6), (7), (8), (9), (10),
  (11), (12), (13), (14), (15),
  (16), (17), (18), (19), (20),
  (21), (22), (23), (24), (25),
  (26), (27), (28), (29), (30);


SELECT * FROM T_1;

-- RE RUN:

SELECT * FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
WHERE TABLE_CATALOG = CURRENT_DATABASE(); 


-- TIME TRAVEL:

CREATE OR REPLACE TABLE t6
(
    ID NUMBER,
    NAME VARCHAR,
    CITY VARCHAR
);

INSERT INTO t6 VALUES
(1,'PRAVEEN','HYD'),
(2,'KUMAR','CHN'),
(3,'RAM','BNG');

select * from t6; 

-- let's update; 
update t6 set CITY = 'PUNE'; -- 01bfbe6e-3202-044c-0000-0010b69a5f45 ;

select * from t6; -- all updated



SELECT * FROM t6 BEFORE (STATEMENT => '01bfbe6e-3202-044c-0000-0010b69a5f45');

-- Let's create a temp table:

create or replace temporary table temp_timestamp
(
ID NUMBER,
    NAME VARCHAR,
    CITY VARCHAR
);

INSERT INTO temp_timestamp
Select * from t6 BEFORE (STATEMENT => '01bfbe6e-3202-044c-0000-0010b69a5f45');

select * from temp_timestamp;


-- merge for teh updates
merge into t6 as ta
using temp_timestamp as s 
on ta.id = s.id
when matched
then update
set  ta.name = s.name,
ta.city = s.city;



select * from t6; -- city restored.


-- Data Retentiuon Time:

--> permanent Table - 90 Days
--> Transient Table - 90 Days
--> Temporary Table - 90 Days

-- How to Change: 
    alter table table_name set data_retention_time_in_days = 0;
    alter table table_name set data_retention_time_in_days = 0; 
    alter table table_name set data_retention_time_in_days = 0; 

-- SHOW TABLES:

show tables;
   
    

