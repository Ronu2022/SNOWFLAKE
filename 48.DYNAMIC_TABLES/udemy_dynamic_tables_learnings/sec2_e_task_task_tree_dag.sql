-- change context
use role sysadmin;          -- use sysadmin role
use schema demo_db.public;  -- use default public schema
use warehouse compute_wh;   -- use compute warehouse


-- creating a simple task that runs every hours

create or replace task task_a
schedule = '1 minute'
as
select current_database();


create or replace task task_b
warehouse = compute_wh
after task_a
as
select current_schema(); 

create or replace task task_c
warehouse = compute_wh
after task_b
as 
select current_warehouse(); 

create or replace task task_d
warehouse = compute_wh
after  task_b
as 
select current_region(); 


create or replace task task_e
warehouse = compute_wh
after  task_c, task_d
as 
select current_timestamp(); 


// let's resume tasks: 
--> remember: for resumeing task , you need to start from teh children
--> else would throw error.
--> for eg , task a resumed, if you then do task b resume - throw error then you have to suspend task_a, then resume task_b then task_a
alter task task_e resume; -- was teh last of tasks, hence first resumption
alter task task_d resume; -- task_d
alter task task_c resume;
alter task task_b resume;
alter task task_a resume;

// suspend: 

alter task task_a suspend;
alter task task_b suspend; 
alter task task_c suspend; 
alter task task_d suspend; 
alter task task_e suspend; 


-- checked teh status for task_a, was failed
/* Cannot execute task , EXECUTE MANAGED TASK privilege must be granted to owner role*/ 

select current_role(); --SYSADMIN 
--> SYSADMIN has to priveleges or grants to execute task 

use role accountadmin;
grant execute task, execute managed task on account to role sysadmin;

use role sysadmin;

grant execute task, execute managed task on account to role sysadmin; 

--> there are task where you give warehouse = warehouse-name -> these are tasks
--> if you givge warehouse = Null => these are serverless task managed by snwoflake , and serverless usage 

show grants on role sysadmin;

show grants to role sysadmin;




// No Resume: 
alter task task_e resume; -- was teh last of tasks, hence first resumption
alter task task_d resume; -- task_d
alter task task_c resume;
alter task task_b resume;
alter task task_a resume;



alter task task_a suspend;
alter task task_b suspend; 
alter task task_c suspend; 
alter task task_d suspend; 
alter task task_e suspend; 



