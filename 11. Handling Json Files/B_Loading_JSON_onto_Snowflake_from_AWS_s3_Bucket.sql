use IOT_V2;
CREATE SCHEMA s3_load_Lecture_series;

-- creating an external stage

CREATE OR REPLACE STAGE stage_Lectures
url = 's3://awsglue-datasets/examples/us-legislators/all/' /* Public URl of AWS with some json files,publically accessible*/
comment = 'US Legislature Data'; /* hence no other credentials were required*/

-- desc and listing the stage 

desc stage stage_Lectures; point no 31 -- shows the location teh stage is pointing to
list @stage_Lectures; -- there are 6 files and we dont know how these json look like

-- let's define a file format 
create or replace file format json_file_format_lectures
type = 'JSON'
compression = 'AUTO';

desc file format json_file_format_lectures;


-- let's select the external_table

select * from @stage_Lectures/areas.json (file_format => 'json_file_format_lectures') t;


-- $ notation to access the data

select t.$1:name::string as state_name from @stage_Lectures/areas.json (file_format => 'json_file_format_lectures') t;
 
select t.$1:type::strig as state_type from @stage_Lectures/areas.json (file_format => 'json_file_format_lectures') t; 


select t.$1:id::string as state_id, 
       t.$1:name::string as state_name,
       t.$1:type::string as state_type
from  @stage_Lectures/areas.json (file_format => 'json_file_format_lectures') t;
    


-- create a table with variant dtype:

create or replace table area_json
(
 area_json_x variant
);

-- we can copy the above


select * from area_json;


insert into area_json (area_json_x)
select parse_json(t.$1) as area_json_x
from @stage_Lectures/areas.json (file_format => 'json_file_format_lectures') t;

select * from area_json;  -- query time is faster than the external_table

select area_json_x:id::string as state_id,
        area_json_x:name::string as state_name, 
        area_json_x:type::string as state_type 
from area_json;



-- let's create one more table


select * from @stage_Lectures/areas.json (file_format => 'json_file_format_lectures') t

create or replace table area_std
(
  state_id string,
  state_name varchar,
  state_type varchar
);

insert into area_std(state_id,state_name,state_type) 
select 
    t.$1:id as state_id, 
    t.$1:name::varchar as state_name,
    t.$1:type::varchar as state_type 
from @stage_Lectures/areas.json (file_format => 'json_file_format_lectures')t;

select * from area_std;



-- You can directly create a table without copying as well 

create table xyg as
select 
    t.$1:id::string as state_id,
    t.$1:name::varchar as state_name,
    t.$1:type::varchar as state_type
from @stage_Lectures/areas.json (file_format => 'json_file_format_lectures')t;

select * from xyg;


----------------------------------------------------------------------------------------------------------------------------------\

-- let's go for another file 

list @stage_Lectures;

select * from @stage_Lectures/persons.json (file_format => 'json_file_format_lectures') t;

create or replace table person_json
(
  person_json_x variant
); 


insert into person_json(person_json_x) 
select parse_json(t.$1) as person_json_x
from @stage_Lectures/persons.json (file_format => 'json_file_format_lectures') t; 

select * from person_json;



select 
  person_json_x:id::string as id,
  person_json_x:given_name::string as given_name,
  person_json_x:family_name::string as family_name,
  person_json_x:name::string as name,
  person_json_x:birth_date::date as dob,
  CASE
    WHEN person_json_x:gender::string = 'male' then 'M'
    WHEN person_json_x:gender::string = 'Female' then 'F'
    ELSE 'U'
    END AS gender
from person_json;



