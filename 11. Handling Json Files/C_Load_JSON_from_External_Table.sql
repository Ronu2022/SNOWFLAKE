CREATE DATABASE external_stage_json;


-- In sf there are 3 internal stages: 
  -- user stage refereed as @~
  -- table stage regeered as @%table-name
  -- named internal stage @stage-name

  create or replace file format json_format
  type = 'json'
  compression = 'auto';

  create or replace stage json_stage
  url = 's3://awsglue-datasets/examples/us-legislators/all/'
  file_format = json_format;


  show stages;
  list @json_stage;


  select * from @json_stage/memberships.json; -- select the file in the satge

create table json_file
(
  x_var variant
);

insert into json_file(x_var)
select parse_json(t.$1) as x_var
from @json_stage/memberships.json (file_format => 'json_format')t;



select * from json_file; 


select x_var:person_id::varchar as person_id,
x_var:area_id::varchar  as area_id,
case when x_var:person_id::varchar  <> x_var:area_id::varchar then 'NO'
ELSE 'YES'
END AS flag
from json_file order by 1 desc;



list @json_stage;

select * from @json_stage/organizations.json ;

select t.$1:identifiers[0] from @json_stage/organizations.json (file_format => 'json_format')t;

select t.$1:identifiers[0] from @json_stage/organizations.json (file_format => 'json_format')t;

select t.$1:classification::varchar from @json_stage/organizations.json (file_format => 'json_format')t;

select t.$1:identifiers from @json_stage/organizations.json (file_format => 'json_format')t;

select t.identifiers[0] from @json_stage/organizations.json (file_format => 'json_format')t;






