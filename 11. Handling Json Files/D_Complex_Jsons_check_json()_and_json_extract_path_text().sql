
-- Unless the structure of the Json is valid, querrying wont work 
-- query json : using the langubgae that uyou use - here sql  also called as processing json or qorking with json 

-- creating an employee table


create or replace transient table employee_json_01 (
    json_data variant
);

-- lets insert sample data
insert into employee_json_01
select parse_json(
    ' {"employee": 
        {
            "id": 1,
            "name": "John Doe",
            "designation": "Software Engineer",
            "department": "IT",
            "salary": 60000,
            "joining_date": "01-01-2020",
            "is_manager":false,
            "is_contractor":null
    
        }}
    '
);

-- execute select sql statement 

select  * from employee_json_01;

-- usage of cololon notation to access root element or elements:
select json_data:employee from employee_json_01;

-- acccessing the 2nd level element:
select json_data:employee:id from employee_json_01;

select 
    json_data:employee:id::integer as emp_id,
    json_data:employee:name::varchar as name,
    json_data:employee:designation::varchar as designation,
    json_data:employee:department::varchar as department,
    json_data:employee:salary::float as salary,
    json_data:employee:joining_date::varchar as doj,
    json_data:employee:is_manager::boolean as manager_Status,
    json_data:employee:is_contractor::boolean as is_contractor
from employee_json_01;


-- lets understand the BRACKET  NOTATION:

-- alternative to colon notation


select json_data:employee from employee_json_01;
select json_data['employee'] from employee_json_01; -- the key text is case sensitive

select JSON_DATA['employee'],json_data['Employee'] from employee_json_01;  

-- how to access next to root level element
-- we can say 2nd level elements

select json_data['employee']['name'] from employee_json_01; 

-- data type and alias
select json_data['employee']['name']::varchar as name from employee_json_01;  

-- extracting all the element from json file using bracket
select 
    json_data['employee']['id']::varchar as emp_id,
    json_data['employee']['name']::varchar as name,
    json_data['employee']['designation']::varchar as designation,
    json_data['employee']['salary'] as salary,
    to_date(json_data['employee']['joining_date']::varchar,'DD-MM-YYYY') as doj ,
    json_data['employee']['is_manager']::boolean as is_manager,
    json_data['employee']['is_contractor']::boolean as is_contractor
    from employee_json_01;
    
    
-- colon, brackcet & dot, mixing them together
select 
    json_data:employee.id as emp_id,
    json_data:employee:name::varchar as name,
    json_data:employee['designation']::varchar as designation,
    json_data['employee'].salary as salary,
    to_date(json_data['employee']['joining_date']::varchar,'DD-MM-YYYY') as doj,
    json_data['employee']['is_manager']::boolean as is_manager,
    json_data['employee']['is_contractor']::boolean as is_contractor
    from employee_json_01;


-------------------------------------------------------------------------------------------------------------------------------------

/* HANDLING COMPLEX JSON DATA*/

-- creating an employee table
create or replace transient table employee_json_02 (
    json_data variant
);

-- lets insert sample data
insert into employee_json_02
select parse_json(
    ' {
    "employees": [
        {
            "id": "1",
            "name": "John Doe",
            "designation": "Software Engineer",
            "department": "IT",
            "salary": 60000,
            "joining_date": "01-01-2020",
            "education": [
                {
                    "degree": "Bachelor of Science in Computer Science",
                    "university": "University of XYZ",
                    "year": "2010-2014"
                },
                {
                    "degree": "Master of Science in Computer Science",
                    "university": "University of ABC",
                    "year": "2015-2017"
                }
            ],
            "skills": [
                "Java",
                "Python",
                "JavaScript",
                "SQL"
            ],
            "previous_experience": [
                {
                    "company": "Company A",
                    "position": "Software Developer",
                    "duration": "2 years"
                },
                {
                    "company": "Company B",
                    "position": "Senior Software Developer",
                    "duration": "3 years"
                }
            ]
        }
    ]}
    '
);

select check_json(json_data) from employee_json_02; -- null thus correct -- explanation below




-- execute select SQL statement
select json_data from employee_json_02;

--accessing root elements
select json_data:employees from employee_json_02;
select json_data:employees[0] from employee_json_02;
select json_data:employees[0]:department from employee_json_02; -- mark [0] forllowed by departemnet
select json_data:employees[0]:joining_date from employee_json_02;
select json_data:employees[0]:name::varchar from employee_json_02;



-- all elements:

select 
    json_data:employees[0]:id::varchar as emp_id, 
    json_data:employees[0]:name::varchar as name,
    json_data:employees[0]:designation::varchar as designation,
    json_data:employees[0]:salary::float as salary,
    to_date(json_data:employees[0]:joinin_date::varchar,'DD-MM-YYY') as doj,
    json_data:employees[0]:education[0]:degree::varchar as degree1,
    json_data:employees[0]:education[0]:university::varchar as university1,
    json_data:employees[0]:education[0]:year::varchar as passout_year_1,
    json_data:employees[0]:education[1]:degree::varchar as degree2,
    json_data:employees[0]:education[1]:university::varchar as university2,
    json_data:employees[0]:education[1]:year::varchar as passout_year_2,
    json_data:employees[0]:previous_experience[0]:company::varchar as compnay_1, -- previous experience
    json_data:employees[0]:previous_experience[0]:duration::varchar as duration_1,
    json_data:employees[0]:previous_experience[0]:position::varchar as position_1,
    json_data:employees[0]:previous_experience[1]:company::varchar as company_1,
    json_data:employees[0]:previous_experience[1]:duration::varchar as duration_2,
    json_data:employees[0]:previous_experience[1]:position::varchar as position_2,
    json_data:employees[0]:skills[0]::varchar as subject_1, -- skills
    json_data:employees[0]:skills[1]::varchar as subject_2,
    json_data:employees[0]:skills[2]::varchar as subject_3,
    json_data:employees[0]:skills[1]::varchar as subject_4

  from employee_json_02;
    
-- note this previous experinece and skills section could also be kept as an object and array respectively


 select 
    json_data:employees[0]:id::varchar as emp_id, 
    json_data:employees[0]:name::varchar as name,
    json_data:employees[0]:designation::varchar as designation,
    json_data:employees[0]:salary::float as salary,
    to_date(json_data:employees[0]:joinin_date::varchar,'DD-MM-YYY') as doj,
    json_data:employees[0]:education[0]:degree::varchar as degree1,
    json_data:employees[0]:education[0]:university::varchar as university1,
    json_data:employees[0]:education[0]:year::varchar as passout_year_1,
    json_data:employees[0]:education[1]:degree::varchar as degree2,
    json_data:employees[0]:education[1]:university::varchar as university2,
    json_data:employees[0]:education[1]:year::varchar as passout_year_2,
    json_data:employees[0]:previous_experience[0]::object as previous_experience_1,
    json_data:employees[0]:previous_experience[1]::object as previous_experience_2,  -- represented with {}
    json_data:employees[0]:skills::array as skills -- represented with []

  from employee_json_02;   
    
------------------------------------------------------------------------------------------------------------------------------------

/*USING JSON FUNCTIONS*/

/*CHECK_JSON()*/

-- lets understand the use of 
-- check_json() SQL function
-- creating an employee table

create or replace  table employee_json_03
(
    json_as_str varchar()
);

-- lets insert sample data
insert into employee_json_03
values(
    ' {"employee": 
        {
            "id": 1,
            "name": "John Doe",
            "designation": "Software Engineer",
            "department": "IT",
            "salary": 60000,
            "joining_date": "01-01-2020",
            "is_manager":false,
            "is_contractor":null
    
        }}
    '
);

insert into employee_json_03
values(
    ' {"employee": 
        {
            "id": 2,
            "name": "Jane Smith",
            "designation": "Manager",
            "department": "Finance",
            "salary": 80000,
            "joining_date": "01-02-2021",
            "is_manager":false,
            "is_contractor":null
    
        }}
    '
);

insert into employee_json_03  -- note here we are not using parse_json
values(
    ' {"employee": 
        {
            "id": 3,
            "name": "Bob Johnson",
            "designation": "Data Analyst",
            "department": "Data Science",
            "salary": 70000,
            "joining_date": "01-03-2022",
            "is_manager":false 
            "is_contractor":null
    
        }}
    '
); -- in this look : after is_manage, there is no comma: so we left that deliberately.
-- but still it goes because we have not used it as a parse_json

-- parse_json: ensures that the database considers it as a json object and once it is a json object, querying data becomes easy:
   -- as was shown above ways using : or dot noation
-- without parse_json, database considers this a a normal textfile.


select * from employee_json_03;

select json_as_str:employee[0]:id from employee_json_03; -- shows error because it is no longer a json object and we had not used parse_json during insertion.


-- thus to check if it is valid json or not when there is a text based string type entry without parsing 

select check_json(json_as_str) from employee_json_03; -- gives 2 nulls that is 2 insertions are okay
-- the third is not shown : thus it has error in it null = okay and correct.

------------------------------------------------------------------------------------------------------------------------------------

/*JSON_EXTRACT_PATH_TEXT*/



create or replace  table json_04 
(
    id number,
    json_data variant
);

-- quick check with path json

insert into json_04 
select 1, parse_json('{"root_A1": "A1_value"}');

insert into json_04 
select 2, parse_json('{"root_B1": "B1_value"}');

insert into json_04 
select 3, parse_json('{"root_A1": {"root_A1_L2": "A1_L2_value"}}');

insert into json_04 
select 4, parse_json('{"root_B1": {"root_B1_L2": "B1_L2_value"}}');

insert into json_04 
select 5, parse_json('
                 {"root_A1": 
                    {"root_A1_L2": 
                        ["zero", "one", "two"]
                    }
                 }');

-- note in the above ["zeo","one","two"] these are array types data 
-- recall object was uing {}
-- array was using []

                 
insert into json_04 
select 6, parse_json('
                 {"root_B1": 
                    {"root_B1_L2": 
                        ["zero", "one", "two"]
                    }
                 }');


select * from json_04;



--json_extract_path_text()
  -- it checks if particular element exists;
  -- if it does, it returns its value
  -- if it doesnt returns null.


select id,json_data, json_extract_path_text(json_data,'root_A1') from json_04; -- check where ever it matches ot returns the value
select id, json_data, json_extract_path_text(json_data,'root_B1') from json_04; 


select check_json(json_data) from json_04; -- all valid entries


------------------------------------------------------------------------------------------------------------------------------------
/*Real Life example*/


-- lets create employee table
create or replace  table employee_json_05 
(
    emp_id number,
    json_data variant
);

insert into employee_json_05
select 1, parse_json(
    ' {"employee": 
        {
            "id": 1,
            "name": "John Doe",
            "designation": "Software Engineer",
            "department": "IT",
            "salary": 60000,
            "joining_date": "01-01-2020",
            "is_manager":false,
            "is_contractor":null,
            "contact_details":[
                {"type":"fax","value":"123"},                   
                {"type":"phone","value":"456"},
                {"type":"mobile","value":"789"}
            ]
        }}
    '
);

insert into employee_json_05
select 2,parse_json(
    ' {"employee": 
        {
            "id": 2,
            "name": "Jane Smith",
            "designation": "Manager",
            "department": "Finance",
            "salary": 80000,
            "joining_date": "01-02-2021",
            "is_manager":false,
            "is_contractor":null,
            "contact_details":[
                {"type":"mobile","value":"123"},                 
                {"type":"fax","value":"456"},
                {"type":"phone","value":"789"}
            ]
        }}
    '
);

insert into employee_json_05
select 3, parse_json(
    ' {"employee": 
        {
            "id": 3,
            "name": "Bob Johnson",
            "designation": "Data Analyst",
            "department": "Data Science",
            "salary": 70000,
            "joining_date": "01-03-2022",
            "is_manager":false,
            "is_contractor":null,
            "contact_details":[
                {"type":"mobile","value":"333"}
            ]
    
        }}
    '
);

insert into employee_json_05
select 4,parse_json(
    ' {"employee": 
        {
            "id": 4,
            "name": "Rob S",
            "designation": "Data Engineer",
            "department": "Data Engineering",
            "salary": 50000,
            "joining_date": "01-03-2020",
            "is_manager":false,
            "is_contractor":true,
            "contact_details":[
                {"type":"fax","value":"444"}
            ]
    
        }}
    '
);

insert into employee_json_05
select 5, parse_json(
    ' {"employee": 
        {
            "id": 5,
            "name": "Kim Joseph",
            "designation": "Data Manager",
            "department": "Data Engineering",
            "salary": 90000,
            "joining_date": "01-03-2019",
            "is_manager":false,
            "is_contractor":true,
            "contact_details":[
                {"type":"phone","value":"555"}
            ]
    
        }}
    '
);


select * from employee_json_05;


select
    emp_id,
    json_data:employee:name::varchar as name,
    json_data:employee:designation::varchar as designation,
    json_data:employee:salary::float as salary,
    to_date(json_data:employee:joining_date::varchar,'dd-mm-yyyy') as doj,
    json_data:employee:is_manager::varchar as manager_status,
    json_data:employee:is_contractor::boolean as contractor_status,
    json_data:employee:contact_details[0]:value::integer as fax,
    json_data:employee:contact_details[1]:value::integer as phone_number_land,
    json_data:employee:contact_details[2]:value::integer as phone_number_mobile
from employee_json_05; -- but this was hard coding 
-- note: the position of type = fax and the nummber, type = phone and number , type = mobile and number are changing
-- in the first entry it was fax-phone_mobile so it got for all other rows,all though they weren't the same,
-- in th esecond row it was mobile-fax-phone
    
    
    


-- Let's access the contact details using indexing: 

select
    emp_id,
    json_data:employee:name::varchar as name,
    json_data:employee:designation::varchar as designation,
    json_data:employee:salary::float as salary,
    to_date(json_data:employee:joining_date::varchar,'dd-mm-yyyy') as doj,
    json_data:employee:is_manager::varchar as manager_status,
    json_data:employee:is_contractor::boolean as contractor_status,
CASE
WHEN
    json_data:employee:contact_details[0]:type::varchar = 'phone'
    THEN
    json_data:employee:contact_details[0]:value::varchar
WHEN
    json_data:employee:contact_details[1]:type::varchar = 'phone'
    THEN
    json_data:employee:contact_details[1]:value::varchar
WHEN
    json_data:employee:contact_details[2]:type::varchar = 'phone'
    THEN
    json_data:employee:contact_details[2]:value::varchar
ELSE 'Not Known'
END AS phone,
CASE
WHEN
    json_data:employee:contact_details[0]:type::varchar = 'mobile'
    THEN
    json_data:employee:contact_details[0]:value::varchar
WHEN
    json_data:employee:contact_details[1]:type::varchar = 'mobile'
    THEN
    json_data:employee:contact_details[1]:value::varchar
WHEN
    json_data:employee:contact_details[2]:type::varchar = 'mobile'
    THEN
    json_data:employee:contact_details[2]:value::varchar
END AS mobile,

CASE
WHEN
    json_data:employee:contact_details[0]:type::varchar = 'fax'
    THEN
    json_data:employee:contact_details[0]:value::varchar
WHEN
    json_data:employee:contact_details[1]:type::varchar = 'fax'
    THEN
    json_data:employee:contact_details[1]:value::varchar
WHEN
    json_data:employee:contact_details[2]:type::varchar = 'fax'
    THEN
    json_data:employee:contact_details[2]:value::varchar
ELSE 'Not Known'
END AS fax

from employee_json_05;


--- you can acheive the the same using 


select json_extract_path_text(json_data,'employee:contact_details[0]:value') from employee_json_05;



select 
    emp_id, 
    json_data:employee.name::varchar as name,
    json_data:employee.designation::varchar as designation,
    json_data:employee.salary as salary,
    to_date(json_data:employee.joining_date::varchar,'DD-MM-YYYY') as doj,
    json_data:employee.is_manager::boolean as is_manager,
    json_data:employee.is_contractor::boolean as is_contractor,
    case 
    when 
        json_extract_path_text(json_data, 'employee.contact_details[0].type')= 'phone' 
    then 
        json_extract_path_text(json_data, 'employee.contact_details[0].value')
    when 
        json_extract_path_text(json_data, 'employee.contact_details[1].type')= 'phone' 
    then 
        json_extract_path_text(json_data, 'employee.contact_details[1].value')   
    when 
        json_extract_path_text(json_data, 'employee.contact_details[2].type')= 'phone' 
    then 
        json_extract_path_text(json_data, 'employee.contact_details[2].value') 
    else 'Not Known'
    end as phone,
    case 
    when 
        json_extract_path_text(json_data, 'employee.contact_details[0].type')= 'mobile' 
    then 
        json_extract_path_text(json_data, 'employee.contact_details[0].value')
    when 
        json_extract_path_text(json_data, 'employee.contact_details[1].type')= 'mobile' 
    then 
        json_extract_path_text(json_data, 'employee.contact_details[1].value')   
    when 
        json_extract_path_text(json_data, 'employee.contact_details[2].type')= 'mobile' 
    then 
        json_extract_path_text(json_data, 'employee.contact_details[2].value') 
    else 'Not Known'
    end as mobile,
    case 
    when 
        json_extract_path_text(json_data, 'employee.contact_details[0].type')= 'fax' 
    then 
        json_extract_path_text(json_data, 'employee.contact_details[0].value')
    when 
        json_extract_path_text(json_data, 'employee.contact_details[1].type')= 'fax' 
    then 
        json_extract_path_text(json_data, 'employee.contact_details[1].value')   
    when 
        json_extract_path_text(json_data, 'employee.contact_details[2].type')= 'fax' 
    then 
        json_extract_path_text(json_data, 'employee.contact_details[2].value') 
    else 'Not Known'
    end as fax  
    from employee_json_05;
