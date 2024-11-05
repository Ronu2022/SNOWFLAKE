CREATE OR REPLACE SEQUENCE my_sequence
    START = 1
    INCREMENT = 1;


CREATE OR REPLACE TABLE emp_details
(
    id INTEGER DEFAULT my_sequence.nextval,
    name VARCHAR,
    manager_id INTEGER,
    salary INTEGER,
    designation VARCHAR
);

INSERT INTO emp_details(name,manager_id,salary,designation) VALUES
('Sripadh',NULL,10000,'CEO'),
('Satya',5,1400,'Software Engineer'),
('Jia',5,500,'Data Analyst'),
('David',5,1800,'Data Scientist'),
('Michael',7,3000,'Manager'),
('Arvind',7,2400,'Architect'),
('Asha',1,4200,'CTO'),
('Maryam',1,3500,'Manager'),
('Reshma',8,2000,'Business Analyst'),
('Akshay',8,2500,'Java Developer');

SELECT * FROM emp_details;


-- Find the Hierachy of Employees Under a given Manager 'Asha':



WITH RECURSIVE emp_hierachy AS
(
    SELECT id,name,manager_id,designation, 1 AS lvl FROM emp_details WHERE name = 'Asha'
      UNION ALL
    SELECT e.id,e.name,e.manager_id,e.designation,h.lvl + 1 AS lvl FROM emp_hierachy AS h
    JOIN emp_details e
    ON h.id = e.manager_id
)SELECT * FROM emp_hierachy;




-- Show the hierarchical relationship:


WITH RECURSIVE emp_hierarchy AS
(
    select id, name,manager_id,designation, name as hierachy from emp_details where name = 'Asha'
    union all
    select e.id,e.name,e.manager_id,e.designation,
           concat(h.hierachy,' -> ',e.name) as hierachy
    from emp_hierarchy as h
    join emp_details  as e
    on h.id = e.manager_id
) select * from emp_hierarchy


-- let's say I need manager's name as well

WITH RECURSIVE emp_hierarchy AS
(
    select id, name,manager_id,designation, name as hierachy from emp_details where name = 'Asha'
    union all
    select e.id,e.name,e.manager_id,e.designation,
           concat(h.hierachy,' -> ',e.name) as hierachy
    from emp_hierarchy as h
    join emp_details  as e
    on h.id = e.manager_id
) select eh.id, eh.name, eh.manager_id,e1.name AS manager_name,eh.hierachy
  FROM emp_hierarchy AS eh
  JOIN emp_details AS e1
  ON eh.manager_id = e1.id;

  
-- Look for the hierachy for the employee = 'David':

with recursive empl_hierachy as
(
    select id,name,manager_id,designation,0 as lvl from emp_details where name = 'David'
    union all
    select e.id,e.name,nvl(e.manager_id,0)as manager_id,e.designation,h.lvl+1 AS lvl 
    from empl_hierachy  as h
    join emp_details as e on h.manager_id = e.id
) select * from empl_hierachy
