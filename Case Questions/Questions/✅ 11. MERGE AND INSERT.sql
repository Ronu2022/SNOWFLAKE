CREATE OR REPLACE TABLE employees
(
    id INT,
    name STRING
);


-- Source table
CREATE OR REPLACE TABLE new_employees
(
    id INT,
    name STRING
);

 -- Insert into target table
INSERT INTO employees (id, name) VALUES
    (1, 'Alice'),
    (2, 'Bob');

-- Insert into source table
INSERT INTO new_employees (id, name) VALUES
    (2, 'Robert'),  -- existing ID (will update)
    (3, 'Charlie'); -- new ID (will insert)


/* Update the employees table with new records, and when the id is seen with a different name, replace the name in employees table with the new name*/

/* Query Way*/
SELECT 
    COALESCE(employees.id,new_employees.id) as id,
    COALESCE(new_employees.name,employees.name) as name
from employees
full outer join new_employees on employees.id = new_employees.id
ORDER BY COALESCE(employees.id,new_employees.id)  ASC


/* But what if I wish to update the employees table*/


MERGE INTO employees as oe
USING new_employees as ne ON oe.id = ne.id
WHEN MATCHED THEN UPDATE SET oe.name = ne.name
WHEN NOT MATCHED THEN INSERT (id,name) VALUES
(ne.id,ne.name)

select * from employees ORDER BY ID ASC;
