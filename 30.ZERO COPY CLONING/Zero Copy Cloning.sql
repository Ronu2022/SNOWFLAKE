CREATE OR REPLACE DATABASE zero_copy_cloning;
CREATE OR REPLACE SCHEMA cloned_objects;

// CLONING A TABLE:

CREATE OR REPLACE TABLE cloned_purchases_history
CLONE PRACTISE.PUBLIC.USER_PURCHASES; -- Done

SELECT * FROM zero_copy_cloning.cloned_objects.cloned_purchases_history;


// Cloning Schema:

CREATE OR REPLACE SCHEMA cloned_schema 
CLONE PRACTISE.PUBLIC; -- Cloned


// CLONING DATABASE:

CREATE OR REPLACE DATABASE mydb_cloned 
CLONE MYDB;



// UPDATE in the Source_oject:
SELECT * FROM  PRACTISE.PUBLIC.USER_PURCHASES;

ALTER TABLE  PRACTISE.PUBLIC.USER_PURCHASES
ADD COLUMN CURRENCY  VARCHAR;

UPDATE PRACTISE.PUBLIC.USER_PURCHASES
SET currency = 'INR';

SELECT * FROM PRACTISE.PUBLIC.USER_PURCHASES; -- updated
-- let's check for the cloned table.
SELECT * FROM  zero_copy_cloning.cloned_objects.cloned_purchases_history -- Not updated. 
-- hence any changes in the source after getting cloned wont refelct in the cloned object and vice versa


// Time travel Concept with Cloning 

CREATE OR REPLACE TABLE zero_copy_cloning.cloned_objects.dummy_table
(
    a int,
    b varchar
); -- 01bc290a-3201-9884-000d-1a9a0004a586 (query Id)

Select * from zero_copy_cloning.cloned_objects.dummy_table;

-- Let's delete the above Table 

DROP TABLE zero_copy_cloning.cloned_objects.dummy_table; -- 01bc2903-3201-9884-000d-1a9a0004a56e (query Id)

-- Let's clone the dropped table


CREATE OR REPLACE TABLE zero_copy_cloning.cloned_objects.cloned_dummy_table
CLONE zero_copy_cloning.cloned_objects.dummy_table;


AT (STATEMENT => '01bc290a-3201-9884-000d-1a9a0004a586');



CREATE OR REPLACE TABLE zero_copy_cloning.cloned_objects.cloned_dummy_table
CLONE dummy_table at (statement => '01bc290a-3201-9884-000d-1a9a0004a586');



