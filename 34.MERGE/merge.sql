USE DATABASE PRACTISE;

-- This is the target table: where records are to be inserted.
CREATE OR REPLACE TABLE friends 
(
    id INT,
    name VARCHAR,
    phone VARCHAR
);

INSERT INTO friends (id, name, phone)
VALUES 
    (1, 'Alice', '1234567890'),
    (2, 'Bob', '2345678901');


-- SOurce Table: 

CREATE OR REPLACE TABLE new_friends (
    id INT,
    name VARCHAR,
    phone VARCHAR
);


INSERT INTO new_friends (id, name, phone)
VALUES 
    (1, 'Alice', '9999999999'),  -- update needed
    (3, 'Charlie', '3456789012');  -- new entry



-- Now observe for Alice there is a change in phone number thus the target tablke friends would require an update. 
-- so we need to check if it is present it is to be updated
-- if not then inserted.
-- something like:
/*
MERGE INTO target_table AS target
USING source_table AS source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE ...
WHEN NOT MATCHED THEN
    INSERT ...
*/

MERGE INTO friends AS target
USING new_friends AS source 
ON target.id = source.id
WHEN MATCHED THEN 
    UPDATE 
        SET 
        target.name = source.name,
        target.phone = source.phone
WHEN NOT MATCHED THEN
    INSERT(id, name, phone) 
    VALUES(source.id, source.name, source.phone);
