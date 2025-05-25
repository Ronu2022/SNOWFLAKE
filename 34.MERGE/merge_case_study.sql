
CREATE OR REPLACE TABLE friends (
    id INT,
    name VARCHAR,
    phone VARCHAR
);



CREATE OR REPLACE TABLE new_friends (
    id INT,
    name VARCHAR,
    phone VARCHAR
);



INSERT INTO friends (id, name, phone)
VALUES 
    (1, 'Alice', '1234567890'),
    (2, 'Bob', '2345678901');


--  Data for new_friends (staging with duplicates)

INSERT INTO new_friends (id, name, phone)
VALUES 
    (1, 'Alice B', '8888888888'),  -- Same ID, updated name + phone
    (1, 'Alice C', '9999999999'),  -- Duplicate ID, another version
    (3, 'Charlie', '3456789012');  -- New person



/*

You are a data engineer at a company. Your job is to:

✅ Clean and update the friends table using data from new_friends
Your MERGE must:
Update the row if the id already exists (like Alice, ID = 1)

Insert the row if the id doesn’t exist (like Charlie, ID = 3)

✅ But — from the new_friends table, only keep one row per id

You can decide the logic: pick the one with the highest phone number (for now)
*/





-- code 

merge into friends as target
using 
(
    select id,name,phone, row_number() over (partition by id order by phone desc) as rn
    from new_friends qualify rn = 1
) as source on target.id = source.id
when matched then 
update
    set target.name = source.name,
        target.phone = source.phone
when not matched then
    insert(id,name,phone)
    values(source.id,source.name,source.phone)


-- now lets try making it within a procedure: 
-- first delete the records and re run the intial create statement.
-- so that it comes to square one again

CREATE OR REPLACE TABLE friends (
    id INT,
    name VARCHAR,
    phone VARCHAR
);



CREATE OR REPLACE TABLE new_friends (
    id INT,
    name VARCHAR,
    phone VARCHAR
);



INSERT INTO friends (id, name, phone)
VALUES 
    (1, 'Alice', '1234567890'),
    (2, 'Bob', '2345678901');


--  Data for new_friends (staging with duplicates)

INSERT INTO new_friends (id, name, phone)
VALUES 
    (1, 'Alice B', '8888888888'),  -- Same ID, updated name + phone
    (1, 'Alice C', '9999999999'),  -- Duplicate ID, another version
    (3, 'Charlie', '3456789012');  -- New person


// PROCEDURE:

Create or replace procedure proc_merge()
Returns string
language sql as
$$
    BEGIN
    merge into friends as target
    using
    (
        select id,name,phone, row_number() over (partition by id order by phone desc) as rn
        from new_friends qualify rn = 1 
    ) as source on  target.id = source.id
    when matched then
    update
        set  
            target.name = source.name,
            target.phone = source.phone
            
    when not matched then
        insert(id,name,phone) 
        values(source.id,source.name, source.phone);
    
    RETURN 'Insert and Merge Done';
    select * from 

END;
$$; 

// TASK:

Create or replace task task_name
warehouse = COMPUTE_WH
SCHEDULE = 'USING CRON * * * * * UTC'
AS CALL proc_merge();
\
// RESUMING TASK:
ALTER TASK task_name RESUME;
ALTER TASK task_name SUSPEND;

select * from friends;
