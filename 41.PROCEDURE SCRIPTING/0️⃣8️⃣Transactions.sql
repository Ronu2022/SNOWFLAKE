CREATE OR REPLACE DATABASE TRANSACTIONS;
CREATE OR REPLACE SCHEMA CLASS;
/* TRANSACTIONS:

✅ What is a Transaction? (In Easiest Words)
    A transaction is like a promise:
    “I’ll do multiple things… but if anything goes wrong, I’ll undo everything.”
    Either everything succeeds, or nothing happens.

    -- SNOWFLAKE SUPPORTS AUTO COMMIT => I.E IF YOU EXECUTE ANY STATEMENT, THE CHANGES HAPPEN
    -- IF WE WANT TO COMMIT A LIST OF STATEMENTS AS AS GROUP, WE HAVE TI KEEP THEM UNDER TRANSACTIONS.
    -- A TRANSACTION -- SEQUENCE OF SQL STATEMNT THAT ARE COMMITTED OR ROLLED BACK AS A UNIT.
    -- CN BE STARTED EXPLICITLY BY EXECUTING BEGIN TRANSACTION.
    -- CAN BE ENDED EXPLICITLY BY EXECUTING COMMIT OR ROLLBACK.
    -- IF YOU END IT WITH COMMIT -> ALL STATEMNTS IN THAT TRANSACTION WILL BE COMMITTED.
-- IF YOU ROLLBACK, ALL THE STATEMNETS WILL EB ROLLED BACK.
-- IN CASE OF FAILURE WITHIN THE TRNASACTION ALL STATEMNTS IN THAT TRANSACTION WILL EB ROLLED BACK.
--- IF WE EXECYTE A DDL(CREATE, DROP, ALTER) INSIDE A TRANSACTION, IT WILL B TREATED AS SEPARATE TRANSACTION AND WILL EB COMMITED IMMEDIATELY.



BEGIN;  -- Start a transaction

INSERT INTO orders VALUES (1, 'Item A', 500);
INSERT INTO payments VALUES (1, 500);

COMMIT;  -- If both above succeed, save changes

If something goes wrong (e.g., second insert fails), you can rollback:
ROLLBACK;  -- Undo everything done after BEGIN
🧠 In Summary
Action	                         Meaning
BEGIN	                         Start the transaction block
COMMIT	                         Save everything done since BEGIN
ROLLBACK	                     Undo everything since BEGIN

*/


--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// EXAMPLE: 

CREATE OR REPLACE TABLE TRANSACTIONS.CLASS.bank_accounts (
    account_id INT,
    holder_name STRING,
    balance INT
);

-- Sample data
INSERT INTO TRANSACTIONS.CLASS.bank_accounts VALUES
(1, 'Alice', 1000),
(2, 'Bob', 500);


create or replace procedure balance_update(name_var varchar, bal_var int)
returns varchar
language sql
execute as caller as 
$$
declare
    bal int;
BEGIN -- block of procedure
    BEGIN TRANSACTION; -- block of transaction(scope of transaction starts with begin an dends with commit/rollback)
        UPDATE TRANSACTIONS.CLASS.bank_accounts 
        SET balance = balance - :bal_var WHERE holder_name = :name_var;
    
        select balance into bal from TRANSACTIONS.CLASS.bank_accounts
        where holder_name = :name_var; 
    
        if(:bal <0) then 
            ROLLBACK;
            RETURN 'Balance cant be updated, likely negative balance after update';
        ELSE 
            COMMIT;
            RETURN ('Balance updated');
        END IF;
   

end;
$$;

call balance_update('Alice',400); -- outputr Balance updated

select * from TRANSACTIONS.CLASS.bank_accounts; -- updated. -- current balance - 600


call balance_update('Alice',650); -- Balance cant be updated, likely negative balance after update

select * from TRANSACTIONS.CLASS.bank_accounts;  -- bvalance for alioce is still 600 because it was initially deleted then rolled back 

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE TRANSACTIONS.CLASS.SAMPLE_TRAN(id INT);


select * from TRANSACTIONS.CLASS.SAMPLE_TRAN; -- 0 records.
create or replace procedure proc_transaction_control_demo1()
returns varchar
language sql
execute as caller as 
$$
begin 
    insert into TRANSACTIONS.CLASS.SAMPLE_TRAN values(100);
    insert into TRANSACTIONS.CLASS.SAMPLE_TRAN values(200);
    insert into TRANSACTIONS.CLASS.SAMPLE_TRAN values(300);
    insert into TRANSACTIONS.CLASS.SAMPLE_TRAN values('abc');
    return 'Successfull';
END; 
$$;

call proc_transaction_control_demo1(); -- o/p Uncaught exception of type 'STATEMENT_ERROR' on line 6 at position 4 : DML operation to table TRANSACTIONS.CLASS.SAMPLE_TRAN failed on column ID with error: Numeric value 'abc' is not recognized.

 SELECT * FROM TRANSACTIONS.CLASS.SAMPLE_TRAN; -- look 100, 200,300 are all inserted.  because no transactions are there
 



create or replace procedure proc_transaction_control_demo1()
returns varchar
language sql
execute as caller as 
$$
    begin
        begin transaction;
            insert into TRANSACTIONS.CLASS.SAMPLE_TRAN values(100);
            insert into TRANSACTIONS.CLASS.SAMPLE_TRAN values(200);
            insert into TRANSACTIONS.CLASS.SAMPLE_TRAN values(300);
            insert into TRANSACTIONS.CLASS.SAMPLE_TRAN values('abc');
        commit;
    return 'Successfull';
    end;
$$;

select * from  TRANSACTIONS.CLASS.SAMPLE_TRAN; -- 100,200,300 records.
-- first truncate

TRUNCATE  table TRANSACTIONS.CLASS.SAMPLE_TRAN; 
SELECT * FROM TRANSACTIONS.CLASS.SAMPLE_TRAN; -- 0 records m now call teh proicedure

Call  proc_transaction_control_demo1();


// CREATE THE SAME PROCEDURE WITH EXCEPTION:

create or replace procedure proc_transaction_control_demo1()
returns varchar
language sql
execute as caller as
BEGIN
    begin TRANSACTION;
            insert into TRANSACTIONS.CLASS.SAMPLE_TRAN values(100);
            insert into TRANSACTIONS.CLASS.SAMPLE_TRAN values(200);
            insert into TRANSACTIONS.CLASS.SAMPLE_TRAN values(300);
            insert into TRANSACTIONS.CLASS.SAMPLE_TRAN values('abc');
    COMMIT; 
RETURN 'SUCCESS.';
EXCEPTION 
    when statement_error then
    rollback;
    return object_construct('Comment', 'Failue',
                            'Error Type','Statement_error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
    when expression_error then
    rollback;
    return object_construct('Comment', 'Failure',
                            'ERROR TYPE', 'Expression Error',
                            'SQLCODE',sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE',sqlstate);
END; 


CALL proc_transaction_control_demo1();

