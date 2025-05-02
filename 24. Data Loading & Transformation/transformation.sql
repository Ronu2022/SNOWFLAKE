USE DATABASE mydb;

// Publicly accessible staging area  

CREATE OR REPLACE STAGE MYDB.external_stages.aws_ext_stage
  url='s3://bucketsnowflakes3';


  
// listing the files in external stage

LIST @MYDB.external_stages.aws_ext_stage;


//Case 1: Just Viewing Data from ext stage

select $1, $2, $3, $4, $5, $6 from @MYDB.external_stages.aws_ext_stage/OrderDetails.csv; -- seE here we are not having the skip_gheader hence the issue.


// Creating the file+format

  CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  SKIP_HEADER = 1;

//Giving Alias Names to fields

SELECT 
    t.$1 AS OID,
    t.$2 AS AMT,
    t.$3 AS PFT,
    t.$4 AS QNT,
    t.$5 AS CAT,
    t.$6 AS sub_cat
FROM @MYDB.external_stages.aws_ext_stage/OrderDetails.csv  (FILE_FORMAT => my_csv_format) t;



// Transforming Data while loading


-- Load Data with only relevant fields:

CREATE OR REPLACE TABLE MYDB.PUBLIC.ORDERS_EX 
(
    ORDER_ID VARCHAR(30),
    AMOUNT INT
);

-- COPY:

COPY INTO  MYDB.PUBLIC.ORDERS_EX FROM 
(
    SELECT t.$1, t.$2 FROM @MYDB.external_stages.aws_ext_stage/OrderDetails.csv  (FILE_FORMAT => my_csv_format) t
);

Select * from MYDB.PUBLIC.ORDERS_EX;


SELECT 
t.$1 as order_id, t.$2 as amt, t.$3 as profit, t.$4 as qnty, t.$5 as cat, t.$6  as sub_cat
FROM @MYDB.external_stages.aws_ext_stage/OrderDetails.csv(file_format => my_csv_format) t limit 10;


// Case3: applying basic transformation by using functions

CREATE OR REPLACE TABLE MYDB.PUBLIC.ORDERS_EX_transformed 
(
    ORDER_ID VARCHAR(30),
    PROFIT INT,
	AMOUNT INT,    
    CAT_SUBSTR VARCHAR(5),
    CAT_CONCAT VARCHAR(60),
	PFT_OR_LOSS VARCHAR(10)
  );

  // COPY comand using functions:

  COPY INTO MYDB.PUBLIC.ORDERS_EX_transformed FROM 
  (
    SELECT t.$1, t.$3, t.$2, SUBSTR(t.$5, 1,5),
           CONCAT(t.$5,'-',t.$6),
           CASE WHEN t.$3 > 0 THEN 'PROFIT'
                WHEN t.$3 < 0 THEN 'LOSS'
                WHEN t.$3 = 0 THEN 'BE'
            END 
    FROM @MYDB.external_stages.aws_ext_stage/OrderDetails.csv (file_format => my_csv_format) t


  );


  // Case 4: Loading sequence numbers in columns

  -- Sequence CReation
  CREATE OR REPLACE SEQUENCE auto_increment
  START = 1;

-- Table Creation:
CREATE OR REPLACE TABLE MYDB.PUBLIC.LOAN_PAYMENT
(
    seq_id NUMBER DEFAULT auto_increment.nextval,
    Loan_ID STRING,
    loan_status STRING,
    Principal STRING,
    terms STRING,
    effective_date STRING,
    due_date STRING,
    paid_off_time STRING,
    past_due_days STRING,
    age STRING,
    education STRING,
    Gender STRING
    
);
  

-- copying data
COPY INTO MYDB.PUBLIC.LOAN_PAYMENT(Loan_ID,loan_status,Principal,terms,effective_date,due_date,paid_off_time,past_due_days,age,education,Gender)
FROM
( SELECT t.$1, t.$2,t.$3, t.$4, t.$5,t.$6,
CAST((TIME(TO_TIMESTAMP(t.$7,'MM/DD/YYYY HH24:MI'))) AS STRING),t.$8, t.$9 , t.$10 , t.$11 FROM @MYDB.external_stages.aws_ext_stage/Loan_payments_data.csv (file_format => my_csv_format) t);

-- Check
SELECT * FROM MYDB.PUBLIC.LOAN_PAYMENT;





