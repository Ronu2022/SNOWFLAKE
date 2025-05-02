USE DATABASE mydb;

-- Create a Schema for External Stages

CREATE OR REPLACE SCHEMA MYDB.external_stages;


-- Creating the table

CREATE OR REPLACE TABLE MYDB.PUBLIC.LOAN_PAYMENT
(
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

 SELECT * FROM PUBLIC.LOAN_PAYMENT;

 //Loading the data from S3 bucket

COPY INTO MYDB.PUBLIC.LOAN_PAYMENT
FROM  s3://bucketsnowflakes3/Loan_payments_data.csv
file_format = (type = 'csv', field_delimiter = ',', skip_header = 1);


//Validate the data
SELECT * FROM PUBLIC.LOAN_PAYMENT;



//Check the count

SELECT COUNT(*) FROM PUBLIC.LOAN_PAYMENT; -- 500








