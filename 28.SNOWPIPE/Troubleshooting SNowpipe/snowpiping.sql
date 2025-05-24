// 3 JSON Files are to be used.
// There in the other section.

// DB and Schema Creation

CREATE OR REPLACE DATABASE spring_loans_db;
CREATE OR REPLACE SCHEMA loan_ingest;

// Table Creation 

CREATE OR REPLACE TABLE spring_loans_db.loan_ingest.raw_loan_applications
(
    raw_data VARIANT,
    ingestion_time  TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);



// CREATING FILE FORMAT

create or replace file format json_format_loans
type = 'json'
strip_outer_array = True
ignore_utf8_errors = True; 




// Creation of Folders in S3.

-- spring-loans-data-pipeline/loan_applications

// Creation of Role: 
-- loanpipelinerole
-- Copy the ARN : arn:aws:iam::481665128591:role/loanpipelinerole


// Creation of Storage Integration

Create or Replace storage integration s3_int
type = external_stage
storage_provider = s3
enabled = True
storage_aws_role_arn = 'arn:aws:iam::481665128591:role/loanpipelinerole'
storage_allowed_locations = ('s3://spring-loans-data-pipeline/loan_applications/')
comment = 'Integration Object'; 


// Describing Storage Integration: 

DESC STORAGE INTEGRATION s3_int;
-- copy : STORAGE_AWS_IAM_USER_ARN - arn:aws:iam::077380691495:user/47wz0000-s 

// Go to Your role created -> Trust Relationship -> Edit Policy
/*
    it was before: 
    {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::481665128591:root"
            },
            "Action": "sts:AssumeRole",
            "Condition": {}
        }
    ]
}

now paste the storage_aws_iam_user_arn in the value of "AWS", it finally becomes:

{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Principal": {
				"AWS": "arn:aws:iam::077380691495:user/47wz0000-s"
			},
			"Action": "sts:AssumeRole",
			"Condition": {}
		}
	]
}  

update policy
*/ 


// STAGE CREATION:


create or replace stage loan_applications_stage
url = 's3://spring-loans-data-pipeline/loan_applications/'
storage_integration = s3_int
file_format = json_format_loans
comment = 'Stage for storage integration';


LIST @loan_applications_stage; -- No files yet


// Creation of Table ( already done above):


// Creation of Pipe

create or replace pipe loan_applications_pipe
auto_ingest = True as
copy into spring_loans_db.loan_ingest.raw_loan_applications(raw_data)
from @loan_applications_stage
pattern = '.*loan_applications.*';



// Desc pipe: 

DESC PIPE loan_applications_pipe; 
-- copy notification channel: 
-- arn:aws:sqs:ap-south-1:077380691495:sf-snowpipe-AIDAREBB7UYT7MVPOGYP7-Z5W3ICfSzjFJu_9rGTKkXA;

// Connecting Notification:
/*went to folder -> Properties -> Edit Notification 
Event name
loan_application_event

Prefix - optional
loan_applications

Object creation:
Selected 
Put
s3:ObjectCreated:Put
Post
s3:ObjectCreated:Post
Copy
Destination: 
SQS queue

Send notifications to an SQS queue to be read by a server.

Specify SQS queue:

Enter SQS queue ARN
arn:aws:sqs:ap-south-1:077380691495:sf-snowpipe-AIDAREBB7UYT7MVPOGYP7-Z5W3ICfSzjFJu_9rGTKkXA*/



// Uploading the file.

-- uploaded one file 

list @loan_applications_stage;


// Checking the details of the file: 
with cte as
(
select array_agg(object_construct(*)) as array_column from
(
select * from table
(
    infer_schema(
    location => '@loan_applications_stage/loan_applications_valid.json',
    file_format => 'json_format_loans'
    
    )
        
    )
)
) select t.value:"COLUMN_NAME"::STRING as column_names
from cte, 
lateral flatten(input => array_column) t


// Checking from the ingested table

select * from spring_loans_db.loan_ingest.raw_loan_applications;

Select 
    ingestion_time,
    raw_data:application_id::STRING as application_ids,
    raw_data:application_meta.channel::STRING as application_meta_channel,
    raw_data:application_meta.date::DATETIME as application_meta_time,
    raw_data:applocation_meta.submitted_by::VARCHAR as application_meta_submitted_by,
    raw_data:customer_info.credit_score::INT as cx_credit_score,
    raw_data:customer_info.dob::DATE as cx_dob,
    raw_data:customer_info.name::VARCHAR as cx_name,
    raw_data:loan_details.amount::int as loan_amount,
    raw_data:loan_details.interest_rate::float as interest_rate,
    raw_data:loan_details.term_months::int as loan_tenure,
    raw_data:product::VARCHAR as product_name
from spring_loans_db.loan_ingest.raw_loan_applications ORDER BY ingestion_time DESC; 





// uploaded the second file : loan_applications_invalid.json


LIST  @loan_applications_stage;


// Lets scheck for ingestion:

Select system$pipe_status('loan_applications_pipe');


/*
{"executionState":"RUNNING",
"pendingFileCount":0,
"lastIngestedTimestamp":"2025-05-24T09:47:05.916Z",
"lastIngestedFilePath":"loan_applications_invalid.json"
"notificationChannelName":"arn:aws:sqs:ap-south-1:077380691495:sf-snowpipe-AIDAREBB7UYT7MVPOGYP7-Z5W3ICfSzjFJu_9rGTKkXA",
"numOutstandingMessagesOnChannel":1,
"lastReceivedMessageTimestamp":"2025-05-24T09:47:05.834Z",
"lastForwardedMessageTimestamp":"2025-05-24T09:47:07.57Z",
"lastPulledFromChannelTimestamp":"2025-05-24T09:49:13.689Z",
"lastForwardedFilePath":"spring-loans-data-pipeline/loan_applications/loan_applications_invalid.json",
"pendingHistoryRefreshJobsCount":0}


-- if you see the content: "lastReceivedMessageTimestamp":"2025-05-24T09:47:05.834Z",
"lastForwardedMessageTimestamp":"2025-05-24T09:47:07.57Z", remains mostly similar, so data is loaded
there seem to be no issues with respect to the process as such
*/



// Checking fFOR COPY HISTORY

SELECT *
FROM TABLE(
  INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW_LOAN_APPLICATIONS',
    START_TIME => DATEADD(HOUR, -4, CURRENT_TIMESTAMP())
  )
)
ORDER BY LAST_LOAD_TIME DESC; -- tHIS ALSO SHOWS THE FILE HAS BEEN INGESTED AND SNOWPIPE HAS COPIED THE CONTENT. 

-- SO WHATS THE ERROR



SELECT 
  raw_data,
  raw_data:application_id::STRING AS application_id,
  raw_data:product::STRING AS product,
  raw_data:loan_details.amount::NUMBER AS amount
FROM spring_loans_db.loan_ingest.raw_loan_applications
ORDER BY ingestion_time DESC
LIMIT 5; -- THERE IS A DATATYPE MISMATCH


select * from spring_loans_db.loan_ingest.raw_loan_applications;
    -- LOOK AT THE AMOUNT SECTION, EVERYWHERE IT IS  IN NUMBER BUT HERE IT IS IN VARCHAR. SO THE ERROR.
    -- SOLUTION: EITHER REMOVE IT LIKE UPDATE TABEL_NAME SET AMOUNT = 12,000 WHERE NAME = 'ABC';
    -- ELSE, DROP THE TABLE, ASK THE TEAM TO UPDATE THE CONTENTS
    -- AND RE DO THE PROCESS.
