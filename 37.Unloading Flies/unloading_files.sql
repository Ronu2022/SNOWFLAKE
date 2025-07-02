--==================================================
-- Unloading data to external cloud storage locations
--===================================================

CREATE OR REPLACE DATABASE MY_DB; -- databse
CREATE OR REPLACE SCHEMA EXT_STAGES; -- for external staging
CREATE OR REPLACE SCHEMA FILE_FORMATS; -- file_format check 


---------------------------------------------------------------------------------------------------------------------
// Add new aws s3 location to our storage int object to store output files
---------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE STORAGE INTEGRATION s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::481665128591:role/unloading_role'
STORAGE_ALLOWED_LOCATIONS = ('s3://unlodingbucket/snowflake_unloading/')
COMMENT = 'Storage iNtegration';


DESC STORAGE INTEGRATION s3_int; 
--STORAGE_AWS_IAM_USER_ARN
-- arn:aws:iam::663590917978:user/hl231000-s
---------------------------------------------------------------------------------------------------------------------
// Create file format object
---------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FILE FORMAT MY_DB.FILE_FORMATS.CSV_FILEFORMAT
TYPE = 'CSV'
FIELD_DELIMITER = '|'
SKIP_HEADER = 1
EMPTY_FIELD_AS_NULL = TRUE;

// Create stage object with integration object & file format object
// Using the Storeage Integration object that was already created


CREATE OR REPLACE STAGE MY_DB.EXT_STAGES.MYS3_OUTPUT
URL = 's3://unlodingbucket/snowflake_unloading/'
STORAGE_INTEGRATION = S3_INT
FILE_FORMAT = CSV_FILEFORMAT;

// Generate files and store them in the stage location:

COPY INTO @MY_DB.EXT_STAGES.MYS3_OUTPUT
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

/*rows_unloaded	    input_bytes	    output_bytes
150000	            24196144	    9036143 */


//Listing files under my s3 bucket
LIST @MY_DB.EXT_STAGES.MYS3_OUTPUT;
/*
name	                                                      size	            md5	                            last_modified
s3://unlodingbucket/snowflake_unloading/data_0_1_0.csv.gz	2710445	7e83cc1a914b97f33f65ab2bd0cbad23	Wed, 2 Jul 2025 10:18:50 GMT
s3://unlodingbucket/snowflake_unloading/data_0_2_0.csv.gz	1805774	d2ba6ff110c87cc18b682f78f33ecd8d	Wed, 2 Jul 2025 10:18:50 GMT
s3://unlodingbucket/snowflake_unloading/data_0_3_0.csv.gz	4519924	94c90106968a23f8b20643e3b9622670	Wed, 2 Jul 2025 10:18:50 GMT*/

-- if you observe the o/p there looks to be 3 separate files, which actually it is not, by default parallelism works taht is snowflake braks one file to chuncks and each chunks 
   -- unloaded together => different compute threads work for the same 
   -- You can change that with SINGLE = TRUE. => would take more time for bigger  files.
-- files -> successfully unloaded in the folders but in separate chuncks
-- the files are in compressed format => directlky viewing them is not possible.

// OVERWRITE
COPY INTO @MY_DB.EXT_STAGES.MYS3_OUTPUT
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
OVERWRITE = TRUE -- existing files got overwritten
SINGLE =TRUE;

//SINGLE FILE:

COPY INTO @MY_DB.EXT_STAGES.MYS3_OUTPUT/upoloaded_cx -- upoloaded_cx is the name of the file you want that to be uploaded.
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
OVERWRITE = TRUE
SINGLE = TRUE; --> gets unloaded in one go -> no parallelism happens here.

LIST @MY_DB.EXT_STAGES.MYS3_OUTPUT;
/*name	`                                                size	       md5	                                    last_modified
s3://unlodingbucket/snowflake_unloading/upoloaded_cx	9039340	7cad6710b450e48636fd4ea05c20f9fc	Wed, 2 Jul 2025 11:01:47 GMT*/
-- look here -> since we did OVERWRITE = TRUE, the xisting file in the stage got replaced or over written 



// MAX_FILE_SIZE:

COPY INTO @MY_DB.EXT_STAGES.MYS3_OUTPUT/upoloaded_cx_2
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
OVERWRITE = TRUE
SINGLE = TRUE
MAX_FILE_SIZE = 1000000; -- wont work ? why because you have given single = True , => max size or chunck size dowesant make sense


COPY INTO @MY_DB.EXT_STAGES.MYS3_OUTPUT/upoloaded_cx_2
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
OVERWRITE = TRUE
SINGLE = FALSE
MAX_FILE_SIZE = 1000000;

LIST @MY_DB.EXT_STAGES.MYS3_OUTPUT;

/*
name	                                                               size	           md5	                            last_modified
s3://unlodingbucket/snowflake_unloading/upoloaded_cx	            9039340	7cad6710b450e48636fd4ea05c20f9fc	Wed, 2 Jul 2025 11:01:47 GMT
s3://unlodingbucket/snowflake_unloading/upoloaded_cx_2_0_0_0.csv.gz	1084692	1fd2dba8a6939d0ff4f85a581fe4053b	Wed, 2 Jul 2025 11:08:23 GMT
s3://unlodingbucket/snowflake_unloading/upoloaded_cx_2_0_0_1.csv.gz	1086446	33fd489cbb1d319c3386e8e4a05f834d	Wed, 2 Jul 2025 11:08:23 GMT
s3://unlodingbucket/snowflake_unloading/upoloaded_cx_2_0_0_2.csv.gz	541300	ffd654b01d70af29a4b4f47a9a24b69e	Wed, 2 Jul 2025 11:08:23 GMT
s3://unlodingbucket/snowflake_unloading/upoloaded_cx_2_0_2_0.csv.gz	1087207	d18b23bff16c0aa644f99e0f191762f0	Wed, 2 Jul 2025 11:08:23 GMT
s3://unlodingbucket/snowflake_unloading/upoloaded_cx_2_0_2_1.csv.gz	1085429	9bb4f77f4b57bab64fc04e3f8842c5bd	Wed, 2 Jul 2025 11:08:23 GMT
s3://unlodingbucket/snowflake_unloading/upoloaded_cx_2_0_2_2.csv.gz	1088694	6ce08fff76dfbab5d9c04843ff980afa	Wed, 2 Jul 2025 11:08:23 GMT
s3://unlodingbucket/snowflake_unloading/upoloaded_cx_2_0_2_3.csv.gz	1086925	5c0182b6d9cb657207c93d88f6f50eb0	Wed, 2 Jul 2025 11:08:23 GMT
s3://unlodingbucket/snowflake_unloading/upoloaded_cx_2_0_2_4.csv.gz	175600	f971ff7ac5994309cb86d98cc13956c6	Wed, 2 Jul 2025 11:08:23 GMT
s3://unlodingbucket/snowflake_unloading/upoloaded_cx_2_0_3_0.csv.gz	1085663	3057806a25cb03b06a7bcab419661c69	Wed, 2 Jul 2025 11:08:23 GMT
s3://unlodingbucket/snowflake_unloading/upoloaded_cx_2_0_3_1.csv.gz	721047	7a93d81410fd75b6072e3af53ff31ec6	Wed, 2 Jul 2025 11:08:23 GMT
*/


copy into @MY_DB.EXT_STAGES.MYS3_OUTPUT/detailed_op
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
overwrite = True
single = TRUE
detailed_output = True;


COPY INTO @MY_DB.EXT_STAGES.MYS3_OUTPUT/detailed_op_quiery_id
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
OVERWRITE = TRUE
SINGLE = TRUE
DETAILED_OUTPUT = TRUE
INCLUDE_QUERY_ID = TRUE; -- WOuild throw error  because 


-- 
COPY INTO @MY_DB.EXT_STAGES.MYS3_OUTPUT/detailed_op_quiery_id
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
OVERWRITE = FALSE
SINGLE = TRUE
DETAILED_OUTPUT = TRUE
INCLUDE_QUERY_ID = TRUE; -- invalid property combination 'INCLUDE_QUERY_ID'='true' and 'SINGLE'='true'


COPY INTO @MY_DB.EXT_STAGES.MYS3_OUTPUT/detailed_op_quiery_id
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
OVERWRITE = FALSE
SINGLE = FALSE
DETAILED_OUTPUT = TRUE
INCLUDE_QUERY_ID = TRUE; -- Worked

LIST @MY_DB.EXT_STAGES.MYS3_OUTPUT;



