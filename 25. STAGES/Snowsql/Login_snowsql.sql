Step 1: Install SnowSQL (Windows/ Mac)
https://www.snowflake.com/en/developers/downloads/snowsql/

Step 2: Open Command Prompt
/* Check the current version of snowsql Installed.*/
-v  

Step 3: Go to Snowflake -> Admin -> Accounts -> 3 dots -> Manage URL -> Copy the Current URL
for me it was 
https://pmoqnth-pj70448.snowflakecomputing.com

what we need from it is :
pmoqnth-pj70448

Step 4: Get The Username from Snowflake
USername = the name you give to log in

for me, it was: AJAYMOHANTY2024


Step 5: Command Propmt Log in 

snowsql -a pmoqnth-pj70448 -u AJAYMOHANTY2024

Step 6: Give the Password
password wont be reflecting while you type, just type and press enter

Step 7: Give your Data Base, Schema

USE DATABASE MYDB;
USE SCHEMA PUBLIC;

Step 8: You are inside the schema.
