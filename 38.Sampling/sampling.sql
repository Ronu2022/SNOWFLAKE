// WHAT IS DATA SAMPLING

-- selecting a part of data or subset of records from a table
    -- for Query Building or testing
    -- data analysis or understanding

-- useful in Dev environment where we use small warehouyses and occupy less storage.
--- we can sample a fraction or % of rows
-- we can sample fixed number of rows.

// Sampling Methods Supported By SNwoflake: 

-- Berrnouilli or Row --- where the probability of including a row is p/100
                        -- we can say this gives almost p% of data
                        -- good for smaller tables.
-- system or block : Where the probability of including a block is p/100
                    -- we can say this gives data from p% of blocks
                    -- good for larger tables


-- for eg: if a table contains 4 million records stored in 600 micro partitions and i need 10% of data for my testing.
-- Bernoulli or Row - - will fetch data from 10% of 600 = 60 micro partitions.


-----------------------------------------------------------------------------------------------------------------------------------

select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER; 
select count(*) from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;-- 150000


SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER SAMPLE(10); -- 14.9k rows -- 10 % of total rowes randomly sleected
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER SAMPLE(1); -- 1.5 k => 1 % of total rows randoimly selected

-------------------------------------------------------------------------------------------------------------------------------

/*What is BERNOULLI sampling?
BERNOULLI sampling means:

Snowflake looks at each row individually, and randomly decides whether to include it in the sample.

Think of it like flipping a coin for each row to decide: "Should I keep this row in the sample?"*/

SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER SAMPLE BERNOULLI (10);  -- goes row by row
-- snwoflake goes through row by ro to decide weather to include thatrow into the sample or not.

--This will randomly include about 10% of the rows from the table.

-- 💡 It's row-based, so Snowflake must look at every row to decide.

-- Granularity	Row-by-row decision
-- Performance	Slower on large tables (since it touches all rows)
-- Use case	When true randomness across individual rows is needed

-------------------------------------------------------------------------------------------------------------------------------------
/*
What is SYSTEM sampling in Snowflake?
    -- SYSTEM sampling is block-based sampling, not row-based.

    --Instead of checking every row, Snowflake:

    -- Randomly selects entire blocks of data

    -- A block is a chunk of storage — how Snowflake stores table data internally

    -- So this method is much faster than Bernoulli for large tables
*/

SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER SAMPLE SYSTEM(10); -- 14.9k 
-- This returns approximately 10% of rows by randomly selecting about 10% of blocks.

-- Granularity	Block-level (not row-by-row)
-- Performance	Faster than BERNOULLI, especially on large tables


--------------------------------------------------------------------------------------------------------------------------------------
/*

Using SEED with SAMPLE
When you add a SEED value to a sampling query, you tell Snowflake:

“Use this specific random‑number seed, so I can get the same sample each time I run the query (as long as the table’s physical layout hasn’t changed).”

Let’s take a practical scenario:

SELECT * FROM orders SAMPLE SYSTEM(10) SEED(111);
If you:

Run this now

Then run the exact same query again 5 minutes later

On the same table, with no changes in data or clustering

👉 You will get the same set of rows each time.

*/

SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER SAMPLE (1);
 -- 1.5k rows

 SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER SAMPLE (0.1); -- 159 ROWS

 SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER SAMPLE (0.01); -- 17 ROWS.

 SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER SAMPLE (0.004); -- if you re run it keep[s changing]

 SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER SAMPLE SYSTEM(0.004); -- rerun will keepit  changing
 SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER SAMPLE SYSTEM(0.002) SEED (122);-- if you re run this the samples remain intact dont randomly change.
 

-- SEED is applicable only for SYSTEM or BLOCK type 
-- Bernoulli won't be affected by SEED

-- IF YOU NEED FIXED NUMBER OF ROWS (but different everytime):

SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER SAMPLE (4 ROWS);







