Create or replace database clustering;
create or replace schema clustering_schema; 

-- CREATING SALES TABLE
CREATE OR REPLACE TABLE SALES
(
    Date DATE,
    Region STRING,
    ProductID INT,
    SalesAmount FLOAT,
    CustomerID INT
);


INSERT INTO SALES VALUES ('2024-03-26', 'West', 1, 855.61, 10);
INSERT INTO SALES VALUES ('2024-01-19', 'West', 5, 233.3, 10);
INSERT INTO SALES VALUES ('2024-01-22', 'North', 4, 102.61, 4);
INSERT INTO SALES VALUES ('2024-10-27', 'North', 3, 328.42, 3);
INSERT INTO SALES VALUES ('2024-04-10', 'North', 4, 101.37, 9);
INSERT INTO SALES VALUES ('2024-12-18', 'West', 2, 486.48, 10);
INSERT INTO SALES VALUES ('2024-06-05', 'North', 5, 204.98, 3);
INSERT INTO SALES VALUES ('2024-06-20', 'East', 3, 245.12, 8);
INSERT INTO SALES VALUES ('2024-01-19', 'North', 4, 74.69, 1);
INSERT INTO SALES VALUES ('2024-09-06', 'East', 4, 93.12, 8);


-- CREATING INVENTORY TABLE
CREATE OR REPLACE TABLE INVENTORY
(
    Date DATE,
    ProductID INT,
    StockQuantity INT
);


INSERT INTO INVENTORY VALUES ('2024-11-11', 1, 50);  -- Laptop
INSERT INTO INVENTORY VALUES ('2024-03-07', 2, 30);  -- Smartphone
INSERT INTO INVENTORY VALUES ('2024-08-21', 3, 45);  -- Tablet
INSERT INTO INVENTORY VALUES ('2024-06-10', 4, 80);  -- Headphones
INSERT INTO INVENTORY VALUES ('2024-07-06', 5, 35);  -- Monitor
INSERT INTO INVENTORY VALUES ('2024-12-23', 6, 25);  -- Office Chair
INSERT INTO INVENTORY VALUES ('2024-07-01', 7, 200); -- Notebook
INSERT INTO INVENTORY VALUES ('2024-05-16', 8, 18);  -- Printer
INSERT INTO INVENTORY VALUES ('2024-11-06', 9, 120); -- Water Bottle
INSERT INTO INVENTORY VALUES ('2024-11-28', 10, 60); -- Gaming Mouse


-- -- CREATING PRODUCTS TABLE
CREATE OR REPLACE TABLE PRODUCTS 
(
    ProductID INT,
    ProductName STRING,
    Category STRING,
    Price FLOAT
);


INSERT INTO PRODUCTS VALUES (1, 'Laptop', 'Electronics', 999.99);
INSERT INTO PRODUCTS VALUES (2, 'Smartphone', 'Electronics', 499.99);
INSERT INTO PRODUCTS VALUES (3, 'Tablet', 'Electronics', 299.99);
INSERT INTO PRODUCTS VALUES (4, 'Headphones', 'Accessories', 89.99);
INSERT INTO PRODUCTS VALUES (5, 'Monitor', 'Electronics', 199.99);
INSERT INTO PRODUCTS VALUES (6, 'Office Chair', 'Furniture', 149.99);
INSERT INTO PRODUCTS VALUES (7, 'Notebook', 'Stationery', 2.49);
INSERT INTO PRODUCTS VALUES (8, 'Printer', 'Office Supplies', 179.99);
INSERT INTO PRODUCTS VALUES (9, 'Water Bottle', 'Home & Kitchen', 15.99);
INSERT INTO PRODUCTS VALUES (10, 'Gaming Mouse', 'Gaming Accessories', 59.99);



-- CREATING SHIPMENTS TABLE
CREATE OR REPLACE TABLE SHIPMENTS (
    ShipmentID STRING,
    OrderID INT,
    ShipDate DATE,
    OriginRegion STRING,
    DestinationRegion STRING,
    Carrier STRING,
    ShipmentWeight FLOAT,
    Status STRING
);

INSERT INTO SHIPMENTS VALUES ('SHP000', 1000, '2024-03-25', 'North', 'West', 'UPS', 9.09, 'Delivered');
INSERT INTO SHIPMENTS VALUES ('SHP001', 1001, '2024-07-15', 'South', 'North', 'FedEx', 47.59, 'In Transit');
INSERT INTO SHIPMENTS VALUES ('SHP002', 1002, '2024-09-28', 'North', 'East', 'DHL', 12.78, 'Delayed');
INSERT INTO SHIPMENTS VALUES ('SHP003', 1003, '2024-06-20', 'South', 'East', 'BlueDart', 48.86, 'Delivered');
INSERT INTO SHIPMENTS VALUES ('SHP004', 1004, '2024-03-03', 'South', 'West', 'Delhivery', 39.62, 'Cancelled');
INSERT INTO SHIPMENTS VALUES ('SHP005', 1005, '2024-01-12', 'North', 'East', 'UPS', 17.19, 'In Transit');
INSERT INTO SHIPMENTS VALUES ('SHP006', 1006, '2024-03-26', 'East', 'North', 'DHL', 14.35, 'Delivered');
INSERT INTO SHIPMENTS VALUES ('SHP007', 1007, '2024-01-12', 'North', 'West', 'FedEx', 40.36, 'Out for Delivery');
INSERT INTO SHIPMENTS VALUES ('SHP008', 1008, '2024-02-17', 'East', 'North', 'Delhivery', 41.38, 'Delivered');
INSERT INTO SHIPMENTS VALUES ('SHP009', 1009, '2024-09-26', 'North', 'South', 'BlueDart', 45.50, 'In Transit');



--CREATING CUSTOMERS TABLE
CREATE OR REPLACE TABLE CUSTOMERS 
(
    CustomerID INT PRIMARY KEY,
    Name VARCHAR(255),
    Region VARCHAR(50),
    AgeGroup VARCHAR(20)
);

INSERT INTO CUSTOMERS VALUES (1, 'Customer1', 'North', '36-45');
INSERT INTO CUSTOMERS VALUES (2, 'Customer2', 'West', '26-35');
INSERT INTO CUSTOMERS VALUES (3, 'Customer3', 'North', '18-25');
INSERT INTO CUSTOMERS VALUES (4, 'Customer4', 'North', '36-45');
INSERT INTO CUSTOMERS VALUES (5, 'Customer5', 'East', '46+');
INSERT INTO CUSTOMERS VALUES (6, 'Customer6', 'North', '36-45');
INSERT INTO CUSTOMERS VALUES (7, 'Customer7', 'South', '26-35');
INSERT INTO CUSTOMERS VALUES (8, 'Customer8', 'East', '46+');
INSERT INTO CUSTOMERS VALUES (9, 'Customer9', 'North', '36-45');
INSERT INTO CUSTOMERS VALUES (10, 'Customer10', 'West', '46+');



-- CREATING TRANSACTIONS TABLE
CREATE OR REPLACE TABLE TRANSACTIONS 
(
    TransactionID STRING,
    CustomerID STRING,
    TransactionDate DATE,
    TransactionAmount FLOAT,
    PaymentMethod STRING,
    TransactionStatus STRING
);


INSERT INTO TRANSACTIONS VALUES ('TXN000', '3', '2024-03-05', 812.31, 'Credit Card', 'Completed');
INSERT INTO TRANSACTIONS VALUES ('TXN001', '6', '2024-07-08', 529.77, 'PayPal', 'Pending');
INSERT INTO TRANSACTIONS VALUES ('TXN002', '9', '2024-03-02', 586.35, 'Debit Card', 'Completed');
INSERT INTO TRANSACTIONS VALUES ('TXN003', '2', '2024-10-19', 208.09, 'Bank Transfer', 'Failed');
INSERT INTO TRANSACTIONS VALUES ('TXN004', '5', '2024-09-22', 395.69, 'Credit Card', 'Completed');
INSERT INTO TRANSACTIONS VALUES ('TXN005', '9', '2024-01-25', 871.14, 'Crypto', 'Completed');
INSERT INTO TRANSACTIONS VALUES ('TXN006', '3', '2024-09-22', 701.80, 'Credit Card', 'Pending');
INSERT INTO TRANSACTIONS VALUES ('TXN007', '3', '2024-10-18', 406.39, 'Debit Card', 'Refunded');
INSERT INTO TRANSACTIONS VALUES ('TXN008', '5', '2024-10-06', 959.97, 'PayPal', 'Completed');
INSERT INTO TRANSACTIONS VALUES ('TXN009', '6', '2024-09-29', 253.95, 'Credit Card', 'Failed');


-- Final Tables:
select * from SALES;
select * from INVENTORY;
select * from PRODUCTS;
select * from SHIPMENTS;
select * from CUSTOMERS;
select * from TRANSACTIONS;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Scenario: Reporting involves filtering Sales and Inventory tables by Date, Region, and ProductID.
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Clustering Definition:


Select * from sales where date = '2024-04-10' and PRODUCTID = 4; -- 163ms

ALTER TABLE SALES CLUSTER BY (DATE,REGION,PRODUCTID); -- Cluster Created

Select * from sales where date = '2024-04-10' and PRODUCTID = 4 and region = 'North'; -- 90 ms  took less time

ALTER TABLE INVENTORY CLUSTER BY (DATE,PRODUCTID);



Select 
    s.region, p.productname,
    sum(s.salesamount) as total_sales,
    sum(i.stockquantity) as available_stock
from sales as s 
left join products as p on s.productid = p.productid
left join inventory as i on s.productid = i.productid
WHERE s.Date BETWEEN '2024-01-01' AND '2024-01-31' AND s.Region = 'North'
group by s.region, p.productname
order by TOTAL_SALES desc; 

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Scenario: Analysts frequently filter Transactions by TransactionDate and CustomerID.
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 2. Historical Data with Temporal Queries

select * from TRANSACTIONS;

ALTER TABLE TRANSACTIONS CLUSTER BY (transactiondate,customerid);

select 
    customerid,
    count(transactionid) as total_transactions,
    sum(TRANSACTIONAMOUNT) as total_transactions_amount
from transactions where TransactionDate BETWEEN '2024-03-01' AND '2024-07-20'  AND CustomerID IN (3, 6)
group by customerid;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Scenario: Shipments are analyzed by DestinationRegion and ShipDate.
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 3. Geo-Spatial Analysis

-- Clustering Definition:

Select * from shipments;


select 
DESTINATIONREGION,
count(shipmentid) as total_shipments,
min(shipdate) as FirstShipmentDate,
max(shipdate) as LastShipmentDate
FROM shipments
WHERE DESTINATIONREGION IN ('West','North','South') AND SHIPDATE BETWEEN '2024-03-25' AND '2024-09-28'
GROUP BY DESTINATIONREGION; -- 120ms


ALTER TABLE SHIPMENTS CLUSTER BY (DESTINATIONREGION, shipdate);

select 
DESTINATIONREGION,
count(shipmentid) as total_shipments,
min(shipdate) as FirstShipmentDate,
max(shipdate) as LastShipmentDate
FROM shipments
WHERE DESTINATIONREGION IN ('West','North') AND SHIPDATE BETWEEN '2024-03-25' AND '2024-09-28'
GROUP BY DESTINATIONREGION; -- 89 ms




------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Clustering Analysis and Optimization
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Analyze Clustering Information:
-- To check the clustering depth and quality:

SELECT SYSTEM$CLUSTERING_INFORMATION('clustering.clustering_schema.sales');


select parse_json(SYSTEM$CLUSTERING_INFORMATION('clustering.clustering_schema.sales'));


-- Accessing using Parse_json
SELECT variant_column:average_depth as average_depth,
       variant_column:average_overlaps as average_overlaps,
       variant_column:cluster_by_keys as cluster_by_keys,
       variant_column:total_constant_partition_count as total_constant_partition_count,
       variant_column:total_partition_count  as total_partition_count
FROM 
(
    select parse_json(SYSTEM$CLUSTERING_INFORMATION('clustering.clustering_schema.sales')) as variant_column -- gave a name variant_column why? because parse_json
    -- brings a variant datatype.
) as json_date;


-- 1. Monitoring Clustering Metrics
    -- Scheduled Clustering Depth Monitoring
        -- You can create a task in Snowflake to regularly check clustering metrics and log results for analysis.


// Log Table:

Create or replace table CLUSTERING_METRICS_LOG 
(
    LogTimestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    TABLENAME STRING DEFAULT 'sales',
    clustering_keys STRING DEFAULT 'Date | Region | PRODUCTID',
    cluster_method STRING,
    average_depth INT,
    average_overlaps int,
    TotalPartitions  int
    
);

// stored procedure:

Create or replace procedure pro_clustering_log()
RETURNS STRING
LANGUAGE SQL AS 
$$
BEGIN
    TRUNCATE clustering.clustering_schema.CLUSTERING_METRICS_LOG; 
    
    INSERT INTO clustering.clustering_schema.CLUSTERING_METRICS_LOG(cluster_method,average_depth,average_overlaps,TotalPartitions)
    WITH CTE AS
    (
        SELECT variant_column:average_depth as average_depth,
               variant_column:average_overlaps as average_overlaps,
               variant_column:cluster_by_keys as cluster_by_keys,
               variant_column:total_constant_partition_count as total_constant_partition_count,
               variant_column:total_partition_count  as total_partition_count
        FROM 
        (
            select parse_json(SYSTEM$CLUSTERING_INFORMATION('clustering.clustering_schema.sales')) as variant_column 
        ) as json_date
    ) select cluster_by_keys, average_depth, average_overlaps,total_partition_count from cte;

    RETURN 'Clustering Metrics Logged Successfully';
END;
$$; 

// task: 
CREATE OR REPLACE TASK dummy_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON * * * * * UTC'
AS CALL  pro_clustering_log(); 


// Resuming Task:

ALTER TASK dummy_task RESUME; 


// Single Time Call - Just to check.

CALL pro_clustering_log();

select * from CLUSTERING_METRICS_LOG;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*
To analyze query performance in Snowflake, you can use Account Usage and Information Schema views. 
Snowflake provides detailed metadata about query history, resource usage, and storage, 
enabling you to identify and optimize slow queries, understand clustering efficiency, and monitor resource consumption.
*/

--1. Query Execution Metrics
-- Create a view to retrieve detailed information about query execution time, scanned rows, and returned rows.

/*
Key Columns : 

total_elapsed_time: Total execution time in milliseconds.
rows_produced: Number of rows produced during query execution.
bytes_scanned: Amount of data scanned, in bytes.

select bytes_scanned from snowflake.account_usage.query_history;
*/



Select 
QUERY_ID,
user_name,
database_name,
schema_name,
query_text,
execution_status,
start_time,
end_time,
total_elapsed_time /1000  AS elapsed_time_seconds,
rows_produced,
    ROWS_INSERTED,
    ROWS_UPDATED,
    ROWS_DELETED,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL,
    ROUND(BYTES_SCANNED / 1024 / 1024,5) AS bytes_scanned_mb,
    ROUND(BYTES_WRITTEN / 1024 / 1024.5) AS bytes_written_mb
    
FROM snowflake.account_usage.query_history

WHERE start_time >= DATEADD(DAY, -7, CURRENT_DATE) -- Last 7 days
AND execution_status = 'SUCCESS'
-- AND SCHEMA_NAME = 'DEMO_SCHEMA'
ORDER BY total_elapsed_time DESC;

-- 2. Long-Running Queries
-- Identify queries with the highest execution times.

CREATE OR REPLACE VIEW long_running_queries AS
SELECT 
    query_id,
    user_name,
    warehouse_name,
    ROUND(total_elapsed_time / 1000,0) AS elapsed_time_seconds,
    query_text
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE)
AND total_elapsed_time > 60000 -- Queries running longer than 60 seconds
ORDER BY total_elapsed_time DESC;

SELECT * FROM long_running_queries;



