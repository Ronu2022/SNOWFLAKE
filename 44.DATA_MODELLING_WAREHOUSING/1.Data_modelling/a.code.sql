CREATE OR REPLACE DATABASE data_modelling;

CREATE OR REPLACE SCHEMA data_modelling_raw;


CREATE OR REPLACE SCHEMA data_modelling.MISC;

CREATE OR REPLACE TABLE data_modelling.data_modelling_raw.Orders
(
    row_id INT,
    order_id VARCHAR,
    order_date DATE,
    ship_date DATE,
    ship_mode VARCHAR,
    customer_id VARCHAR,
    customer_name VARCHAR,
    segment VARCHAR,
    country VARCHAR,
    city VARCHAR,
    state VARCHAR,
    postal_code INT,
    region VARCHAR,
    product_id VARCHAR,
    category VARCHAR,
    sub_category VARCHAR,
    product_name VARCHAR,
    sales DECIMAL,
    quantity INT,
    discount DECIMAL,
    profit DECIMAL
         
);

Select * from data_modelling.data_modelling_raw.Orders;


SELECT GET_DDL('table','data_modelling.data_modelling_raw.Orders');
// Observe: 

-- Problem 1 : REDUNDANCY
    -- Here Lets say you have United States in the COuntry 
    -- Gets stored again and again.
    -- Kind of Creates Data Redundancy.

-- Problem 2: DATA CONSISTENCY
    -- for eg, today -> Product Name is Rau's Ias and that get's stored in a row.
    -- TGomorrow, same product name was changed to lket's say Rau's IAS course -> there would be 2 records with different 
        -- product names, though the purchase was made for the same product.
        
-- Problem 3: Storing New Data (DATA MODELLING):
    -- Let's say a new Product is Introduced but it has not yet been purchased by any => no records because this is an orders table.
    -- Does that mean, we shouldnt have any documentation of new product  as long as there hasn;t been a single purchase ?
    -- to deal with that -> we should be having a table for products where all columns should be nul;l except product_id nad Product_name.

-- Problem 4: Faster Insertion (Data Insertion):


// DATA MODELLING:

    -- Process of Creating a styructured Representation of Data to defince how it is stored, organised and related within a system.
    -- Serving as a blueprint for database design and ensuring data consistency, integrity and scalability. 


// OLTV vs OLAP:

    --OLTP :
        -- ecommerce systems for order processing, banking syustems to handle user transactions.
        -- focuses on managing day to day transactional data.
        -- supports many short, simple queries like SELECT, INSERT, UPDATES and DELETEs
        -- optimised for speed and efficiency in processing large volumes of concurrent transactions.
        -- Data is normalized to avoid redundancy.

    -- OLAP:
        -- Datawarehouse for business Intelligence and Reporting
        -- Designed for COmplex Analytical Queries and Reporting for Histoprical Data.
        -- Supports Fewer, Longer and Complex Read-Only QAueries like Aggregations and Trends.
        -- Optimised for Query Performance, often using Denormalised Data for Faster Retrieval.

    -- Key Diff:
        -- OLTP: Real-Time, Transactional, Normalised, Relational Data Modelling.
        -- OLAP: Historical, Analytical, Denormalised for fast querying, Dimensional Modelling
        
        
// TYPES OF DATA MODEL: 
    -- CONCEPTUAL DATA MODEL: 
        -- Data is logically connected: 
        -- for eg: Entity: is what you care about -> Customer , Orders, Product
            -- Attribute is more details about the Enitity a Customer has Customer Id, Name, Address, Phone number etc.
            -- Minimises Redundancy. 

-------------------------------------------------------------------------------------------------------------------------------
            
/*
Normalisation: 
 -- used to organise an dsturecture Relational databases
 -- Minimises Redundancy -=> Reducing teh repetition of tyhe dsame data across Multiple Rows or Tables.
 -- It ensures that each piece of Data is stored only once, in a Single Location which eliminates Unnecessary Duplication.
 -- improves data integrity and inconsistency.
 -- Data stored at one place => Redicing the risk of Inconsistency
 -- NORMAL FORMS:
    -- 1st Normal Form ->  each cell has only one bvalues
        Customer        Phone Numbers
        Ronu               84560,23456
                This isn't 1st Normaliosed

        Customer        Phone Numbers
        Ronu                84560
        Ronu                23456

        This is a better way of normalisation
        Each cell now has a single value. No comma-separated chaos.


    -- 2nd  Normal Form-> Each 
        -- every detail should depend on the whole Primary Key - not just part of it. 
        --- Imagine this table:
            -- StudentID
            -- Course ID
            -- StudentName
            -- CourseName
        Here's Student name is dependent only on StudentID not the full Key that is StudentID + CourseID
            Fix:
             Split into two tables:
                -- Student(studentID, StudentName)
                -- CourseEnrollment(StudentID, CourseID)
                -- Course(CourseID, CourseName)
             Now each table has a clear purpose, and no detail is misplaced.

                
    --  3rd Normal Form (3NF): No Indirect Dependence
        -- Attribute should depend only on the Primary Key - not the other Attributes
        
        Imagine a table like this:
          employee_id  | Name | Department | Manager  |  | 
            101          Ronu    Finance      Alice
            102          Priya    Marketing     Bob

        --> Here Department Depends upon employee_id
        --> Here Name Depends on the Employee_id
        --> But manager depends on the Deparrtment => so not directlky connected with teh primary Key i.e employee_id
        --> employee_id is the primnary key
        --> Solution:
            Employee Table(Employee_id, Name)
            Department Table (Departement, Manager)
            => the above satifies but needs a bit of tweak
            => I mean, Employee Table(it has all non primary key dependent upon Primary key) but what about teh department of 
                each employee? that's missing.
            => Thus the correct one:
                EMployee Table(Employee_id, Name, Department) => Name and Departement is dependent upoin employee_id
                Department Table (Department, Manager)




    */   

-------------------------------------------------------------------------------------------------------------------------------
/*

order_id	product_name	  order_date	   customer_name	  customer_city	  product_price	      quantity
101	        Laptop	         2025-09-01	     Rahul Mehta	    Mumbai	         55000	              1
102	        Headphones	     2025-09-03	     Sneha Sharma	   Bangalore	      3000	              2
103	        Office Chair	  2025-09-05	Arjun Verma	          Delhi	          7000	              1


2nd normal form => all the columns must be dependent on total Primary key not partial

step 1: 


Orders Table :
-- Order_date is dependednt upon Order__id
-- Customer_name is dependent upon the order_id
-- customer_City is dependent upon the Customer_name which is dependent upon order_id
-- Here primary Key is order_id and order_date:
-- so for 2nd NF -> all attributes should eb fully depenedent upon the entire Primary key
order_id		 order_date	   customer_name	  customer_city
101	        	 2025-09-01	     Rahul Mehta	    Mumbai		
102	        	 2025-09-03	     Sneha Sharma	    Bangalore
103	        	 2025-09-05	     Arjun Verma	    Delhi	

Orders Items:
-- here the composite Primary Key (order_id, product_name)


order_id	product_name   product_price	      quantity
101	        Laptop          55000	              1
102	        Headphones      3000	              2
103	        Office Chair    7000	              1

--> if you observe in  Orders Items Table, product _price is dependent upon Product_name not order_id

thus needs a change:


Step 2:

Orders Table: 

order_id		 order_date	   customer_name	  customer_city
101	        	 2025-09-01	     Rahul Mehta	    Mumbai		
102	        	 2025-09-03	     Sneha Sharma	    Bangalore
103	        	 2025-09-05	     Arjun Verma	    Delhi	


Orders_Items: 
order_id    product_id(FK)   quantity
101	        1                   1
102         2                   2
103         3                   1


Products:

Product_id(PK)  product_name     product_price
1               Laptop              55000
2               Headphones          3000
3               Office Chair        7000 

---------------------------------------------------------------------------------------------------------------------

3rd NF:
order_id	product_name	  order_date	   customer_name	  customer_city	  product_price	      quantity
101	        Laptop	         2025-09-01	     Rahul Mehta	    Mumbai	         55000	              1
102	        Headphones	     2025-09-03	     Sneha Sharma	   Bangalore	      3000	              2
103	        Office Chair	  2025-09-05	Arjun Verma	          Delhi	          7000	              1



order_id		 order_date	   customer_name	  customer_city
101	        	 2025-09-01	     Rahul Mehta	    Mumbai		
102	        	 2025-09-03	     Sneha Sharma	    Bangalore
103	        	 2025-09-05	     Arjun Verma	    Delhi	

=> this was the order_stable but -> Customer_name has no direct relationship with pk that is order_id

create a separate Table for Customers

order_id		 order_date	   customer_name	  customer_city
101	        	 2025-09-01	     Rahul Mehta	    Mumbai		
102	        	 2025-09-03	     Sneha Sharma	    Bangalore
103	        	 2025-09-05	     Arjun Verma	    Delhi	

Orders Table: 
order_id		 order_date	      Customer_id(FK)
101	        	 2025-09-01	      1	    	
102	        	 2025-09-03	      2	    
103	        	 2025-09-05	      3	


Orders_Items: 
order_id    product_id(FK)   quantity
101	        1                   1
102         2                   2
103         3                   1

Products:



Product_id(PK)  product_name     product_price
1               Laptop              55000
2               Headphones          3000
3               Office Chair        7000 
*/


Select * from data_modelling.data_modelling_raw.Orders;


describe TABLE data_modelling.data_modelling_raw.Orders;

CREATE OR REPLACE SEQUENCE data_modelling.MISC.AUTO_SEQ
START = 1
INCREMENT = 1;




// CITY TABLE:



CREATE OR REPLACE TABLE data_modelling.data_modelling_raw.CITY_TABLE
(
    ID INTEGER PRIMARY KEY DEFAULT data_modelling.MISC.AUTO_SEQ.NEXTVAL,
    CITY VARCHAR,
    POSTAL_CODE INTEGER,
    STATE VARCHAR,
    REGION VARCHAR,
    COUNTRY VARCHAR
    
);


INSERT INTO data_modelling.data_modelling_raw.CITY_TABLE(CITY,POSTAL_CODE,STATE,REGION,COUNTRY) 
SELECT CITY,POSTAL_CODE, STATE, REGION, COUNTRY
FROM
(
    SELECT CITY,POSTAL_CODE, STATE, REGION, COUNTRY,ORDER_DATE,
    ROW_NUMBER() OVER(PARTITION BY CITY, POSTAL_CODE ORDER BY ORDER_DATE DESC) AS RN_NO
    FROM data_modelling.data_modelling_raw.Orders 
    QUALIFY RN_NO = 1
) ORDER BY CITY ASC;



// LOCATION_TABLE

CREATE OR REPLACE TABLE data_modelling.data_modelling_raw.LOCATION_TABLE
(
    ID INTEGER PRIMARY KEY DEFAULT data_modelling.MISC.AUTO_SEQ.NEXTVAL,
    POSTAL_CODE INTEGER,
    CITY_ID INTEGER,
    CONSTRAINT FK_CITY_ID FOREIGN KEY (CITY_ID) REFERENCES data_modelling.data_modelling_raw.CITY_TABLE(ID)
);

INSERT INTO data_modelling.data_modelling_raw.LOCATION_TABLE(POSTAL_CODE,CITY_ID) 
SELECT T1.POSTAL_CODE, T2.ID AS CITY_ID
FROM
(
    SELECT POSTAL_CODE, CITY,ORDER_DATE,
    ROW_NUMBER() OVER (PARTITION BY CITY,POSTAL_CODE ORDER BY ORDER_DATE DESC) AS RN
    FROM data_modelling.data_modelling_raw.Orders
    QUALIFY RN = 1
) AS T1
LEFT JOIN data_modelling.data_modelling_raw.CITY_TABLE as T2
on T1.postal_code = T2.postal_code and T1.city = T2.city
ORDER BY CITY_ID ASC;




// CUSTOMER_TABLE:

CREATE OR REPLACE TABLE data_modelling.data_modelling_raw.CUSTOMER_TABLE
(
    CUSTOMER_ID VARCHAR PRIMARY KEY,
    CUSTOMER_NAME VARCHAR,
    SEGMENT VARCHAR,
    LOCATION_ID INT,
    CONSTRAINT FK_LOCATION_ID FOREIGN KEY (LOCATION_ID) REFERENCES data_modelling.data_modelling_raw.LOCATION_TABLE(ID)
);

INSERT INTO data_modelling.data_modelling_raw.CUSTOMER_TABLE(CUSTOMER_ID,CUSTOMER_NAME,SEGMENT,LOCATION_ID)
SELECT C.CUSTOMER_ID,C.CUSTOMER_NAME,C.SEGMENT,L.id AS LOCATION_ID FROM
(
    SELECT CUSTOMER_ID, 
        CUSTOMER_NAME,SEGMENT,CITY,POSTAL_CODE,ORDER_DATE,
        ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY ORDER_DATE DESC) AS RN
        FROM data_modelling.data_modelling_raw.Orders
        QUALIFY RN = 1
) AS C
LEFT JOIN data_modelling.data_modelling_raw.LOCATION_TABLE AS L
ON  C.POSTAL_CODE = L.POSTAL_CODE;



// ORDERS_TABLE:

CREATE OR REPLACE TABLE data_modelling.data_modelling_raw.ORDERS_TABLE
(
    ORDER_ID VARCHAR PRIMARY KEY,
    ORDER_DATE DATE,
    CUSTOMER_ID VARCHAR,
    CONSTRAINT FK_CX_ID FOREIGN KEY (CUSTOMER_ID) 
        REFERENCES data_modelling.data_modelling_raw.CUSTOMER_TABLE(CUSTOMER_ID)
);

INSERT INTO data_modelling.data_modelling_raw.ORDERS_TABLE(ORDER_ID,ORDER_DATE,CUSTOMER_ID)
SELECT 
DISTINCT
ORDER_ID,
ORDER_DATE,
--PRODUCT_ID,
CUSTOMER_ID,
--DISCOUNT
FROM data_modelling.data_modelling_raw.ORDERS
ORDER BY ORDER_ID ASC, ORDER_DATE ASC;


// SUBCATEGORY TABLE:

CREATE OR REPLACE TABLE data_modelling.data_modelling_raw.SUB_CATEGORY_TABLE
(
    ID INTEGER PRIMARY KEY IDENTITY(1,1),
    SUB_CATEGORY VARCHAR,
    CATEGORY VARCHAR
);

INSERT INTO data_modelling.data_modelling_raw.SUB_CATEGORY_TABLE(SUB_CATEGORY,CATEGORY)
SELECT 
 DISTINCT SUB_CATEGORY,
          CATEGORY FROM data_modelling.data_modelling_raw.Orders;




// PRODUCTS TABLE:

CREATE OR REPLACE TABLE data_modelling.data_modelling_raw.PRODUCTS_TABLE
(
    PRODUCT_ID VARCHAR PRIMARY KEY,
    PRODUCT_NAME VARCHAR,
    SUB_CATEGORY_ID INT,
    CONSTRAINT FK_CUB_CAT FOREIGN KEY (SUB_CATEGORY_ID) REFERENCES data_modelling.data_modelling_raw.SUB_CATEGORY_TABLE(ID)

);

INSERT INTO data_modelling.data_modelling_raw.PRODUCTS_TABLE(PRODUCT_ID,PRODUCT_NAME,SUB_CATEGORY_ID)

SELECT P.PRODUCT_ID,P.PRODUCT_NAME,
S.ID AS SUB_CATEGORY_ID
FROM
(

SELECT 
        PRODUCT_ID,
        PRODUCT_NAME,
        SUB_CATEGORY, CATEGORY ,ORDER_DATE,
        ROW_NUMBER() OVER (PARTITION BY PRODUCT_ID ORDER BY ORDER_DATE DESC) AS ROW_NUM
        
    FROM data_modelling.data_modelling_raw.Orders
   -- ORDER BY PRODUCT_ID ASC, ORDER_DATE DESC
    QUALIFY ROW_NUM = 1
) AS P
LEFT JOIN data_modelling.data_modelling_raw.SUB_CATEGORY_TABLE AS S ON P.SUB_CATEGORY = S.SUB_CATEGORY
ORDER BY SUB_CATEGORY_ID ASC;


SELECT * FROM data_modelling.data_modelling_raw.PRODUCTS_TABLE;


// ORDERS_ITEMS TABLE:


CREATE OR REPLACE TABLE data_modelling.data_modelling_raw.ORDER_ITEMS_TABLE
(
    ORDER_ID VARCHAR,
    PRODUCT_ID VARCHAR,
    QUANTITY INT,
    SALES DECIMAL,
    DISCOUNT DECIMAL,
    CONSTRAINT PK_ORDER_ITEMS PRIMARY KEY (ORDER_ID, PRODUCT_ID),
    CONSTRAINT FK_ORDER_ID FOREIGN KEY (ORDER_ID) REFERENCES data_modelling.data_modelling_raw.ORDERS_TABLE(ORDER_ID),
    CONSTRAINT FK_PRODUCT_ID FOREIGN KEY (PRODUCT_ID) REFERENCES data_modelling.data_modelling_raw.PRODUCTS_TABLE(PRODUCT_ID)
);




INSERT INTO data_modelling.data_modelling_raw.ORDER_ITEMS_TABLE(ORDER_ID,PRODUCT_ID,QUANTITY,SALES,DISCOUNT)
SELECT
    DISTINCT
    ORDER_ID,
    PRODUCT_ID,
    QUANTITY,
    SALES,
    DISCOUNT
FROM data_modelling.data_modelling_raw.Orders;
    



// ORDER_SHIPPING_TABLE:

CREATE OR REPLACE TABLE data_modelling.data_modelling_raw.SHIPPING_TABLE
(
    ORDER_ID VARCHAR,
    PRODUCT_ID VARCHAR,
    SHIP_DATE DATE,
    SHIP_MODE VARCHAR,
    CONSTRAINT PK_CONS PRIMARY KEY (ORDER_ID, PRODUCT_ID),
    CONSTRAINT FK_PRODUCT_ID FOREIGN KEY (PRODUCT_ID) REFERENCES data_modelling.data_modelling_raw.PRODUCTS_TABLE(PRODUCT_ID),
    CONSTRAINT FK_ORDER_ID FOREIGN KEY (ORDER_ID) REFERENCES data_modelling.data_modelling_raw.ORDERS_TABLE(ORDER_ID)
    
    
);


INSERT INTO data_modelling.data_modelling_raw.SHIPPING_TABLE(ORDER_ID,PRODUCT_ID,SHIP_DATE,SHIP_MODE)
SELECT  DISTINCT ORDER_ID, PRODUCT_ID, SHIP_DATE,
SHIP_MODE
FROM data_modelling.data_modelling_raw.Orders;

SELECT * FROM data_modelling.data_modelling_raw.SHIPPING_TABLE;




