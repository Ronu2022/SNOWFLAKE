CREATE  OR REPLACE TABLE Products
(
    Product VARCHAR(100),
    Quantity INTEGER NOT NULL
);


INSERT INTO Products VALUES
('Mobile',3),
('TV',5),
('Tablet',4);


SELECT * FROM products;

-- UNGROUP THESE TABLES
/* for instance if Mobile =3, there should be 3 records like mobile 1, mobile 1, mobile 1 */


-- Testing with Just One Product:

WITH RECURSIVE cte_join AS
(
    -- Base case: Select one row with product 'Mobile' and Quantity 1, row_number = 1
    SELECT product, 1 AS Quantity, 1 AS row_number, quantity AS max_quantity
    FROM products
    WHERE product = 'Mobile'

    UNION ALL

    -- Recursive case: Join the CTE to itself to create additional rows
    SELECT 
        h.product, 
        1 AS Quantity, 
        h.row_number + 1 AS row_number,  -- Increment row_number
        h.max_quantity
    FROM cte_join h
    WHERE h.row_number < h.max_quantity  -- Stop recursion when row_number reaches max_quantity
)

-- Final result: Get all the rows from the CTE
SELECT product, Quantity
FROM cte_join
ORDER BY row_number;





-- all  /*Note how long is this*/ 


WITH recursive_cte_mobile AS
(
   SELECT product, 1 AS Quantity, 1 AS row_number, Quantity AS max_quantity FROM products WHERE product = 'Mobile'
    UNION ALL
    SELECT 
     rm.product,
     1 as Quantity,
     rm.row_number + 1 AS row_number,
     rm.max_quantity 
    FROM recursive_cte_mobile AS rm
    WHERE  rm.row_number < rm.max_quantity
),
 recursive_cte_TV AS
 (
     SELECT product, 1 AS Quantity, 1 AS row_number , Quantity AS max_quantity FROM products WHERE product = 'TV'
        UNION ALL
     SELECT 
         rtv.product, 
         1 AS Quantity,
         rtv.row_number + 1 AS row_number,
         rtv.max_quantity
         FROM recursive_cte_TV AS rtv
         WHERE rtv.row_number < rtv.max_quantity
),
recursive_cte_tablet AS
(
    SELECT product,1 AS Quantity, 1 AS row_number, Quantity AS max_quantity FROM products WHERE product = 'Tablet'
      UNION ALL
    SELECT 
        rta.product,
        1 AS Quantity,
        rta.row_number + 1 AS row_number,
        rta.max_quantity 
    FROM recursive_cte_tablet AS rta
    WHERE rta.row_number < rta.max_quantity
            
 )
SELECT product,quantity FROM recursive_cte_mobile
UNION ALL
SELECT product,quantity FROM recursive_cte_TV
UNION ALL
SELECT product,quantity FROM recursive_cte_tablet


WITH recursive cte AS
(
    SELECT product,quantity FROM products
    UNION ALL
    SELECT product, quantity- 1 AS quantity FROM cte
    WHERE quantity-1 > 0
    
),
final_cte AS
(
    select product, 1 AS Quantity
    from cte
)select * from final_cte order by product;
