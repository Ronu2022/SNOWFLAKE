/*Table: Tree

+-------------+------+
| Column Name | Type |
+-------------+------+
| id          | int  |
| p_id        | int  |
+-------------+------+
id is the column with unique values for this table.
Each row of this table contains information about the id of a node and the id of its parent node in a tree.
The given structure is always a valid tree.
 

Each node in the tree can be one of three types:

"Leaf": if the node is a leaf node.
"Root": if the node is the root of the tree.
"Inner": If the node is neither a leaf node nor a root node.
Write a solution to report the type of each node in the tree.

Return the result table in any order.

The result format is in the following example.

 

Example 1:


Input: 
Tree table:
+----+------+
| id | p_id |
+----+------+
| 1  | null |
| 2  | 1    |
| 3  | 1    |
| 4  | 2    |
| 5  | 2    |
+----+------+
Output: 
+----+-------+
| id | type  |
+----+-------+
| 1  | Root  |
| 2  | Inner |
| 3  | Leaf  |
| 4  | Leaf  |
| 5  | Leaf  |
+----+-------+
Explanation: 
Node 1 is the root node because its parent node is null and it has child nodes 2 and 3.
Node 2 is an inner node because it has parent node 1 and child node 4 and 5.
Nodes 3, 4, and 5 are leaf nodes because they have parent nodes and they do not have child nodes.
Example 2:


Input: 
Tree table:
+----+------+
| id | p_id |
+----+------+
| 1  | null |
+----+------+
Output: 
+----+-------+
| id | type  |
+----+-------+
| 1  | Root  |
+----+-------+
Explanation: If there is only one node on the tree, you only need to output its root attributes.
*/
USE Practise;
CREATE OR REPLACE TABLE Tree 
(
    id INT PRIMARY KEY,
    p_id INT
);


INSERT INTO Tree (id, p_id) VALUES 
    (1, NULL), (2, 1), (3, 1), (4, 2), (5, 2), 
    (6, 3), (7, 3), (8, 4), (9, 4), (10, 5),
    (11, 5), (12, 6), (13, 6), (14, 7), (15, 7),
    (16, 8), (17, 8), (18, 9), (19, 9), (20, 10);

select * from Tree;


with cte as 
(
        select t1.id,t1.p_id,
        COALESCE(t2.no_of_child_node,0) as child_node_count,
        Case when 
                 t1.P_ID is NULL  THEN 'Root'
             WHEN
                 t1.P_ID IS NOT NULL AND COALESCE(t2.no_of_child_node,0) >= 1 THEN 'Inner'
             WHEN 
                 t1.P_ID IS NOT NULL AND COALESCE(t2.no_of_child_node,0) = 0  THEN 'Leaf'
            
            END AS node_flag
        from Tree as t1
    left join 
        (
            select p_id,
            count(*) as no_of_child_node
            from TREE
            group by p_id
            order by p_id asc
        ) as t2  on t1.id = t2.p_id
        
) 
select id,node_flag as type from cte;

-- Alternative way:

SELECT 
    id,
    case 
     when p_id is null then 'Root'
     when ID in (select p_id from Tree) Then 'Inner'
     else 'Leaf'
     end as type
from Tree;
