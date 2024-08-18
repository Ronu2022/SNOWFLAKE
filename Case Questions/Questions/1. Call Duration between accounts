CREATE OR REPLACE TABLE call_logs
(
    from_id VARCHAR(20),
    to_id VARCHAR(20),
    duration NUMBER
)COMMENT = 'This is call_logs_table';



INSERT INTO call_logs VALUES
('1','2', 59),
('2','1',11),
('1','3',20),
('3','4',100),
('3','4',200),
('3','4',200),
('4','3',499);

SELECT * FROM call_logs;


SELECT *, LEAST(from_id, to_id) AS id1,
          GREATEST(from_id, to_id) AS id2
FROM call_logs; -- check this for each we get the least value and the greatest
-- then that can be grouped by.




SELECT 
    LEAST(from_id, to_id) AS id1,
    GREATEST(from_id, to_id) AS id2,
    SUM(duration) AS total_duration
FROM call_logs
GROUP BY LEAST(from_id, to_id), GREATEST(from_id, to_id);


WITH formatted_pair AS
(
SELECT *, 
        CASE WHEN from_id < to_id THEN from_id ELSE to_id END AS id1,
        CASE WHEN from_id < to_id  THEN to_id ELSE from_id END AS id2,
        
FROM call_logs
) 
SELECT ID1 as from_id, ID2 as to_id, SUM(duration) as total_duration 
FROM formatted_pair GROUP BY 1,2






