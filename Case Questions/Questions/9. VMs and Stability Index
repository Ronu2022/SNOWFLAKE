CREATE OR REPLACE TABLE vm_stability_data 
(
    calendar_month STRING,
    device_name STRING,
    health_event_name STRING,
    ms_stability_index DECIMAL(5, 2)
);

INSERT INTO vm_stability_data (calendar_month, device_name, health_event_name, ms_stability_index) 
VALUES 
    ('2023-01', 'L18055', 'CPU Spike', 6.3),
    ('2023-02', 'L18055', 'Memory Leak', 6.1),
    ('2023-03', 'L18055', 'Disk Failure', 5.5),
    ('2023-04', 'L18055', 'Network Drop', 7.0),

    -- L18381 
    ('2023-02', 'L18381', 'Memory Leak', 6.4),
    ('2023-03', 'L18381', 'CPU Spike', 6.0),
    ('2023-04', 'L18381', 'Disk Failure', 7.2),
    ('2023-05', 'L18381', 'Network Drop', 6.8),

    -- L18123 
    ('2023-03', 'L18123', 'Disk Failure', 4.8),
    ('2023-04', 'L18123', 'Network Drop', 6.2),
    ('2023-05', 'L18123', 'CPU Spike', 5.3),

    -- L18320 
    ('2023-01', 'L18320', 'Network Drop', 7.2),
    ('2023-02', 'L18320', 'CPU Spike', 5.4),
    ('2023-03', 'L18320', 'Memory Leak', 6.5),
    ('2023-04', 'L18320', 'Disk Failure', 6.0),

    -- L18734 
    ('2023-02', 'L18734', 'CPU Spike', 9.2),
    ('2023-03', 'L18734', 'Memory Leak', 3.9),
    ('2023-04', 'L18734', 'Disk Failure', 7.6),
    ('2023-05', 'L18734', 'Network Drop', 8.1),

    -- L18527 
    ('2023-03', 'L18527', 'Memory Leak', 3.9),
    ('2023-04', 'L18527', 'Network Drop', 6.5),
    ('2023-05', 'L18527', 'CPU Spike', 6.7),

    -- L18213 
    ('2023-04', 'L18213', 'Disk Failure', 8.6),
    ('2023-05', 'L18213', 'Network Drop', 5.2),
    ('2023-06', 'L18213', 'Memory Leak', 7.3),

    -- L18088 
    ('2023-06', 'L18088', 'CPU Spike', 6.9),
    ('2023-07', 'L18088', 'Memory Leak', 4.1),
    ('2023-08', 'L18088', 'Disk Failure', 6.8),

    -- L18149 
    ('2023-04', 'L18149', 'Memory Leak', 5.1),
    ('2023-05', 'L18149', 'Network Drop', 5.9),
    ('2023-06', 'L18149', 'CPU Spike', 6.3),

    -- L18741 
    ('2023-05', 'L18741', 'Disk Failure', 7.8),
    ('2023-06', 'L18741', 'Network Drop', 6.4),
    ('2023-07', 'L18741', 'Memory Leak', 5.2),

    -- L18805 
    ('2023-07', 'L18805', 'CPU Spike', 5.4),
    ('2023-08', 'L18805', 'Disk Failure', 7.0),
    ('2023-09', 'L18805', 'Memory Leak', 6.1),

    -- L18277 
    ('2023-08', 'L18277', 'Memory Leak', 3.7),
    ('2023-09', 'L18277', 'Disk Failure', 6.8),
    ('2023-10', 'L18277', 'Network Drop', 8.0),

    -- L18164 
    ('2023-09', 'L18164', 'Disk Failure', 8.3),
    ('2023-10', 'L18164', 'Memory Leak', 6.2),
    ('2023-10', 'L18164', 'CPU Spike', 5.5),

    -- L18765 
    ('2023-09', 'L18765', 'Memory Leak', 6.8),
    ('2023-10', 'L18765', 'Disk Failure', 4.4),
    ('2023-10', 'L18765', 'Network Drop', 9.5),

    -- L18893 
    ('2023-10', 'L18893', 'Network Drop', 9.7);


SELECT * FROM vm_stability_data;

/*
For Each Virtual Machine if the average  monthly stability indeX is < 5 for that month I want the count of each helth event ocurred that month
Calculate the same for VMs with avg monthly stability indeX > 8.5 for each month and then compare how health events are differing for VMs with stability indeX < 5 and  that with stability indeX > 8.5. 
I want to know what health events are occuring and how many times it is ocurring only if the avg stability indeX of a vm is < 5 for that Month

*/


/* Steps:

- get average stability indeX for all.
- isolate those where the Avg SI < 5.0
- Isolate those where Avg SI > 5.0
- Compare both the segments.
*/





-- Average Monthly indeX for Each vm

WITH Monthly_stability AS /*This to get the avg_stability_reference*/
(
    SELECT 
        calendar_month,
        device_name,
        ROUND(AVG(MS_STABILITY_INDEX),2) AS avg_stability_index
    FROM 
        vm_stability_data
    GROUP BY calendar_month,device_name
    ORDER BY calendar_month,device_name
        
),
Low_Stability AS /*To get the Low_stability records i.e records < 5*/
(
    SELECT 
        v.calendar_month,
        v.device_name,
        h.health_event_name,
        COUNT(h.HEALTH_EVENT_NAME)  as event_count,
    FROM Monthly_stability v
    JOIN vm_stability_data h
    ON v.device_name = h.device_name AND v.calendar_month = h.calendar_month
    WHERE avg_stability_index < 5
    GROUP BY v.calendar_month, v.device_name,h.health_event_name
    ORDER BY v.calendar_month, v.device_name,h.health_event_name
),
High_stability AS /*To Get the High_stability records i.e records with avg_stability_index > 8.5*/
(
        SELECT 
        v.calendar_month,
        v.device_name,
        h.health_event_name,
        COUNT(h.HEALTH_EVENT_NAME)  as event_count
    FROM Monthly_stability v
    JOIN vm_stability_data h
    ON v.device_name = h.device_name AND v.calendar_month = h.calendar_month
    WHERE avg_stability_index > 8.5
    GROUP BY v.calendar_month, v.device_name,h.health_event_name
    ORDER BY v.calendar_month, v.device_name,h.health_event_name
) /* Comparing Low Stability with High Stability records*/

SELECT 
    COALESCE(l.CALENDAR_MONTH,h.CALENDAR_MONTH) AS calendar_month,
    COALESCE(l.DEVICE_NAME,h.DEVICE_NAME) AS device_name,
    COALESCE(l.HEALTH_EVENT_NAME,'No LS Event') AS LS_event,
    COALESCE(l.EVENT_COUNT,0) AS LS_event_count,
    COALESCE(h.HEALTH_EVENT_NAME,'No HS Event') AS HS_event,
    COALESCE(h.EVENT_COUNT,0) AS HS_event_count
FROM Low_Stability l 
FULL OUTER JOIN High_stability h
ON l.CALENDAR_MONTH = h.CALENDAR_MONTH 
   AND l.DEVICE_NAME = h.DEVICE_NAME
   AND l.health_event_name = h.health_event_name
ORDER BY calendar_month,device_name;






-- Using Subqueries (NO CTE):



SELECT 
    COALESCE(l.CALENDAR_MONTH, h.CALENDAR_MONTH) AS calendar_month,
    COALESCE(l.DEVICE_NAME, h.DEVICE_NAME) AS device_name,
    COALESCE(l.HEALTH_EVENT_NAME, 'No LS Event') AS LS_event,
    COALESCE(l.EVENT_COUNT, 0) AS LS_event_count,
    COALESCE(h.HEALTH_EVENT_NAME, 'No HS Event') AS HS_event,
    COALESCE(h.EVENT_COUNT, 0) AS HS_event_count
FROM 
    (
        -- Subquery for Low Stability records
        SELECT 
            v.calendar_month,
            v.device_name,
            h.health_event_name,
            COUNT(h.HEALTH_EVENT_NAME) AS event_count
        FROM 
            vm_stability_data h
        JOIN 
            (
                SELECT 
                    calendar_month, 
                    device_name, 
                    ROUND(AVG(MS_STABILITY_INDEX), 2) AS avg_stability_index
                FROM 
                    vm_stability_data
                GROUP BY 
                    calendar_month, device_name
            ) v
        ON 
            v.device_name = h.device_name 
            AND v.calendar_month = h.calendar_month
        WHERE 
            v.avg_stability_index < 5
        GROUP BY 
            v.calendar_month, v.device_name, h.health_event_name
    ) l 
FULL OUTER JOIN 
    (
        -- Subquery for High Stability records
        SELECT 
            v.calendar_month,
            v.device_name,
            h.health_event_name,
            COUNT(h.HEALTH_EVENT_NAME) AS event_count
        FROM 
            vm_stability_data h
        JOIN 
            (
                SELECT 
                    calendar_month, 
                    device_name, 
                    ROUND(AVG(MS_STABILITY_INDEX), 2) AS avg_stability_index
                FROM 
                    vm_stability_data
                GROUP BY 
                    calendar_month, device_name
            ) v
        ON 
            v.device_name = h.device_name 
            AND v.calendar_month = h.calendar_month
        WHERE 
            v.avg_stability_index > 8.5
        GROUP BY 
            v.calendar_month, v.device_name, h.health_event_name
    ) h
ON 
    l.CALENDAR_MONTH = h.CALENDAR_MONTH 
    AND l.DEVICE_NAME = h.DEVICE_NAME 
    AND l.health_event_name = h.health_event_name
ORDER BY 
    calendar_month, device_name;
