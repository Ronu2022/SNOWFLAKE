
CREATE OR REPLACE TABLE loan_feedback
 (
    feedback_id INTEGER AUTOINCREMENT,
    customer_id INTEGER,
    loan_id INTEGER,
    feedback_text STRING,
    feedback_tags STRING
);


INSERT INTO loan_feedback (feedback_id, customer_id, loan_id, feedback_tags) VALUES
(1, 101, 201, 'fast, friendly, reliable'),
(2, 102, 202, 'friendly, reliable'),
(3, 103, 203, 'fast, reliable'),
(4, 104, 204, 'fast, friendly'),
(5, 105, 205, 'reliable, friendly, fast');

INSERT INTO loan_feedback (feedback_id, customer_id, loan_id, feedback_tags) VALUES
(6, 106, 206, 'fast, friendly, reliable'),
(7, 107, 207, 'friendly, reliable'),
(8, 108, 208, 'fast, reliable, friendly'),
(9, 109, 209, 'friendly, reliable'),
(10, 110, 210, 'fast, reliable'),
(11, 111, 211, 'fast, friendly'),
(12, 112, 212, 'reliable, friendly'),
(13, 113, 213, 'fast, friendly, reliable'),
(14, 114, 214, 'friendly, reliable, fast'),
(15, 115, 215, 'reliable, fast, friendly');


SELECT * FROM loan_feedback;


-- Check in the occurence of Each of the feedback_tags  vis-a-vis the feedback_text:

-- Find for each of the tags whats the count and the customer ids that have used the tags.




WITH feed_back_tags_split_count AS
(

    SELECT customer_id, 
           loan_id,
           splitted_tags.value AS tag_types
    
    FROM loan_feedback,
         TABLE(split_to_table(feedback_tags, ',')) AS splitted_tags
    
 ) SELECT tag_types,
          COUNT(*) AS total_count, 
          LISTAGG(customer_id, ' | ') AS customer_ids,
          FROM feed_back_tags_split_count
          GROUP BY 1
          ORDER BY 2 DESC;
