✅ What it means:
It’s a problem that happens when you try to update rows in a table based on some condition, and the update itself changes the condition — so the same row can get updated more than once by mistake.

❓Why is it called “Halloween”?
It was first found on Halloween day by IBM engineers in 1976 — so they called it the Halloween Problem.


CREATE OR REPLACE TABLE employees (
  id INT,
  name STRING,
  salary INT
);

INSERT INTO employees VALUES 
(1, 'Alice', 50000),
(2, 'Bob', 59000),
(3, 'Charlie', 55000);


-- Let's say we nee dto update salary

UPDATE employees
SET salary = salary + 1000
WHERE salary < 60000;


❓What do you expect?
Each person who earns less than 60000 should get a 1000 raise, once.

❌ What could go wrong (in other systems)?
Bob starts at 59000 → after one raise becomes 60000. But if the database isn’t careful, Bob might get picked again before the update finishes — so he gets two raises, which is wrong.


✅ Does Snowflake have this issue?
No. Snowflake protects you from this.

Behind the scenes, Snowflake makes a temporary copy of the rows before updating — so your update will always run only once per row.
