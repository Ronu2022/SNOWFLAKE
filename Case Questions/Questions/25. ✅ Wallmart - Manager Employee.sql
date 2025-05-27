
CREATE OR REPLACE TABLE Employee 
(
    id INT,
    name VARCHAR(50),
    department VARCHAR(50),
    managerId INT
);


INSERT INTO Employee (id, name, department, managerId) VALUES
(1, 'John', 'HR', NULL),
(2, 'Bob', 'HR', 1),
(3, 'Olivia', 'HR', 1),
(4, 'Emma', 'Finance', NULL),
(5, 'Sophia', 'HR', 1),
(6, 'Mason', 'Finance', 4),
(7, 'Ethan', 'HR', 1),
(8, 'Ava', 'HR', 1),
(9, 'Lucas', 'HR', 1),
(10, 'Isabella', 'Finance', 4),
(11, 'Harper', 'Finance', 4),
(12, 'Hemla', 'HR', 3),
(13, 'Aisha', 'HR', 2),
(14, 'Himani', 'HR', 2),
(15, 'Lily', 'HR', 2);

Select * from Employee;

// write a sql query to find the names of Managers who directly manage atleast 5 employees in the same department.
// Ensure that only employees from departments with more than 10 total employees are considered in your query




with main_cte as
       
(
    select 
    Emp.id,Emp.name,Emp.department as emp_dept, t2.department as manager_department,Emp.managerid,
    NVL(t2.name,'NA') as manager_name
    from Employee as Emp
    left join Employee as t2
    on  coalesce(Emp.Managerid,999) = coalesce(t2.id,999)
    where  NVL(t2.name,'NA') IS NOT NULL -- filtered out Those with No Managers -- because that's how we will get the details 
),
departemental_employee_count as
(
    select  department as emp_dept, count(distinct Name) as reportee
    from Employee
    group by department
    having  count(distinct Name) > 10 -- here total employee count was required hence took it fromn the raw table
)

select emp_dept, manager_name, count(distinct NAME) as reportee_count
from main_cte where emp_dept IN (select emp_dept from departemental_employee_count)
and  emp_dept = manager_department
group by emp_dept, manager_name
having ( count(distinct NAME) > 5);
