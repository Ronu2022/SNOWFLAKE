
CREATE OR REPLACE TABLE Sectors 
(
    company_id INT,
    sector VARCHAR(50)
);

INSERT INTO Sectors (company_id, sector) VALUES
(1, 'Technology'),
(2, 'Healthcare'),
(3, 'Technology');

CREATE OR REPLACE TABLE Transactions (
    transaction_id INT,
    company_id INT,
    transaction_date DATE,
    revenue INT
);

INSERT INTO Transactions (transaction_id, company_id, transaction_date, revenue) VALUES
(101, 1, '2020-01-15', 5000),
(102, 2, '2020-01-20', 8500),
(103, 1, '2020-02-10', 4500),
(104, 3, '2020-02-20', 9900),
(105, 2, '2020-02-25', 7500);


Select * from Transactions;


// Writre as sql Query that calculates the average monthly revenue  for ecah sector in the year 2020


select 
    t.transaction_id,
    t.company_id,
    s.sector,
    t.transaction_date,
    t.revenue
from transactions as t
left join sectors as s on t.company_id = s.company_id;



select year(date_trunc('year',t.transaction_date)) as transaction_year,
        s.sector,
        avg(t.revenue) as avg_revenue
from  transactions as t
left join sectors as s on t.company_id = s.company_id
GROUP BY year(date_trunc('year',t.transaction_date)),s.sector
HAVING year(date_trunc('year',t.transaction_date)) = '2020';
