CREATE  OR REPLACE TABLE Scores (
    id INT PRIMARY KEY,
    score INT
);

INSERT INTO Scores (id, score) VALUES
(1, 100),
(2, 90),
(3, 90),
(4, 80),
(5, 70),
(6,100);


select * from scores;

/*
Write a solution to find the rank of the scores. The ranking should be calculated according to the following rules:

The scores should be ranked from the highest to the lowest.
If there is a tie between two scores, both should have the same ranking.
After a tie, the next ranking number should be the next consecutive integer value. In other words, there should be no holes between ranks.
*/




// Way 1 - Self join
select s1.id, s1.score,
(select 
count(distinct s2.score)from scores as s2 where s2.score >= s1.score) as dr
from
scores as s1
order by s1.score desc;

// Way2: Self Joun:

select s1.id,s1.score, j1.freq as dr
from scores as s1
left join
(
    select score, count(*) as freq
    from scores group by score
) as j1 on s1.score = j1.score
order by s1.score desc;
