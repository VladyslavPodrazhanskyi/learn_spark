USE library;


-- window functions query:

SELECT
    b_name,
    b_quantity,
    LEAD(b_quantity) OVER(ORDER BY b_quantity DESC) as ld,
    LAG(b_quantity) OVER(ORDER BY b_quantity DESC) as lg,
    CAST(b_quantity AS SIGNED)- CAST(COALESCE(LAG(b_quantity) OVER(ORDER BY b_quantity DESC), 0) AS SIGNED) as dif,
    ROW_NUMBER() OVER(ORDER BY b_quantity DESC) as rn,
    RANK() OVER(ORDER BY b_quantity DESC) as rnk,
    DENSE_RANK() OVER(ORDER BY b_quantity DESC) as d_rnk
FROM books;

-- Notice that with UNSIGNED, you're essentially giving yourself twice as much space for the integer 
-- since you explicitly specify you don't need negative numbers.



-- Задача 2.1.8.a{нет}: показать книгу, представленную в библиотеке 
-- максимальным количеством экземпляров


SELECT	*
from books;

DESCRIBE books;

select
	b_name,
	b_quantity
from books
where b_quantity = (
		select max(b_quantity)
		from books
	);

-- В самой формулировке этой задачи скрывается ловушка: в такой формулиовке задача 2.1.8.a 
-- не имеет правильного решения (потому оно и не будет показано). На самом деле, здесь не одна задача, а три.
-- 1) Задача 2.1.8.b{46}: показать просто одну любую книгу, количество экземпляров которой максимально (равно максимуму по всем книгам).
-- 2) Задача 2.1.8.c{48}: показать все книги, количество экземпляров которых максимально (и одинаково для всех этих показанных книг).
-- 3) Задача 2.1.8.d{50}: показать книгу (если такая есть), количество экземпляров которой больше, чем у любой другой книги.

-- add a new book with maximum b_quantity to receve dif results for all 3 above mentioned subtasks
INSERT	INTO books (
	b_id,
	b_name,
	b_year,
	b_quantity
)
VALUES (NULL, 'new_book', 2024, 12);

DELETE FROM books 
WHERE b_id = 8;


-- Solutions:
-- 1) Задача 2.1.8.b{46}: показать просто одну любую книгу, количество экземпляров которой максимально (равно максимуму по всем книгам).


SELECT
	b_name,
	b_quantity
FROM books
ORDER BY b_quantity DESC
LIMIT 1;


-- ORACLE
select
	b_name, 
	b_quantity
FROM (
	SELECT
		b_name,
		b_quantity,
		ROW_NUMBER() OVER(ORDER BY b_quantity DESC) as rn
	FROM books) ranked_derived_table
WHERE rn = 1;



-- MS SQL, ORACLE


-- SELECT b_name, b_quantity 
-- from books 
-- order by b_quantity DESC 
-- OFFSET 0 ROWS
-- FETCH NEXT 1 ROWS ONLY; 
-- MS SQL 
-- SELECT TOP 1
-- b_name, b_quantity
-- from books b
-- order by b_quantity DESC;




-- 2) Задача 2.1. 8.c{48}: показать все книги, количество экземпляров которых максимально (и одинаково для всех этих показанных книг).

-- 2.1. subquery
SELECT
	b_name,
	b_quantity
FROM books
WHERE	b_quantity = (
	SELECT	MAX(b_quantity)
	FROM books
);

-- 2.2. save max quantity in the variable use it in the query:
SELECT MAX(b_quantity) 
INTO @max_quantity
FROM books;

SELECT
	b_name,
	b_quantity
FROM books
WHERE b_quantity = @max_quantity;


-- 2.3. using rank() window function:

select
	b_name, 
	b_quantity
FROM ( SELECT
		b_name,
		b_quantity,
		RANK() OVER(ORDER BY b_quantity DESC) AS rn
	  FROM books) ranked_derived_table
WHERE rn = 1;




-- 3) Задача 2.1.8.d{50}: показать книгу (если такая есть), количество экземпляров которой больше, чем у любой другой книги.
-- 3.1. using variables

SELECT max(b_quantity)
INTO @max_quantity
FROM books;

select count(*)
INTO @max_count
FROM books
where b_quantity = @max_quantity;

SELECT
	b_name,
	b_quantity
FROM books
WHERE
	b_quantity = @max_quantity
	and @max_count = 1;


-- 3.2. 
-- create rank CTE 
-- creater competitors CTE where for all rank calculated competitors count
-- select book name, quantity where rank = 1 and number of competitors = 1



with rank_cte AS (
SELECT b_name, 
	b_quantity, 
	RANK() OVER(ORDER BY b_quantity DESC) AS rnk
from books
),
competitors_cte as (
SELECT rnk, 
	count(*) as competitors
from rank_cte
GROUP BY rnk
)
SELECT rcte.b_name,
	rcte.b_quantity	
FROM rank_cte as rcte
INNER JOIN competitors_cte as ccte
ON rcte.rnk = ccte.rnk              -- USING(rnk) 
WHERE rcte.rnk = 1 
AND competitors = 1;


-- 3.3. Using ALL  ( using correlated subquery): 

-- The ANY and ALL operators allow you to perform a comparison 
-- between a single column value and a range of other values.
--  returns a boolean value as a result
-- ANY means that the condition will be true if the operation is true for any of the values in the range.
-- ALL means that the condition will be true only if the operation is true for all values in the range. 

select * from books;

SELECT `b_name`,
`b_quantity`
FROM `books` AS `ext`
WHERE `b_quantity` > ALL (SELECT `b_quantity`
						FROM `books` AS `int`
						WHERE `ext`.`b_id` != `int`.`b_id`);


-- Это пример коррелирующего подзапроса:					
-- Коррелирующий подзапрос — это подзапрос, который содержит ссылку на столбцы 
-- из включающего его запроса (назовем его основным). 
-- Таким образом, коррелирующий подзапрос будет выполняться для каждой строки основного запроса, 
-- так как значения столбцов основного запроса будут меняться.
					

-- Chapter 8 task:					
-- для каждого читателя нам нужно показать ровно одну 
-- (любую, если их может быть несколько, но — одну) 
-- запись из таблицы subscriptions, 
-- соответствующую первому визиту читателя в библиотеку.
					
SELECT * FROM subscriptions; 


-- 8.1. Ranking solution ( решение на основе ранжирования)

-- a) my solution:
WITH rang_func_CTE AS (
	SELECT *, 
	ROW_NUMBER() over(PARTITION BY sb_subscriber ORDER by sb_start) as row_num	
FROM subscriptions) 
SELECT `sb_id`,
	`sb_subscriber`,
	`sb_book`,
	`sb_start`,
	`sb_finish`,
	`sb_is_active`
FROM rang_func_CTE
WHERE row_num = 1
					
-- b) books solution:
SELECT `subscriptions`.`sb_id`,
`sb_subscriber`,
`sb_book`,
`sb_start`,
`sb_finish`,
`sb_is_active`
FROM
`subscriptions`
JOIN (SELECT `sb_id`,
ROW_NUMBER()
OVER (
PARTITION BY `sb_subscriber`
ORDER BY `sb_start` ASC) AS `visit`
FROM
`subscriptions`) AS `prepared`
ON `subscriptions`.`sb_id` = `prepared`.`sb_id`
WHERE `visit` = 1

-- c) emulation of ROW_NUMBER function in old mysql versions:

SELECT subscriptions.sb_id,
	sb_subscriber,
	sb_book,
	sb_start,
	sb_finish,
	sb_is_active
FROM
subscriptions
JOIN (SELECT sb_id,
	@row_num = IF(
		@prev_val = sb_subscriber, 
		@row_num + 1, 
		1) AS visit, 
	@prev_val := sb_subscriber
	FROM subscriptions,
		(SELECT @row_num := 1) as x,      -- start var values   (SELECT @row_num := 1)                        
		(SELECT @prev_val := '') as y
	ORDER BY 
		sb_subscriber ASC, 
		sb_start ASC) AS prepared
ON subscriptions.sb_id = prepared.sb_id
WHERE visit = 1



-- 8.2. Agregation solution ( решение на основе агрегирующих функций)


with subscriber_minstart_cte AS (
select 	
	sb_subscriber, 
	min(sb_start) AS min_start
FROM subscriptions
Group BY sb_subscriber),

min_id_cte AS (Select min(sb_id)	
from subscriptions as sb
join subscriber_minstart_cte as cte
ON sb.sb_subscriber = cte.sb_subscriber
and sb.sb_start = cte.min_start
group by sb.sb_subscriber)

SELECT * FROM subscriptions
WHERE sb_id IN (SELECT * from min_id_cte);

-- 8.3. Correlated solution:
SELECT * 
from subscriptions AS ext
where sb_id = (
	select sb_id 
	From subscriptions AS inr
	WHERE ext.sb_subscriber = inr.sb_subscriber	
	ORDER by sb_start 
	LIMIT 1
)


SELECT `sb_id`,
`sb_subscriber`,
`sb_book`,
`sb_start`,
`sb_finish`,
`sb_is_active`
FROM
`subscriptions` AS `outer`
WHERE `sb_id` = (SELECT `sb_id`
FROM
`subscriptions` AS `inner`
WHERE `outer`.`sb_subscriber` = `inner`.`sb_subscriber`
ORDER BY `sb_start` ASC
LIMIT 1)



-- Задание 2.1.8.TSK.A: показать идентификатор одного (любого) читателя,
-- взявшего в библиотеке больше всего книг.


--  limit solution
SELECT sb_subscriber, 
	COUNT(sb_book) AS total_book_count 
FROM subscriptions
GROUP BY sb_subscriber
ORDER BY total_book_count DESC 
LIMIT 1;


-- row_number solution
WITH book_count_cte AS (
	SELECT sb_subscriber, 
		COUNT(sb_book) AS total_book_count 
	FROM subscriptions
	GROUP BY sb_subscriber),
row_num_cte AS (
	SELECT *,
		ROW_NUMBER() OVER(			
			ORDER BY total_book_count DESC) AS rnm	
	FROM book_count_cte
)
SELECT sb_subscriber,
	total_book_count
FROM row_num_cte
WHERE rnm = 1;


-- Задание 2.1.8.TSK.B: показать идентификаторы всех «самых читающих
-- читателей», взявших в библиотеке больше всего книг.




-- aggreation solution
WITH book_count_cte AS 
(SELECT sb_subscriber, 
	COUNT(sb_book) AS total_book_count 
FROM subscriptions
GROUP BY sb_subscriber
)
SELECT * FROM book_count_cte
WHERE total_book_count = (
	SELECT MAX(total_book_count)
	FROM book_count_cte
);


-- ranking solution
WITH book_count_cte AS (
	SELECT sb_subscriber, 
		COUNT(sb_book) AS total_book_count 
	FROM subscriptions
	GROUP BY sb_subscriber),
row_num_cte AS (
	SELECT *,
		RANK() OVER(			
			ORDER BY total_book_count DESC) AS rnk	
	FROM book_count_cte
)
SELECT sb_subscriber,
	total_book_count
FROM row_num_cte
WHERE rnk = 1;


-- Задание 2.1.8.TSK.C: показать идентификатор «читателя-рекордсмена»,
-- взявшего в библиотеке больше книг, чем любой другой читатель.


-- correlated subquery with ALL

WITH book_count_CTE AS (
	SELECT sb_subscriber, 
		COUNT(sb_book) AS total_book_count 
	FROM subscriptions
	GROUP BY sb_subscriber
)
SELECT * FROM book_count_CTE as ext
WHERE total_book_count > ALL(
	SELECT total_book_count
 	FROM book_count_CTE as intern
 	WHERE ext.sb_subscriber != intern.sb_subscriber
 );


-- ranking solution 
-- 1) create CTE with 2 columns  sb_subscriber, total_book_count 
-- 2) add additional column rank 
-- 3) add additional column rnk_count -  quantity of subscriber with the same rank ( with join)
-- 4) filter row with maximum rank and quantity of subscribers  = 1 

WITH book_count_CTE AS (
	SELECT sb_subscriber, 
		COUNT(sb_book) AS total_book_count 
	FROM subscriptions
	GROUP BY sb_subscriber), 
ranked_CTE AS (
	SELECT *, 
		RANK() OVER(ORDER BY total_book_count CTE) AS rnk
),
rank_count_CTE AS (
	SELECT rnk, 
		COUNT(sb_subscriber) rnk_count
	GROUP BY rnk
)

SELECT * FROM 






-- Задание 2.1.8.TSK.D: написать второй вариант решения задачи 2.1.8.d
-- (основанный на общем табличном выражении) для MySQL, проэмулиро-
-- вав общее табличное выражение через подзапросы.





