USE library;


-- subquery to check active books:
SELECT sb_subscriber, sb_book 
FROM subscriptions
WHERE sb_is_active = 'Y';



-- insert row add dublicate book to 1 of the subscriber:
INSERT INTO subscriptions 
(sb_id, sb_subscriber, sb_book, sb_start, sb_finish, sb_is_active)
VALUES (101, 3, 1, '2016-01-12', '2018-01-12', 'Y');

-- delete additional row
DELETE FROM subscriptions 
WHERE sb_id = 101;



-- Задача 2.1.9.a{59}: показать, сколько в среднем 
-- экземпляров книг 
-- сейчас на руках у каждого читателя.


-- SELECT SUM(active_books) / COUNT(sb_subscriber) AS avg_books 
SELECT AVG(active_books) AS avg_books
FROM (SELECT sb_subscriber, 
	COUNT(sb_book) AS active_books 
FROM subscriptions
WHERE sb_is_active = 'Y'
GROUP BY sb_subscriber) AS act_count_dt;


-- Задача 2.1.9.b{59}: показать, сколько в среднем 
-- книг сейчас 
-- на руках у каждого читателя.


-- SELECT SUM(active_books) / COUNT(sb_subscriber) AS avg_books 
SELECT AVG(active_books) AS avg_books
FROM (SELECT sb_subscriber, 
	COUNT(DISTINCT sb_book) AS active_books 
FROM subscriptions
WHERE sb_is_active = 'Y'
GROUP BY sb_subscriber) AS act_count_dt;



-- Задача 2.1.9.c{60}: показать, на сколько в среднем дней читатели берут
-- книги (учесть только случаи, когда книги были возвращены).


select	AVG(DATEDIFF(sb_finish,  sb_start)) AS avg_term
from subscriptions
WHERE sb_is_active = "N";


select	*,
	AVG(DATEDIFF(sb_finish,  sb_start)) OVER() AS avg_term
from subscriptions
WHERE sb_is_active = "N";



-- Задача 2.1.9.d{60}: показать, сколько в среднем дней читатели читают книгу
-- (учесть оба случая — и когда книга была возвращена, и когда книга не
-- была возвращена).

-- Итого, у нас есть четыре варианта расчёта времени чтения книги:
-- sb_finish в прошлом, книга возвращена: sb_finish - sb_start.
-- sb_finish в будущем, книга не возвращена: sb_finish - sb_start.
-- sb_finish в прошлом, книга не возвращена: текущая_дата - sb_start.
-- sb_finish в будущем, книга возвращена: текущая_дата - sb_start.

-- Легко заметить, что алгоритмов вычисления всего два, но активируется каж-
-- дый из них двумя независимыми условиями. Самым простым способом решения
-- здесь является объединение результатов двух запросов с помощью оператора UN-
-- ION. Важно помнить, что UNION по умолчанию работает в DISTINCT-режиме, т.е.
-- нужно явно писать UNION ALL.


SELECT AVG(diff) FROM (
SELECT DATEDIFF(sb_finish, sb_start) AS "diff"
FROM subscriptions 
WHERE (sb_finish <= CURDATE() and sb_is_active = "N")
	OR (sb_finish > CURDATE() and sb_is_active = "Y")
UNION ALL
SELECT DATEDIFF(CURDATE() , sb_start) AS "diff"
FROM subscriptions 
WHERE (sb_finish > CURDATE() and sb_is_active = "N")
	OR (sb_finish <= CURDATE() and sb_is_active = "Y")
) AS date_diff_dt;


-- Difference between dates in full years:

SELECT YEAR('2012-04-01') - YEAR('2011-05-01') AS year_diff   -- compare only years
-- 1 but full year is not passed 


SELECT  (DATE_FORMAT('2012-04-01', '%m%d') <
DATE_FORMAT('2011-05-01', '%m%d'))
 AS corrections
 -- 1 check dif of month and day 


SELECT YEAR('2012-04-01') - YEAR('2011-05-01') 
	- (DATE_FORMAT('2012-04-01', '%m%d') <
		DATE_FORMAT('2011-05-01', '%m%d')
) AS diff_full_years;
-- calculate dif with correction only full years


SELECT DATEDIFF('2012-04-01', '2011-05-01');
-- 336 



-- Задание 2.1.9.TSK.A: показать, сколько в среднем экземпляров книг есть
-- в библиотеке.

select * from books;

select AVG(b_quantity) as total_quantity
FROM books;
-- 5.625 


-- Задание 2.1.9.TSK.B: показать в днях, сколько в среднем времени чита-
-- тели уже зарегистрированы в библиотеке (временем регистрации считать
-- диапазон от первой даты получения читателем книги до текущей даты).

SELECT AVG(DATEDIFF(CURDATE(), first_start)) AS avg_register
FROM (
SELECT sb_subscriber, 
MIN(sb_start) AS first_start
FROM  subscriptions
GROUP BY sb_subscriber) dt;
-- 4453.3333


SELECT sb_subscriber, 
MIN(sb_start) AS first_start,
DATEDIFF(CURDATE(), MIN(sb_start)) AS register_term,
AVG(DATEDIFF(CURDATE(), MIN(sb_start))) OVER() AS avg_register
FROM  subscriptions
GROUP BY sb_subscriber;


-- Задача 2.1.10.a{64}: показать по каждому году, 
-- сколько раз в этот год читатели брали книги.

select year(sb_start) as year, 
	count(sb_id) as books_taken
from subscriptions
Group by year
ORDER BY year;


-- Задача 2.1.10.b{65}: показать по каждому году, 
-- сколько читателей в год воспользовалось услугами библиотеки.

select year(sb_start) as year, 
	count(DISTINCT sb_subscriber) as books_taken
from subscriptions
Group by year     -- year(sb_start)
ORDER BY year;

-- MySQL позволяет в GROUP BY сослаться на имя только что вычисленного выражения,
-- в товремя как MS SQL Server и Oracle не позволяют этого сделать.


-- Задача 2.1.10.c{66}: показать, сколько книг было возвращено 
-- и не возвращено в библиотеку.


select CASE 
		WHEN sb_is_active = "Y" 
			THEN 'Not returned'
		ELSE "Returned"
	END AS status,
	count(sb_id) as books
from subscriptions
Group by sb_is_active
Order by sb_is_active;


SELECT IF(sb_is_active = "Y", "Not returned", "Returned") AS status,
	COUNT(sb_id) as books
from subscriptions
Group by sb_is_active
Order by sb_is_active;
	

-- Задание 2.1.10.TSK.A: переписать решение 2.1.10.c так, чтобы при под-
-- счёте возвращённых и невозвращённых книг СУБД оперировала исход-
-- ными значениями поля sb_is_active (т.е. Y и N), а преобразование в
-- «Returned» и «Not returned» происходило после подсчёта.


