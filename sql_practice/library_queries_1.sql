use library;

select
	b.b_id ,
	b.b_name,
	g.g_name
from
	books as b
inner join m2m_books_genres mmbg
on
	b.b_id = mmbg.b_id
INNER join genres g 
on
	mmbg.g_id = g.g_id;

select
	*
from
	authors;

select
	*
from
	genres;

select
	DISTINCT sb_subscriber
FROM
	subscriptions;

select
	s_name,
	count(*) as people_count
from
	subscribers
group by
	s_name;

select
	DISTINCT sb_book
FROM
	subscriptions;

select
	sb_book,
	count(*) given_number
FROM
	subscriptions
Group by
	sb_book ;

select
	count(*) as total_books
from
	books;

select
	count(sb_book) as in_use
from
	subscriptions
where
	sb_is_active = 'Y';

select
	count(DISTINCT sb_book) as in_use
from
	subscriptions
where
	sb_is_active = 'Y';

select
	*
from
	subscriptions;
-- 2.1.4.TSK.A
select
	count(sb_id) as total_sb
from
	subscriptions;

select
	count(DISTINCT sb_subscriber) as total_readers
from
	subscriptions;
# 2.1.5.a
select
	sum(b_quantity) as sum, 
	min(b_quantity) as min,
	max(b_quantity) as max,
	AVG(b_quantity) as avg
	-- AVG(CAST(b_quantity AS FLOAT)) as avg  MS SQL Server выбирает тип данных результата на основе типа данных входного параметра
FROM
	books;
-- Задание 2.1.5.TSK.A: показать первую и последнюю даты выдачи книги читателю.
SELECT
	MIN(sb_start) first,
	MAX(sb_start) last
FROM
	subscriptions;
-- 2.1.6.a
select
	b_name,
	b_year
from
	books
order by
	b_year;
-- 2.1.6.b
select
	b_name,
	b_year
from
	books
order by
	b_year DESC;
-- to have null first
SELECT
	b_name,
	b_year
FROM
	books
ORDER BY
	b_year IS NULL DESC,
	b_year ASC;
-- 2.1.7.a
-- показать книги, изданные в период 1990-2000 годов,
-- представленные в библиотеке в количестве трёх и более экземпляров
select
	b_name,
	b_year,
	b_quantity
from
	books
WHERE
	b_year between 1990 and 2000
	AND b_quantity >= 3;
-- Вариант 2: использование двойного неравенства
SELECT
	`b_name`,
	`b_year`,
	`b_quantity`
FROM
	books
WHERE
	`b_year` >= 1990
	AND `b_year` <= 2000
	AND `b_quantity` >= 3;
-- 2.1.7.b показать идентификаторы и даты выдачи книг за лето 2012-го года
SELECT
	sb_id,
	sb_start
from
	subscriptions
WHERE
	sb_start >= '2012-06-01'
	and sb_start < '2012-09-01';
-- неправильный с точки зрения производительности вариант
SELECT
	sb_id,
	sb_start
from
	subscriptions
WHERE
	YEAR(sb_start) = 2012
	and MONTH(sb_start) BETWEEN 6 and 8;

-- 2.1.7.TSK.A: показать книги, количество экземпляров которых
-- меньше среднего по библиотеке.
select
	b_name,
	b_quantity
from
	books
WHERE
	b_quantity < (
	SELECT
		AVG(b_quantity)
	FROM
		books
);
	-- added average column
SELECT
	b_name,
	b_quantity,
	avg
FROM
	(
	SELECT 
		b_name, 
		b_quantity, 
		AVG(b_quantity) OVER() as avg
	FROM
		books 
) derived_table
WHERE
	b_quantity < avg;


-- 2.1.7.TSK.B 
-- показать идентификаторы и даты выдачи книг за
-- первый год работы библиотеки

SELECT
	sb_book,
	sb_start
FROM
	subscriptions
WHERE
	YEAR(sb_start) = (
	SELECT
		Year(MIN(sb_start))
	FROM
		subscriptions);


