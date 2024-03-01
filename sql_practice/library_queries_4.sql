-- Задача 2.2.1.a{70}: показать всю человекочитаемую информацию обо всех
-- книгах (т.е. название, автора, жанр).

SelECT books.b_name,
	authors.a_name, 
	genres.g_name 
FROM books
JOIN m2m_books_genres mmbg
ON books.b_id = mmbg.b_id 
JOIN genres
ON mmbg.g_id = genres.g_id 
Join m2m_books_authors mmba 
ON books.b_id = mmba.b_id 
JOIN authors
ON mmba.a_id = authors.a_id;



-- Ключевое отличие решений для MySQL и Oracle от решения для MS SQL
-- Server заключается в том, что эти две СУБД поддерживают специальный синтаксис
-- указания полей, по которым необходимо производить объединение: если такие
-- поля имеют одинаковое имя в объединяемых таблицах, вместо конструкции ON
-- первая_таблица.поле = вторая_таблица.поле 
-- можно использовать USING(поле), что зачастую очень сильно повышает читаемость запроса.



SELECT `b_name`,
`a_name`,
`g_name`
FROM
`books`
JOIN `m2m_books_authors` USING(`b_id`)
JOIN `authors` USING(`a_id`)
JOIN `m2m_books_genres` USING(`b_id`)
JOIN `genres` USING(`g_id`)


-- Задача 2.2.1.b{72}: показать всю человекочитаемую информацию обо всех
-- обращениях в библиотеку (т.е. имя читателя, название взятой книги).


SELECT b_name,
	s_id,
	s_name,
	sb_start,
	sb_finish
FROM subscriptions
JOIN books
ON sb_book = b_id 
JOIN subscribers
ON sb_subscriber = s_id 



-- Задание 2.2.1.TSK.A: показать список книг, у которых более одного автора.


select b_id, b_name,
	count(a_id) AS authors_number
FROM m2m_books_authors mmba 
JOIN authors USING (a_id)
JOIN books 
USING (b_id)
GROUP BY b_id, b_name
HAVING authors_number > 1;



-- Задание 2.2.1.TSK.B: показать список книг, относящихся ровно к одному жанру.


select books.b_id, 
	b_name,	
	count(mmbg.g_id) AS genres_number
FROM books
JOIN m2m_books_genres mmbg
ON  books.b_id = mmbg.b_id 
GROUP BY b_id, b_name
HAVING genres_number = 1;



-- Запросы на объединение и преобразование столбцов в строки.


-- Задача 2.2.2.a{75}: показать все книги с их авторами 
-- (дублирование названий книг не допускается).

select b_name,	 
	GROUP_CONCAT(a_name ORDER BY a_name SEPARATOR ', ') AS authors		
FROM m2m_books_authors
JOIN authors USING (a_id)
JOIN books USING (b_id)
Group by b_id                         -- ATTENTION use PRIMARY KEY for GROUP BY, not book name !!!!
ORDER BY b_name;


-- И ещё одна особенность MySQL заслуживает внимания: в строке 1 мы извлекаем поле b_name, 
-- которое не упомянуто в выражении GROUP BY в строке 8 и не является агрегирующей функцией. 
-- MS SQL Server и Oracle не позволяют поступать подобным образом и строго требуют, 
-- чтобы любое поле из конструкции SELECT, не являющееся агрегирующей функций, 
-- было явно упомянуто в выражении GROUP BY.


-- Задача 2.2.2.b{79}: показать все книги с их авторами и жанрами (дублиро-
-- вание названий книг и имён авторов не допускается).


select b_name,	 
	GROUP_CONCAT(DISTINCT a_name ORDER BY a_name SEPARATOR ', ') AS authors,
	GROUP_CONCAT(DISTINCT g_name ORDER BY g_name SEPARATOR ', ') AS genres	
FROM m2m_books_authors mmba 
JOIN authors USING (a_id)
JOIN books
USING (b_id)
JOIN m2m_books_genres mmbg 
USING (b_id)
JOIN genres 
USING(g_id)
Group by b_id                         -- ATTENTION use PRIMARY KEY for GROUP BY, not book name !!!! 
ORDER BY b_name;




-- Задание 2.2.2.TSK.A: показать все книги с их жанрами (дублирование
-- названий книг не допускается).


SELECT b_name, 
	GROUP_CONCAT(g_name ORDER BY g_name SEPARATOR ', ') AS genres
FROM books
JOIN m2m_books_genres AS mmg
ON books.b_id = mmg.b_id
JOIN genres
ON mmg.g_id = genres.g_id
GROUP BY books.b_id       
       


-- Задание 2.2.2.TSK.B: показать всех авторов со всеми написанными ими
-- книгами и всеми жанрами, в которых они работали (дублирование имён
-- авторов, названий книг и жанров не допускается).


SELECT authors.a_id,
	GROUP_CONCAT(DISTINCT books.b_name ORDER BY books.b_name SEPARATOR ', ') AS books,
	GROUP_CONCAT(DISTINCT g_name ORDER BY g_name SEPARATOR ', ') AS genres
FROM authors
JOIN m2m_books_authors mmba 
ON authors.a_id = mmba.a_id
JOIN books 
ON books.b_id = mmba.b_id
JOIN m2m_books_genres mmbg 
ON books.b_id = mmbg.b_id
JOIN genres
ON mmbg.g_id = genres.g_id
GROUP by authors.a_id ;


-- запросы на объединение и подзапросы с условием IN

-- Очень часто запросы на объединение можно преобразовать в запросы с подзапросом 
-- и ключевым словом IN (обратное преобразование тоже возможно). 
-- Рас смотрим несколько типичных примеров:

-- Задача 2.2.3.a{87}: показать список читателей, 
-- когда-либо бравших в библиотеке книги (использовать JOIN).

SELECT DISTINCT readers.s_id, 
	s_name
FROM subscribers AS readers
INNER JOIN subscriptions AS sbs
ON readers.s_id = sbs.sb_subscriber 


-- Задача 2.2.3.b{88}: показать список читателей, 
-- когда-либо бравших в библиотеке книги (не использовать JOIN).


SELECT s_id, 
	s_name
FROM subscribers
WHERE s_id in (
	SELECT DISTINCT sb_subscriber
	FROM subscriptions	
)

-- IN работает быстрее JOIN, 
-- в остальных случаях стоит проводить дополнительные исследования.



-- Задача 2.2.3.c{90}: показать список читателей, 
-- никогда не бравших в библиотеке книги (использовать JOIN).


SELECT readers.s_id, readers.s_name 
FROM subscribers AS readers
LEFT JOIN subscriptions AS sbs
ON readers.s_id = sbs.sb_subscriber
WHERE sbs.sb_subscriber IS NULL;


-- Задача 2.2.3.d{91}: показать список читателей, 
-- никогда не бравших в библиотеке книги (не использовать JOIN).


SELECT s_id, 
	s_name
FROM subscribers
WHERE s_id NOT IN (
	SELECT DISTINCT sb_subscriber
	FROM subscriptions);



-- Задание 2.2.3.TSK.A: показать список книг, которые когда-либо были
-- взяты читателями.


select b_id, 
	b_name
FROM books 
WHERE b_id IN (
	SELECT DISTINCT sb_book 
	FROM subscriptions 
);

select DISTINCT b_id, 
	b_name
FROM books 
JOIN subscriptions 
ON b_id = sb_book


-- Задание 2.2.3.TSK.B: показать список книг, которые никто из читателей
-- никогда не брал.


select b_id, 
	b_name
FROM books 
WHERE b_id NOT IN (
	SELECT DISTINCT sb_book 
	FROM subscriptions 
);


select DISTINCT b_id, 
	b_name
FROM books 
LEFT JOIN subscriptions 
ON b_id = sb_book
WHERE sb_book is NULL


-- Нетривиальные случаи использования 
-- условия IN и запросов на объединение


-- Существуют задачи, которые на первый взгляд решаются очень просто. 
-- Однако оказывается, что простое и очевидное решение является неверным.


-- Задача 2.2.4.a{96}: показать список читателей, у которых сейчас на руках
-- нет книг

-- мои неудачные попытки отбор только тех читатетелй, 
-- у которых sb_is_active = "N" во всех колонках, 
-- без учета читателей, которых вообще нет в таблице subscriptions



SELECT DISTINCT sb_subscriber
FROM subscriptions AS external
WHERE sb_is_active = 'N'
  AND NOT EXISTS (
    SELECT 1
    FROM subscriptions AS internal
    WHERE external.sb_subscriber = internal.sb_subscriber
      AND internal.sb_is_active = 'Y'
);


SELECT sb_subscriber
FROM subscriptions
GROUP BY sb_subscriber
HAVING SUM(CASE WHEN sb_is_active = 'Y' THEN 1 ELSE 0 END) = 0;


-- all 

SELECT DISTINCT sb_subscriber
FROM subscriptions t1
WHERE sb_is_active = 'N'
  AND 'N' = ALL (
    SELECT sb_is_active
    FROM subscriptions t2
    WHERE t1.sb_subscriber = t2.sb_subscriber
);


SELECT DISTINCT sb_subscriber
FROM subscriptions t1
WHERE 'N' = ALL (
    SELECT sb_is_active
    FROM subscriptions t2
    WHERE t1.sb_subscriber = t2.sb_subscriber    	 
);




-- Задача 2.2.4.a{96}: показать список читателей, у которых сейчас на руках
-- нет книг (использовать JOIN).


-- Признаком того, что читатель вернул все книги, является отсутствие
-- (COUNT(…) = 0) у него записей со значением sb_is_active = 'Y'. Проблема в
-- том, что мы не можем «заставить» COUNT считать только значения Y — он будет
-- учитывать любые значения, не равные NULL. Отсюда легко следует вывод, что нам
-- осталось превратить в NULL любые значения поля sb_is_active, не равные Y



select s_id, 
	s_name	
FROM subscribers 
	LEFT OUTER JOIN subscriptions
	ON s_id = sb_subscriber
GROUP BY s_id, s_name                                                                     
HAVING COUNT(IF(sb_is_active = "Y", sb_is_active, NULL)) = 0;   # null is not counted in COUNT !!! 
#  HAVING SUM(IF(sb_is_active = "Y", 1, 0)) = 0

select s_id, 
	s_name	
FROM subscribers 
	LEFT OUTER JOIN subscriptions
	ON s_id = sb_subscriber
GROUP BY s_id, s_name  
HAVING SUM(CASE
				WHEN sb_is_active = 'Y' THEN 1
				ELSE 0
			END) = 0
-- HAVING COUNT(CASE
-- 				WHEN sb_is_active = 'Y' THEN sb_is_active
-- 				ELSE NULL
-- 			END) = 0

			

-- WRONG SOLUTION:

			
SELECT `s_id`,
`s_name`
FROM
`subscribers`
LEFT OUTER JOIN `subscriptions`
ON `s_id` = `sb_subscriber`
WHERE `sb_is_active` = 'N'
OR `sb_is_active` IS NULL
GROUP BY `s_id`
HAVING COUNT(`sb_is_active`) = 0	
	
-- Это решение учитывает никогда не бравших книги читателей
-- (sb_is_active IS NULL), 
-- а также тех, у кого есть на руках хотя бы одна книга была здана (sb_is_active = 'N'), 
-- а не обязательно все книги зданы. 
	

-- Задача 2.2.4.b{99}: показать список читателей, у которых сейчас на руках
-- нет книг (не использовать JOIN).

SELECT s_id, 
	s_name
FROM subscribers 
WHERE s_id NOT IN ( SELECT DISTINCT sb_subscriber 
					FROM subscriptions 
					WHERE sb_is_active = "Y")

-- WRONG SOLUTION:
-- Но это — решение задачи «показать читателей, которые хотя бы раз вернуликнигу». 
-- Данная проблема (характерная для многих подобных ситуаций) 
-- лежит не -- столько в области правильности составления запросов, 
-- сколько в области понимания смысла (семантики) модели базы данных.

SELECT s_id, 
	s_name
FROM subscribers 
WHERE s_id  IN ( SELECT DISTINCT sb_subscriber 
					FROM subscriptions 
					WHERE sb_is_active = "N");
					
/*

Если по некоторому факту выдачи книги отмечено, что она возвращена (в
поле sb_is_active стоит значение N), это совершенно не означает, что рядом нет
другого факта выдачи тому же читателю книги, которую он ещё не вернул.
Также очевидно, что читатели, никогда не бравшие книг, не попадут в список
людей, у которых на руках нет книг (идентификаторы таких читателей вообще ни
разу не встречаются в таблице subscriptions).

*/


-- Задание 2.2.4.TSK.A: показать список книг, ни один экземпляр которых
-- сейчас не находится на руках у читателей.


SELECT b_id,
	b_name
from books
WHERE b_id NOT IN (
					SELECT sb_book
					FROM subscriptions
					WHERE sb_is_active = 'Y');


SELECT DISTINCT b_id,
	b_name
FROM books
	LEFT OUTER JOIN subscriptions
	ON b_id=sb_book
GROUP BY b_id, b_name
HAVING COUNT(IF(sb_is_active = "Y", sb_is_active, NULL)) = 0;
-- HAVING SUM(IF(sb_is_active = "Y", 1, 0)) = 0;


SELECT DISTINCT b_id,
	b_name
FROM books
	LEFT OUTER JOIN subscriptions
	ON b_id=sb_book
GROUP BY b_id, b_name
HAVING COUNT(IF(sb_is_active = "Y", sb_is_active, NULL)) = 0;
-- HAVING SUM(IF(sb_is_active = "Y", 1, 0)) = 0;
-- HAVING COUNT(
-- 	CASE
-- 		WHEN sb_is_active = "Y" THEN sb_is_active
-- 		ELSE Null
-- 	END) = 0;
-- HAVING SUM(
-- 	CASE
-- 		WHEN sb_is_active = "Y" THEN 1
-- 		ELSE 0
-- 	END) = 0;


SELECT * FROM subscriptions s



-- 2.2.5. Двойное использование условия IN  ( вместо джойнов можно использовать IN для связывания таблиц, если потом не нужно делать агрегации):

-- Задача 2.2.5.a{102}: показать книги из жанров «Программирование» и/или
-- «Классика» (без использования JOIN; идентификаторы жанров известны).


select distinct books.b_id, b_name
from books
WHERE b_id IN (
			SELECT DISTINCT  b_id
			from m2m_books_genres
			WHERE g_id in (2, 5))
ORDER BY b_name;


-- Задача 2.2.5.b{103}: показать книги из жанров «Программирование» и/или
-- «Классика» (без использования JOIN; идентификаторы жанров неизвестны).



select distinct books.b_id, b_name
from books
WHERE b_id IN (
			SELECT DISTINCT b_id
			from m2m_books_genres
			WHERE g_id IN (
						SELECT g_id FROM genres
						WHERE g_name IN ("Classic", "Programming")));



-- Задача 2.2.5.c{104}: показать книги из жанров «Программирование» и/или
-- «Классика» (с использованием JOIN; идентификаторы жанров известны).

select distinct books.b_id, b_name
from books
	join m2m_books_genres mmbg
	ON books.b_id = mmbg.b_id
WHERE g_id in (2, 5);


-- Задача 2.2.5.d{105}: показать книги из жанров «Программирование» и/или
-- «Классика» (с использованием JOIN; идентификаторы жанров неизвестны).

SELECT distinct books.b_id, b_name
FROM books
	JOIN m2m_books_genres mmbg
	ON books.b_id = mmbg.b_id
	JOIN genres
	ON mmbg.g_id = genres.g_id
WHERE g_name IN ("Classic", "Programming");


-- Вариант такого задания, где вместо «и/или» стоит строгое «и», рассмотрен в
-- примере 18{135}.



-- Задание 2.2.5.TSK.A: показать книги, написанные Пушкиным и/или Азимовым (индивидуально или в соавторстве — не важно).



-- 3 IN without JOINS
SELECT DISTINCT b.b_id, b_name
FROM books b
WHERE b.b_id IN (SELECT DISTINCT mmba.b_id
				FROM m2m_books_authors mmba
				WHERE mmba.a_id IN (SELECT DISTINCT authors.a_id
									FROM authors
								    WHERE a_name IN ("Alexander Pushkin", "Isaac Asimov")));


-- 2 IN with 1 JOIN
SELECT DISTINCT b.b_id, b_name
FROM books AS b
	JOIN m2m_books_authors mmba
	ON b.b_id =  mmba.b_id
WHERE mmba.a_id IN (SELECT DISTINCT authors.a_id
					FROM authors
					WHERE a_name IN ("Alexander Pushkin", "Isaac Asimov"));


-- 1 IN with 2 JOINS

SELECT DISTINCT b.b_id, b_name
FROM books AS b
	JOIN m2m_books_authors mmba
	ON b.b_id =  mmba.b_id
	JOIN authors a
	ON mmba.a_id = a.a_id
WHERE a_name IN ("Alexander Pushkin", "Isaac Asimov");



-- Задание 2.2.5.TSK.B: показать книги, написанные Карнеги и Страуструпом в соавторстве.

-- 1 IN with 2 JOINS (так как потом нужны агрегации)

SELECT DISTINCT b.b_id, b_name
FROM books AS b
	JOIN m2m_books_authors mmba
	ON b.b_id =  mmba.b_id
	JOIN authors a
	ON mmba.a_id = a.a_id
GROUP BY b_id, b_name
HAVING SUM(IF(a.a_name IN ("Dale Carnegie", "Bjarne Stroustrup"), 1, 0)) =  2;
-- HAVING COUNT(IF(a.a_name IN ("Dale Carnegie", "Bjarne Stroustrup"), 1, NULL)) =  2;




-- 2.2.6. Пример 16: запросы на объединение и функция COUNT

-- Задача 2.2.6.a{106}: показать книги, у которых более одного автора.

select books.b_id,
	b_name,
	count(a_id) AS authors_count
from books
join m2m_books_authors mmba
on books.b_id = mmba.b_id
group by books.b_id, b_name
having count(a_id) > 1;
-- having authors_count > 1;


-- Задача 2.2.6.b{107}: показать, сколько реально экземпляров каждой книги
-- сейчас есть в библиотеке.


/*
 MY Solution:

 1. Create  given_count_CTE with b_id and given_count columns
 where given_count - quantity of active subscriptions for current b_id.

 2. Left join between books and given_count_CTE

 3. SELECT from joined table: b_id, b_name
 and calculate real_count as case:

 if given_quantity is NULL then real_count = b_quantity
 (case when books was not given at all, or all copies were returned).
 else: real_count =  b_quantity - given_count

  */

WITH given_count_CTE AS (
select sb_book, count(sb_id) as given_count
from subscriptions
where sb_is_active = "Y"
group by sb_book)
SELECT b_id, b_name,
	CASE
		WHEN given_count IS NULL THEN b_quantity
		ELSE b_quantity - given_count
	END
	AS real_count
FROM books
	LEFT OUTER JOIN given_count_CTE AS given
	ON given.sb_book = books.b_id
ORDER BY real_count DESC;


/*
 Solution with correlated subquery:

 1. Left join books and subscriptions AS external table for correlated subquery

 2. Select DISTINCT b_id, b_name
 and real_count using correlated subquery:

 real_count  = b_quantity - (correlated_subquery)

 3) correlated_subquery( for every ext.sb_book subquery returns dif result_:

 3.1. from subscription table
      where sb_is_active = "Y"
      and int.sb_book = ext.sb_book              ??? ext.b_id


 3.2. select count(int.sb_book)

 */

SELECT DISTINCT b_id
, b_name
, (b_quantity - (
	SELECT COUNT(intern.sb_book)          # count don't consider Null so count never is NULL
	FROM subscriptions AS intern
    WHERE intern.sb_book = ext.sb_book
    	AND intern.sb_is_active = "Y"
)) AS real_count
from books
	LEFT OUTER JOIN subscriptions AS ext
	ON b_id = sb_book
ORDER BY real_count DESC;

SELECT * FROM books;








				
				