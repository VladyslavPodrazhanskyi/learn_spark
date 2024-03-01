SELECT `s_name`,
COUNT(*) AS `people_count`
FROM
`subscribers`
GROUP BY `s_name`


SELECT COUNT(*) AS `total_books`
FROM
`books`;


select *, count(sb_book)over(PARTITION by sb_subscriber) as book_count
from subscriptions;


select * from m2m_books_authors mmba

select * from m2m_books_genres mmbg 



select *, ROW_NUMBER() over(ORDER by sb_start) as row_num
from subscriptions;
