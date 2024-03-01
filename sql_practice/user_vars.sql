-- In SQL, user variables are used to store temporary values during the execution of a query. 
-- They are denoted by the @ symbol followed by a variable name. 
-- User variables can be helpful in situations 
-- where you need to maintain state or perform calculations within a query.
-- 
-- Here is a basic explanation of the syntax for using user variables in SQL:
-- 
-- Declaration:
-- 
-- To declare a user variable, you typically use the := operator.
-- Example: @variable_name := initial_value;

-- Initialization:
-- 
-- You can initialize a user variable when declaring it.
-- Example: SELECT @counter := 1;

-- Usage:
-- You can use the user variable in subsequent parts of your query.

-- Example: SELECT column1, column2, @counter := @counter + 1 AS row_number FROM table1;
-- Note on Order of Assignment:
-- 
-- In SQL, the order of assignment of user variables in a SELECT statement is not guaranteed. 
-- The SQL standard does not specify the order in which expressions are evaluated.
-- To ensure a specific order of assignment, 
-- it's a good practice to use the assignment within a single expression or use the ORDER BY clause.

use library;


SELECT 0            -- Select @counter := 5;
INTO @counter;
-- FROM books;




select *,  
	@counter := @counter + 1 AS rnum
from books
order by b_name;







