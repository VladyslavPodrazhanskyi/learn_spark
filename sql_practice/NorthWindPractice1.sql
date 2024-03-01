-- https://en.wikiversity.org/wiki/Database_Examples/Northwind/MySQL
-- https://en.wikiversity.org/wiki/Database_Examples/Northwind

-- https://www.w3schools.com/sql/sql_distinct.asp

USE Northwind;

SELECT * FROM Customers;

SELECT CustomerName,City FROM Customers;

SELECT DISTINCT Country FROM Customers
ORDER BY Country;

SELECT COUNT(DISTINCT Country) FROM Customers;

SELECT * FROM Customers
WHERE Country='Mexico';

SELECT * FROM Customers
WHERE CustomerID=1;

SELECT * FROM Customers
WHERE CustomerID > 80;


SELECT * FROM Products
ORDER BY Price;
