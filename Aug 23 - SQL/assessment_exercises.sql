-- 1. Hands-on Exercise: Filtering Data using SQL Queries
SELECT * 
FROM Products 
WHERE Category = 'Electronics' 
AND Price > 500;

-- 2. Hands-on Exercise: Total Aggregations using SQL Queries
SELECT SUM(Quantity) AS TotalQuantitySold 
FROM Orders;

-- 3. Hands-on Exercise: Group By Aggregations using SQL Queries
SELECT ProductID, SUM(TotalAmount) AS TotalRevenue
FROM Orders
GROUP BY ProductID;

-- 4. Hands-on Exercise: Order of Execution of SQL Queries
SELECT ProductID, SUM(TotalAmount) AS TotalRevenue
FROM Orders
WHERE Quantity > 1  
GROUP BY ProductID  
HAVING SUM(TotalAmount) > 50000  
ORDER BY TotalRevenue DESC;

-- 5. Hands-on Exercise: Rules and Restrictions to Group and Filter Data in SQL Queries
SELECT ProductID, SUM(TotalAmount) AS TotalAmount
FROM Orders
GROUP BY ProductID;

-- 6. Hands-on Exercise: Filter Data based on Aggregated Results using Group By and Having
SELECT CustomerID, COUNT(OrderID) AS TotalOrders
FROM Orders
GROUP BY CustomerID
HAVING COUNT(OrderID) > 5;

-- 1. Basic Stored Procedure
CREATE PROCEDURE GetAllCustomers
AS
BEGIN
    SELECT * FROM Customers;
END;
EXEC GetAllCustomers;

-- 2. Stored Procedure with Input Parameter
CREATE PROCEDURE GetOrderDetailsByOrderID
    @OrderID INT
AS
BEGIN
    SELECT * 
    FROM Orders 
    WHERE OrderID = @OrderID;
END;
EXEC GetOrderDetailsByOrderID @OrderID = 2;

-- 3. Stored Procedure with Multiple Input Parameters
CREATE PROCEDURE GetProductsByCategoryAndPrice
    @Category VARCHAR(50),
    @MinPrice DECIMAL(10, 2)
AS
BEGIN
    SELECT * 
    FROM Products 
    WHERE Category = @Category 
    AND Price >= @MinPrice;
END;
EXEC GetProductsByCategoryAndPrice @Category = 'Electronics', @MinPrice = 20000;

-- 4. Stored Procedure with Insert Operation
CREATE PROCEDURE InsertNewProduct
    @ProductName VARCHAR(100),
    @Category VARCHAR(50),
    @Price DECIMAL(10, 2),
    @StockQuantity INT
AS
BEGIN
    INSERT INTO Products (ProductName, Category, Price, StockQuantity)
    VALUES (@ProductName, @Category, @Price, @StockQuantity);
END;
EXEC InsertNewProduct @ProductName = 'Table Fan', @Category = 'Electronics', @Price = 20000, @StockQuantity = 6;

-- 5. Stored Procedure with Update Operation
CREATE PROCEDURE UpdateCustomerEmail
    @CustomerID INT,
    @NewEmail VARCHAR(100)
AS
BEGIN
    UPDATE Customers
    SET Email = @NewEmail
    WHERE CustomerID = @CustomerID;
END;
EXEC UpdateCustomerEmail @CustomerID = 2, @NewEmail = 'anu@gmail.com';

-- 6. Stored Procedure with Delete Operation
CREATE PROCEDURE DeleteOrderByID
    @OrderID INT
AS
BEGIN
    DELETE FROM Orders
    WHERE OrderID = @OrderID;
END;
EXEC DeleteOrderByID @OrderID = 2;

-- 7. Stored Procedure with Output Parameter
CREATE PROCEDURE GetTotalProductsInCategory
    @Category VARCHAR(50),
    @TotalProducts INT OUTPUT
AS
BEGIN
    SELECT @TotalProducts = COUNT(*)
    FROM Products
    WHERE Category = @Category;
END;
EXEC GetTotalProductsInCategory @Category = 'Electronics', @TotalProducts = 350;
