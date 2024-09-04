-- Create Customers Table
CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY IDENTITY(1,1),
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Email VARCHAR(100),
    PhoneNumber VARCHAR(15)
);

-- Insert Sample Data into Customers
INSERT INTO Customers (FirstName, LastName, Email, PhoneNumber)
VALUES 
('Amit', 'Sharma', 'amit.sharma@example.com', '9876543210'),
('Priya', 'Mehta', 'priya.mehta@example.com', '8765432109'),
('Rohit', 'Kumar', 'rohit.kumar@example.com', '7654321098'),
('Neha', 'Verma', 'neha.verma@example.com', '6543210987'),
('Siddharth', 'Singh', 'siddharth.singh@example.com', '5432109876'),
('Asha', 'Rao', 'asha.rao@example.com', '4321098765'),
('Raj', 'Patel', 'raj.patel@example.com', '3210987654'),
('Meera', 'Gupta', 'meera.gupta@example.com', '2109876543'),
('Vikram', 'Yadav', 'vikram.yadav@example.com', '1098765432');

-- Create Products Table
CREATE TABLE Products (
    ProductID INT PRIMARY KEY IDENTITY(1,1),
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    Price DECIMAL(10, 2),
    StockQuantity INT
);

-- Insert Sample Data into Products
INSERT INTO Products (ProductName, Category, Price, StockQuantity)
VALUES 
('Laptop', 'Electronics', 75000.00, 15),
('Smartphone', 'Electronics', 25000.00, 30),
('Desk Chair', 'Furniture', 5000.00, 10),
('Monitor', 'Electronics', 12000.00, 20),
('Bookshelf', 'Furniture', 8000.00, 8),
('Tablet', 'Electronics', 20000.00, 12),
('Headphones', 'Electronics', 3000.00, 25);

-- Create Orders Table
CREATE TABLE Orders (
    OrderID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT,
    ProductID INT,
    Quantity INT,
    TotalAmount DECIMAL(10, 2),
    OrderDate DATE,
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);

-- Insert Sample Data into Orders
INSERT INTO Orders (CustomerID, ProductID, Quantity, TotalAmount, OrderDate)
VALUES 
(1, 1, 2, 150000.00, '2024-08-01'),
(2, 2, 1, 25000.00, '2024-08-02'),
(3, 3, 1, 5000.00, '2024-08-03'),
(4, 4, 2, 24000.00, '2024-08-04'),
(5, 5, 1, 8000.00, '2024-08-05'),
(6, 6, 2, 12000.00, '2024-08-06'),
(7, 7, 1, 20000.00, '2024-08-07');

-- 1. Retrieve all products from the Products table that belong to the category 'Electronics' and have a price greater than 500.
SELECT * 
FROM Products
WHERE Category = 'Electronics' AND Price > 500;

-- 2. Calculate the total quantity of products sold from the Orders table.
SELECT SUM(Quantity) AS TotalQuantity
FROM Orders;

-- 3. Calculate the total revenue generated for each product in the Orders table.
SELECT 
    p.ProductName,
    SUM(o.Quantity * p.Price) AS TotalRevenue
FROM Orders o
JOIN Products p ON o.ProductID = p.ProductID
GROUP BY p.ProductName;

-- 4. Write a query that uses WHERE, GROUP BY, HAVING, and ORDER BY clauses and explain the order of execution.
-- Query to find the average price of products in each category
-- Only include categories where the average price is greater than 15000
-- Order the results by average price in ascending order
SELECT 
    p.Category,
    AVG(p.Price) AS AveragePrice
FROM Products p
WHERE p.StockQuantity > 0
GROUP BY p.Category
HAVING AVG(p.Price) > 15000
ORDER BY AveragePrice ASC;

-- 5. Write a query that corrects a violation of using non-aggregated columns without grouping them.
SELECT 
    MAX(Category) AS Category,  -- Aggregate function for non-aggregated column
    AVG(Price) AS AveragePrice
FROM Products;

-- 6. Retrieve all customers who have placed more than 5 orders using GROUP BY and HAVING clauses.
SELECT 
    c.CustomerID,
    c.FirstName,
    c.LastName,
    COUNT(o.OrderID) AS OrderCount
FROM Orders o
JOIN Customers c ON o.CustomerID = c.CustomerID
GROUP BY c.CustomerID, c.FirstName, c.LastName
HAVING COUNT(o.OrderID) > 5;

-- 7. Create a stored procedure named GetAllCustomers that retrieves all customer details from the Customers table.
CREATE PROCEDURE GetAllCustomers
AS
BEGIN
    SELECT * FROM Customers;
END;

-- 8. Create a stored procedure named GetOrderDetailsByOrderID that accepts an OrderID as a parameter and retrieves the order details for that specific order.
CREATE PROCEDURE GetOrderDetailsByOrderID
    @OrderID INT
AS
BEGIN
    SELECT * FROM Orders WHERE OrderID = @OrderID;
END;

-- 9. Create a stored procedure named GetProductsByCategoryAndPrice that accepts a product Category and a minimum Price as input parameters and retrieves all products that meet the criteria.
CREATE PROCEDURE GetProductsByCategoryAndPrice
    @Category VARCHAR(50),
    @MinPrice DECIMAL(10, 2)
AS
BEGIN
    SELECT * FROM Products 
    WHERE Category = @Category AND Price >= @MinPrice;
END;

-- 10. Create a stored procedure named InsertNewProduct that accepts parameters for ProductName, Category, Price, and StockQuantity and inserts a new product into the Products table.
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

-- 11. Create a stored procedure named UpdateCustomerEmail that accepts a CustomerID and a NewEmail parameter and updates the email address for the specified customer.
CREATE PROCEDURE UpdateCustomerEmail
    @CustomerID INT,
    @NewEmail VARCHAR(100)
AS
BEGIN
    UPDATE Customers
    SET Email = @NewEmail
    WHERE CustomerID = @CustomerID;
END;

-- 12. Create a stored procedure named DeleteOrderByID that accepts an OrderID as a parameter and deletes the corresponding order from the Orders table.
CREATE PROCEDURE DeleteOrderByID
    @OrderID INT
AS
BEGIN
    DELETE FROM Orders
    WHERE OrderID = @OrderID;
END;

-- 13. Create a stored procedure named GetTotalProductsInCategory that accepts a Category parameter and returns the total number of products in that category using an output parameter.
CREATE PROCEDURE GetTotalProductsInCategory
    @Category VARCHAR(50),
    @TotalProducts INT OUTPUT
AS
BEGIN
    SELECT @TotalProducts = COUNT(*)
    FROM Products
    WHERE Category = @Category;
END;
