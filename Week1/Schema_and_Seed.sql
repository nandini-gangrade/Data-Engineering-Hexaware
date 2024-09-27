-- Create Tables
CREATE TABLE customer_dim (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(255),
    location VARCHAR(255)
);

CREATE TABLE product_dim (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(255)
);

CREATE TABLE order_fact (
    order_id INT PRIMARY KEY,
    product_id INT,
    customer_id INT,
    quantity INT,
    order_amount DECIMAL(10, 2),
    order_date DATETIME,
    FOREIGN KEY (product_id) REFERENCES product_dim(product_id),
    FOREIGN KEY (customer_id) REFERENCES customer_dim(customer_id)
);

-- Inserting Data into customer_dim
INSERT INTO customer_dim (customer_id, customer_name, location) VALUES (2001, 'Alice', 'New York');
INSERT INTO customer_dim (customer_id, customer_name, location) VALUES (2002, 'Bob', 'California');
INSERT INTO customer_dim (customer_id, customer_name, location) VALUES (2003, 'Charlie', 'Texas');
INSERT INTO customer_dim (customer_id, customer_name, location) VALUES (2004, 'David', 'Florida');
INSERT INTO customer_dim (customer_id, customer_name, location) VALUES (2005, 'Eva', 'Washington');

-- Inserting Data into product_dim
INSERT INTO product_dim (product_id, product_name, category) VALUES (1, 'Smartphone', 'Electronics');
INSERT INTO product_dim (product_id, product_name, category) VALUES (2, 'Laptop', 'Electronics');
INSERT INTO product_dim (product_id, product_name, category) VALUES (3, 'Tablet', 'Electronics');
INSERT INTO product_dim (product_id, product_name, category) VALUES (4, 'Headphones', 'Accessories');
INSERT INTO product_dim (product_id, product_name, category) VALUES (5, 'Smartwatch', 'Wearables');

-- Inserting Data into order_fact
INSERT INTO order_fact (order_id, product_id, customer_id, quantity, order_amount, order_date) VALUES (1, 1, 2001, 2, 149.99, '2024-09-27 08:15:20');
INSERT INTO order_fact (order_id, product_id, customer_id, quantity, order_amount, order_date) VALUES (2, 2, 2002, 1, 799.99, '2024-09-27 08:16:20');
INSERT INTO order_fact (order_id, product_id, customer_id, quantity, order_amount, order_date) VALUES (3, 3, 2003, 3, 299.99, '2024-09-27 08:17:20');
INSERT INTO order_fact (order_id, product_id, customer_id, quantity, order_amount, order_date) VALUES (4, 4, 2004, 1, 199.99, '2024-09-27 08:18:20');
INSERT INTO order_fact (order_id, product_id, customer_id, quantity, order_amount, order_date) VALUES (5, 5, 2005, 4, 399.99, '2024-09-27 08:19:20');

-- 1. Retrieve all customer data
SELECT * FROM customer_dim;

-- 2. Retrieve all product data
SELECT * FROM product_dim;

-- 3. Retrieve all order data
SELECT * FROM order_fact;
