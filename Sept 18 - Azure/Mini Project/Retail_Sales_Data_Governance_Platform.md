# Retail Sales Data Governance Platform Using Unity Catalog

## Project Goals
1. **Setup a Unity Catalog Metastore**
2. **Create a Sales Data Schema**
3. **Create and Manage Tables in the Catalog**
4. **Set Up Views and Perform Operations on the Data**
5. **Control Access to the Data**
6. **Explore Data Lineage and Auditing**

---

## Step-by-Step Implementation

### Step 1: Setup Unity Catalog Metastore

1. **Create a Metastore**
   - Navigate to the Databricks admin console.
   - Go to **Unity Catalog**.
   - Click **Create Metastore** and follow the prompts to create a new metastore.

2. **Assign the Metastore to Your Workspace**
   - After creating the metastore, assign it to your workspace through the admin console.

### Step 2: Create a Retail Catalog and Sales Schema

1. **Create the Retail Data Catalog**

   ```sql
   CREATE CATALOG retail_data;
   ```

2. **Create a Sales Schema in the Catalog**

   ```sql
   CREATE SCHEMA retail_data.sales;
   ```

### Step 3: Create Tables in the Sales Schema

1. **Create the `product_sales` Table**

   ```sql
   CREATE TABLE retail_data.sales.product_sales (
       SaleID INT,
       ProductName STRING,
       Quantity INT,
       SaleDate DATE
   );
   ```

2. **Insert Sample Data into the `product_sales` Table**

   ```sql
   INSERT INTO retail_data.sales.product_sales
   VALUES
   (1, 'Product A', 10, '2024-01-01'),
   (2, 'Product B', 5, '2024-02-01'),
   (3, 'Product C', 20, '2024-03-01');
   ```

3. **Create the `customer_data` Table**

   ```sql
   CREATE TABLE retail_data.sales.customer_data (
       CustomerID INT,
       CustomerName STRING,
       Email STRING,
       JoinDate DATE
   );
   ```

4. **Insert Sample Data into the `customer_data` Table**

   ```sql
   INSERT INTO retail_data.sales.customer_data
   VALUES
   (1, 'Abdullah Khan', 'abdullah@example.com', '2023-01-01'),
   (2, 'John Smith', 'john@example.com', '2023-02-01'),
   (3, 'Sharma', 'sharma@example.com', '2023-03-01');
   ```

### Step 4: Create Views and Manage Data

1. **Create a View for Recent Sales (Last 30 Days)**

   ```sql
   CREATE VIEW retail_data.sales.recent_sales AS
   SELECT *
   FROM retail_data.sales.product_sales
   WHERE SaleDate >= current_date() - INTERVAL 30 DAYS;
   ```

2. **Create a View to Join Customer and Sales Data**

   ```sql
   CREATE VIEW retail_data.sales.customer_sales AS
   SELECT c.CustomerID, c.CustomerName, p.ProductName, p.Quantity, p.SaleDate
   FROM retail_data.sales.customer_data c
   JOIN retail_data.sales.product_sales p
   ON c.CustomerID = p.SaleID;
   ```

### Step 5: Implement Data Access Controls

1. **Grant Read Access to a User (e.g., Analyst) to the Recent Sales View**

   ```sql
   GRANT SELECT ON VIEW retail_data.sales.recent_sales TO `analyst@example.com`;
   ```

2. **Grant Full Access to the Sales Data to a Manager**

   ```sql
   GRANT ALL PRIVILEGES ON TABLE retail_data.sales.product_sales TO `manager@example.com`;
   ```

3. **Revoke Access from a User (if needed)**

   ```sql
   REVOKE SELECT ON VIEW retail_data.sales.recent_sales FROM `analyst@example.com`;
   ```

### Step 6: Explore Data Lineage and Auditing

1. **Data Lineage**
   - Go to the Databricks UI under **Catalog Explorer**.
   - Check the lineage of the `product_sales` table and `recent_sales` view to track data sources and usage.

2. **Audit Logs**
   - In the Databricks admin console, navigate to **Audit Logs**.
   - Review logs for operations such as table creation, data insertion, and access control changes.

### Step 7: Explore Advanced Capabilities (Optional)

1. **Data Retention (Vacuum)**

   ```sql
   VACUUM retail_data.sales.product_sales RETAIN 168 HOURS;
   ```

2. **Time Travel**

   - View the history of the `product_sales` table:

     ```sql
     DESCRIBE HISTORY retail_data.sales.product_sales;
     ```

   - Query the table as it existed at a previous version (e.g., Version 2):

     ```sql
     SELECT *
     FROM retail_data.sales.product_sales VERSION AS OF 2;
     ```
