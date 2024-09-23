# Mini Project: Data Governance Using Unity Catalog - Advanced Capabilities

## Task 1: Set Up Unity Catalog Objects with Multiple Schemas

1. **Create a Catalog:**
   ```sql
   CREATE CATALOG finance_data_catalog;
   ```

2. **Create Multiple Schemas:**
   - Schema for transaction data
     ```sql
     CREATE SCHEMA finance_data_catalog.transaction_data
     COMMENT 'Schema for storing transaction data';
     ```
   - Schema for customer data
     ```sql
     CREATE SCHEMA finance_data_catalog.customer_data
     COMMENT 'Schema for storing customer data';
     ```

3. **Create Tables in Each Schema:**
   - For `transaction_data` schema:
     ```sql
     CREATE TABLE finance_data_catalog.transaction_data.transactions (
         TransactionID STRING,
         CustomerID STRING,
         TransactionAmount DECIMAL(10, 2),
         TransactionDate DATE
     )
     COMMENT 'Table for storing transaction records';
     ```
   - For `customer_data` schema:
     ```sql
     CREATE TABLE finance_data_catalog.customer_data.customers (
         CustomerID STRING,
         CustomerName STRING,
         Email STRING,
         Country STRING
     )
     COMMENT 'Table for storing customer information';
     ```

## Task 2: Data Discovery Across Schemas

1. **Explore Metadata:**
   - List tables in the `transaction_data` schema:
     ```sql
     SHOW TABLES IN finance_data_catalog.transaction_data;
     ```
   - List tables in the `customer_data` schema:
     ```sql
     SHOW TABLES IN finance_data_catalog.customer_data;
     ```
   - Describe tables:
     ```sql
     DESCRIBE TABLE finance_data_catalog.transaction_data.transactions; 
     DESCRIBE TABLE finance_data_catalog.customer_data.customers;
     ```

2. **Data Profiling:**
   - Transaction Amounts:
     ```sql
     SELECT 
         MIN(TransactionAmount) AS MinAmount,
         MAX(TransactionAmount) AS MaxAmount,
         AVG(TransactionAmount) AS AvgAmount,
         COUNT(*) AS TotalTransactions
     FROM finance_data_catalog.transaction_data.transactions;
     ```

   - Group transactions by date:
     ```sql
     SELECT 
         TransactionDate,
         SUM(TransactionAmount) AS TotalAmount,
         COUNT(*) AS NumberOfTransactions
     FROM finance_data_catalog.transaction_data.transactions
     GROUP BY TransactionDate
     ORDER BY TransactionDate;
     ```

   - Profiling Customer Data:
     ```sql
     SELECT 
         Country,
         COUNT(*) AS NumberOfCustomers
     FROM finance_data_catalog.customer_data.customers
     GROUP BY Country
     ORDER BY NumberOfCustomers DESC;
     ```

3. **Tagging Sensitive Data:**
   ```sql
   ALTER TABLE finance_data_catalog.customer_data.customers
   ADD TAG ('sensitive' = 'Email');

   ALTER TABLE finance_data_catalog.transaction_data.transactions
   ADD TAG ('sensitive' = 'TransactionAmount');
   ```

## Task 3: Implement Data Lineage and Auditing

1. **Track Data Lineage:**
   ```sql
   CREATE OR REPLACE TABLE finance_data_catalog.merged_data.customer_transactions AS
   SELECT 
       t.TransactionID,
       t.CustomerID,
       c.CustomerName,
       c.Email,
       c.Country,
       t.TransactionAmount,
       t.TransactionDate
   FROM 
       finance_data_catalog.transaction_data.transactions t
   JOIN 
       finance_data_catalog.customer_data.customers c
   ON 
       t.CustomerID = c.CustomerID;
   ```

2. **Audit User Actions:**
   ```sql
   SELECT 
       user_name,
       action_name,
       object_name,
       timestamp,
       details
   FROM 
       catalog_audit_logs
   WHERE 
       object_name IN ('finance_data_catalog.transaction_data.transactions', 
                       'finance_data_catalog.customer_data.customers')
   ORDER BY 
       timestamp DESC;
   ```

## Task 4: Access Control and Permissions

1. **Set Up Roles and Groups:**
   ```sql
   CREATE GROUP DataEngineers;
   CREATE GROUP DataAnalysts;
   ```

   - Grant full access to DataEngineers:
     ```sql
     GRANT ALL PRIVILEGES ON SCHEMA finance_data_catalog.transaction_data TO DataEngineers;
     GRANT ALL PRIVILEGES ON SCHEMA finance_data_catalog.customer_data TO DataEngineers;

     GRANT ALL PRIVILEGES ON TABLE finance_data_catalog.transaction_data.transactions TO DataEngineers;
     GRANT ALL PRIVILEGES ON TABLE finance_data_catalog.customer_data.customers TO DataEngineers;
     ```

   - Grant read-only access to DataAnalysts:
     ```sql
     GRANT SELECT ON SCHEMA finance_data_catalog.customer_data TO DataAnalysts;
     GRANT SELECT ON TABLE finance_data_catalog.customer_data.customers TO DataAnalysts;

     GRANT USAGE ON SCHEMA finance_data_catalog.transaction_data TO DataAnalysts;
     GRANT SELECT ON TABLE finance_data_catalog.transaction_data.transactions TO DataAnalysts;
     ```

2. **Row-Level Security:**
   ```sql
   CREATE OR REPLACE VIEW finance_data_catalog.transaction_data.secure_transactions AS
   SELECT * 
   FROM finance_data_catalog.transaction_data.transactions
   WHERE 
       TransactionAmount <= 10000
       OR current_user() IN ('admin_user', 'high_value_role');

   GRANT SELECT ON VIEW finance_data_catalog.transaction_data.secure_transactions TO DataAnalysts;
   REVOKE SELECT ON TABLE finance_data_catalog.transaction_data.transactions FROM DataAnalysts;
   ```

## Task 5: Data Governance Best Practices

1. **Create Data Quality Rules:**
   ```sql
   ALTER TABLE finance_data_catalog.transaction_data.transactions 
   ADD CONSTRAINT non_negative_transaction CHECK (TransactionAmount >= 0);

   SELECT * FROM finance_data_catalog.transaction_data.transactions WHERE TransactionAmount < 0;
   SELECT * FROM finance_data_catalog.customer_data.customers 
   WHERE Email NOT RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$';
   ```

2. **Validate Data Governance:**
   ```sql
   SELECT user_name, action_name, object_name, timestamp, details
   FROM catalog_audit_logs 
   WHERE object_name IN ('finance_data_catalog.transaction_data.transactions', 
                         'finance_data_catalog.customer_data.customers', 
                         'finance_data_catalog.merged_data.customer_transactions') 
   ORDER BY timestamp DESC;
   ```

## Task 6: Data Lifecycle Management

1. **Implement Time Travel:**
   ```sql
   DESCRIBE HISTORY finance_data_catalog.transaction_data.transactions;
   SELECT * FROM finance_data_catalog.transaction_data.transactions VERSION AS OF 3;
   SELECT * FROM finance_data_catalog.transaction_data.transactions TIMESTAMP AS OF '2024-09-20T12:00:00';
   RESTORE TABLE finance_data_catalog.transaction_data.transactions TO VERSION AS OF 3;
   ```

2. **Run a Vacuum Operation:**
   ```sql
   VACUUM finance_data_catalog.transaction_data.transactions RETAIN 168 HOURS;
   VACUUM finance_data_catalog.transaction_data.transactions RETAIN 24 HOURS;
   OPTIMIZE finance_data_catalog.transaction_data.transactions;
   ```

---

# Mini Project: Advanced Data Governance and Security Using Unity Catalog

## Task 1: Set Up Multi-Tenant Data Architecture Using Unity Catalog

1. **Create a New Catalog:**
   ```sql
   CREATE CATALOG corporate_data_catalog;
   ```

2. **Create Schemas for Each Department:**
   ```sql
   CREATE SCHEMA corporate_data_catalog.sales_data;
   CREATE SCHEMA corporate_data_catalog.hr_data;
   CREATE SCHEMA corporate_data_catalog.finance_data;
   ```

3. **Create Tables in Each Schema:**
   - For Sales Data:
     ```sql
     CREATE TABLE corporate_data_catalog.sales_data.sales_table(
         SalesID STRING,
         CustomerID STRING,
         SalesAmount DECIMAL(10,2),
         SalesDate DATE
     );
     ```
   - For HR Data:
     ```sql
     CREATE TABLE corporate_data_catalog.hr_data.hr_table(
         EmployeeID STRING,
         EmployeeName STRING,
         Department STRING,
         Salary DECIMAL(10,2)
     );
     ```
   - For Finance Data:
     ```sql
     CREATE TABLE corporate_data_catalog.finance_data.finance_table(
         InvoiceID STRING,
         VendorID STRING,
         InvoiceAmount DECIMAL(10,2),
         PaymentDate DATE
     );
     ```

## Task 2: Enable Data Discovery for Cross-Departmental Data

1. **Search for Tables Across Departments:**
   ```sql
   SHOW TABLES IN corporate_data_catalog.sales_data;
   SHOW TABLES IN corporate_data_catalog.hr_data;
   SHOW TABLES IN corporate_data_catalog.finance_data;
   ```

2. **Tag Sensitive Information:**
   ```sql
   ALTER TABLE corporate_data_catalog.hr_data.hr_table
   SET TAG 'sensitive' ON COLUMN Salary;

   ALTER TABLE corporate_data_catalog.finance_data.finance_table
   SET TAG 'sensitive' ON COLUMN InvoiceAmount;
   ```

3. **Data Profiling:**
   ```sql
   SELECT AVG(SalesAmount), MIN(SalesAmount), MAX(SalesAmount) 
   FROM corporate_data_catalog.sales_data.sales_table;

   SELECT AVG(Salary), MIN(Salary), MAX(Salary) 
   FROM corporate_data_catalog.hr_data.hr_table;

   SELECT AVG(InvoiceAmount), MIN(InvoiceAmount), MAX(InvoiceAmount) 
   FROM corporate_data_catalog.finance_data.finance_table;
   ```

## Task 3: Implement Data Lineage and Data Auditing

1. **Track Data Lineage:**
   ```sql
   CREATE TABLE corporate_data_catalog.reporting_data.sales_summary AS
   SELECT 
       s.SalesID,
       s.CustomerID,
       SUM(s.SalesAmount) AS TotalSales
   FROM 
       corporate_data_catalog.sales_data.sales_table s
   GROUP BY 
       s.CustomerID;
   ```

2. **Audit User Access:**
   ```sql
   SELECT user_name, action_name, object_name, timestamp 
   FROM access_logs
   WHERE object_name LIKE 'corporate_data_catalog%';
   ```

## Task 4: Role-Based Access Control

1. **Create Roles for Different Departments:**
   ```sql
   CREATE ROLE SalesRole;
   CREATE ROLE HRRole;
   CREATE ROLE FinanceRole;
   ```

2. **Assign Permissions:**
   ```sql
   GRANT SELECT ON corporate_data_catalog.sales_data.sales_table TO SalesRole;
   GRANT SELECT ON corporate_data_catalog.hr_data.hr_table TO HRRole;
   GRANT SELECT ON corporate_data_catalog.finance_data.finance_table TO FinanceRole;
   ```

3. **Implement Row-Level Security:**
   ```sql
   CREATE OR REPLACE VIEW corporate_data_catalog.sales_data.secure_sales AS
   SELECT * 
   FROM corporate_data_catalog.sales_data.sales_table
   WHERE SalesAmount < 10000 
   OR current_user() = 'admin';
   ```

## Task 5: Data Quality and Governance Policies

1. **Create Data Quality Constraints:**
   ```sql
   ALTER TABLE corporate_data_catalog.finance_data.finance_table 
   ADD CONSTRAINT positive_invoice CHECK (InvoiceAmount > 0);
   ```

2. **Run Data Quality Checks:**
   ```sql
   SELECT * FROM corporate_data_catalog.finance_data.finance_table 
   WHERE InvoiceAmount <= 0;
   ```

## Task 6: Implement Data Lifecycle Management Policies

1. **Implement Time Travel:**
   ```sql
   SELECT * FROM corporate_data_catalog.sales_data.sales_table 
   VERSION AS OF 2;
   ```

2. **Run Vacuum and Optimize:**
   ```sql
   VACUUM corporate_data_catalog.sales_data.sales_table RETAIN 168 HOURS;
   ```
   
---

# Mini Project: Building a Secure Data Platform with Unity Catalog

## Task 1: Set Up Unity Catalog for Multi-Domain Data Management

1. **Create a New Catalog:**
   ```sql
   CREATE CATALOG enterprise_data_catalog;
   ```

2. **Create Schemas for Each Department:**
   ```sql
   CREATE SCHEMA enterprise_data_catalog.marketing_data;
   CREATE SCHEMA enterprise_data_catalog.operations_data;
   CREATE SCHEMA enterprise_data_catalog.it_data;
   ```

3. **Create Tables in Each Schema:**
   - **For Marketing Data:**
     ```sql
     CREATE TABLE enterprise_data_catalog.marketing_data.marketing_table (
         CampaignID INT,
         CampaignName STRING,
         Budget DECIMAL(10,2),
         StartDate DATE
     );
     ```

   - **For Operations Data:**
     ```sql
     CREATE TABLE enterprise_data_catalog.operations_data.operations_table (
         OrderID INT,
         ProductID INT,
         Quantity INT,
         ShippingStatus STRING
     );
     ```

   - **For IT Data:**
     ```sql
     CREATE TABLE enterprise_data_catalog.it_data.it_table (
         IncidentID STRING,
         ReportedBy STRING,
         IssueType STRING,
         ResolutionTime INT
     );
     ```

## Task 2: Data Discovery and Classification

1. **Search for Data Across Schemas:**
   ```sql
   SHOW TABLES IN enterprise_data_catalog;
   ```

2. **Tag Sensitive Information:**
   ```sql
   ALTER TABLE enterprise_data_catalog.marketing_data.marketing_table
   SET TAG 'sensitive' ON COLUMN Budget;

   ALTER TABLE enterprise_data_catalog.it_data.it_table
   SET TAG 'sensitive' ON COLUMN ResolutionTime;
   ```

3. **Data Profiling:**
   ```sql
   SELECT AVG(Budget), MIN(Budget), MAX(Budget) 
   FROM enterprise_data_catalog.marketing_data.marketing_table;

   SELECT COUNT(ShippingStatus), ShippingStatus 
   FROM enterprise_data_catalog.operations_data.operations_table 
   GROUP BY ShippingStatus;
   ```

## Task 3: Data Lineage and Data Auditing

1. **Track Data Lineage Across Schemas:**
   - Link the marketing data with the operations data by joining campaign performance with product orders:
   ```sql
   CREATE TABLE enterprise_data_catalog.reporting.campaign_orders_report AS
   SELECT 
       m.CampaignID, 
       m.CampaignName, 
       m.Budget, 
       o.OrderID, 
       o.ProductID, 
       o.Quantity
   FROM 
       enterprise_data_catalog.marketing_data.marketing_table m
   JOIN 
       enterprise_data_catalog.operations_data.operations_table o
   ON 
       m.CampaignID = o.ProductID;
   ```

## Task 4: Implement Fine-Grained Access Control

1. **Create User Roles and Groups:**
   ```sql
   CREATE GROUP MarketingTeam;
   GRANT USAGE ON SCHEMA enterprise_data_catalog.marketing_data TO MarketingTeam;

   CREATE GROUP OperationsTeam;
   GRANT USAGE ON SCHEMA enterprise_data_catalog.operations_data TO OperationsTeam;

   CREATE GROUP ITSupportTeam;
   GRANT USAGE ON SCHEMA enterprise_data_catalog.it_data TO ITSupportTeam;
   GRANT UPDATE ON TABLE enterprise_data_catalog.it_data.it_table TO ITSupportTeam;
   ```

2. **Implement Column-Level Security:**
   ```sql
   GRANT SELECT ON COLUMN Budget TO MarketingTeam;
   ```

3. **Row-Level Security:**
   ```sql
   CREATE ROW ACCESS POLICY operations_team_policy 
   ON enterprise_data_catalog.operations_data.operations_table
   FOR EACH ROW
   WHEN current_user() = 'operations_rep';
   ```

## Task 5: Data Governance and Quality Enforcement

1. **Set Data Quality Rules:**
   - Campaign budget greater than zero:
     ```sql
     SELECT * FROM enterprise_data_catalog.marketing_data.marketing_table
     WHERE Budget <= 0;
     ```

   - Shipping status is valid (e.g., 'Pending', 'Shipped', 'Delivered'):
     ```sql
     SELECT * FROM enterprise_data_catalog.operations_data.operations_table
     WHERE ShippingStatus NOT IN ('Pending', 'Shipped', 'Delivered');
     ```

   - Issue resolution times are recorded correctly and not negative:
     ```sql
     SELECT * FROM enterprise_data_catalog.it_data.it_table 
     WHERE ResolutionTime < 0;
     ```

2. **Apply Delta Lake Time Travel:**
   ```sql
   RESTORE TABLE enterprise_data_catalog.operations_data.operations_table
   TO VERSION AS OF 1;
   ```

## Task 6: Performance Optimization and Data Cleanup

1. **Optimize Delta Tables:**
   ```sql
   OPTIMIZE enterprise_data_catalog.operations_data.operations_table;
   OPTIMIZE enterprise_data_catalog.it_data.it_table;
   ```

2. **Vacuum Delta Tables:**
   ```sql
   VACUUM enterprise_data_catalog.operations_data.operations_table
   RETAIN 168 HOURS;

   VACUUM enterprise_data_catalog.it_data.it_table 
   RETAIN 168 HOURS;
   ```
