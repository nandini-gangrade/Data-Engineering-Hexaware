# Unity Catalog and Databricks Mini Project

## Table of Contents
1. [Data Governance Using Unity Catalog](#data-governance-using-unity-catalog)
   - [Task 1: Set Up Unity Catalog Objects with Multiple Schemas](#task-1-set-up-unity-catalog-objects-with-multiple-schemas)
   - [Task 2: Data Discovery Across Schemas](#task-2-data-discovery-across-schemas)
   - [Task 3: Implement Data Lineage and Auditing](#task-3-implement-data-lineage-and-auditing)
   - [Task 4: Access Control and Permissions](#task-4-access-control-and-permissions)
   - [Task 5: Data Governance Best Practices](#task-5-data-governance-best-practices)
   - [Task 6: Data Lifecycle Management](#task-6-data-lifecycle-management)
2. [Advanced Data Governance and Security Using Unity Catalog](#advanced-data-governance-and-security-using-unity-catalog)
   - [Task 1: Set Up Multi-Tenant Data Architecture Using Unity Catalog](#task-1-set-up-multi-tenant-data-architecture-using-unity-catalog)
   - [Task 2: Enable Data Discovery for Cross-Departmental Data](#task-2-enable-data-discovery-for-cross-departmental-data)
   - [Task 3: Implement Data Lineage and Data Auditing](#task-3-implement-data-lineage-and-data-auditing)
   - [Task 4: Data Access Control and Security](#task-4-data-access-control-and-security)
   - [Task 5: Data Governance Best Practices](#task-5-data-governance-best-practices)
   - [Task 6: Optimize and Clean Up Delta Tables](#task-6-optimize-and-clean-up-delta-tables)
3. [Building a Secure Data Platform with Unity Catalog](#building-a-secure-data-platform-with-unity-catalog)
   - [Task 1: Set Up Unity Catalog for Multi-Domain Data Management](#task-1-set-up-unity-catalog-for-multi-domain-data-management)
   - [Task 2: Data Discovery and Classification](#task-2-data-discovery-and-classification)
   - [Task 3: Data Lineage and Auditing](#task-3-data-lineage-and-auditing)
   - [Task 4: Implement Fine-Grained Access Control](#task-4-implement-fine-grained-access-control)
   - [Task 5: Data Governance and Quality Enforcement](#task-5-data-governance-and-quality-enforcement)
   - [Task 6: Performance Optimization and Data Cleanup](#task-6-performance-optimization-and-data-cleanup)
4. [Databricks Notebooks and Pipelines](#databricks-notebooks-and-pipelines)
   - [Task 1: Raw Data Ingestion](#task-1-raw-data-ingestion)
   - [Task 2: Data Cleaning](#task-2-data-cleaning)
   - [Task 3: Data Transformation](#task-3-data-transformation)
   - [Task 4: Create a Pipeline to Execute Notebooks](#task-4-create-a-pipeline-to-execute-notebooks)
   - [Bonus Task: Error Handling](#bonus-task-error-handling)
5. [Additional Tasks](#additional-tasks)
   - [Task 1: Customer Data Ingestion](#task-1-customer-data-ingestion)
   - [Task 2: Data Cleaning](#task-2-data-cleaning)
   - [Task 3: Data Aggregation](#task-3-data-aggregation)
   - [Task 4: Pipeline Creation](#task-4-pipeline-creation)
   - [Task 5: Data Validation](#task-5-data-validation)
   - [Task 1: Product Inventory Data Ingestion](#task-1-product-inventory-data-ingestion)
   - [Task 2: Data Cleaning](#task-2-data-cleaning)
   - [Task 3: Inventory Analysis](#task-3-inventory-analysis)
   - [Task 4: Build an Inventory Pipeline](#task-4-build-an-inventory-pipeline)
   - [Task 5: Inventory Monitoring](#task-5-inventory-monitoring)
   - [Task 1: Employee Attendance Data Ingestion](#task-1-employee-attendance-data-ingestion)
   - [Task 2: Data Cleaning](#task-2-data-cleaning)
   - [Task 3: Attendance Summary](#task-3-attendance-summary)
   - [Task 4: Create an Attendance Pipeline](#task-4-create-an-attendance-pipeline)
   - [Task 5: Time Travel with Delta Lake](#task-5-time-travel-with-delta-lake)

---

## Data Governance Using Unity Catalog

### Task 1: Set Up Unity Catalog Objects with Multiple Schemas

1. **Create a Catalog**
   ```sql
   CREATE CATALOG finance_data_catalog;
   ```

2. **Create Multiple Schemas**
   ```sql
   CREATE SCHEMA finance_data_catalog.transaction_data;
   CREATE SCHEMA finance_data_catalog.customer_data;
   ```

3. **Create Tables in Each Schema**
   ```sql
   CREATE TABLE finance_data_catalog.transaction_data.transactions (
     TransactionID INT,
     CustomerID INT,
     TransactionAmount DECIMAL(10,2),
     TransactionDate DATE
   );

   CREATE TABLE finance_data_catalog.customer_data.customers (
     CustomerID INT,
     CustomerName STRING,
     Email STRING,
     Country STRING
   );
   ```

### Task 2: Data Discovery Across Schemas

1. **Explore Metadata**
   ```sql
   SHOW TABLES IN finance_data_catalog.transaction_data;
   SHOW TABLES IN finance_data_catalog.customer_data;
   ```

2. **Data Profiling**
   ```sql
   SELECT TransactionAmount, COUNT(*) AS TransactionCount
   FROM finance_data_catalog.transaction_data.transactions
   GROUP BY TransactionAmount;

   SELECT Country, COUNT(*) AS CustomerCount
   FROM finance_data_catalog.customer_data.customers
   GROUP BY Country;
   ```

3. **Tagging Sensitive Data**
   ```sql
   ALTER TABLE finance_data_catalog.customer_data.customers
   ADD COLUMN Email_Tag STRING DEFAULT 'Sensitive';

   ALTER TABLE finance_data_catalog.transaction_data.transactions
   ADD COLUMN TransactionAmount_Tag STRING DEFAULT 'Sensitive';
   ```

### Task 3: Implement Data Lineage and Auditing

1. **Track Data Lineage**
   ```sql
   CREATE OR REPLACE VIEW finance_data_catalog.lineage AS
   SELECT t.TransactionID, t.CustomerID, t.TransactionAmount, c.CustomerName, c.Country
   FROM finance_data_catalog.transaction_data.transactions t
   JOIN finance_data_catalog.customer_data.customers c
   ON t.CustomerID = c.CustomerID;
   ```

2. **Audit User Actions**
   ```sql
   -- Example commands to enable audit logging, syntax depends on the platform.
   ```

### Task 4: Access Control and Permissions

1. **Set Up Roles and Groups**
   ```sql
   CREATE ROLE DataEngineers;
   CREATE ROLE DataAnalysts;

   GRANT ALL PRIVILEGES ON SCHEMA finance_data_catalog.transaction_data TO ROLE DataEngineers;
   GRANT ALL PRIVILEGES ON SCHEMA finance_data_catalog.customer_data TO ROLE DataAnalysts;

   GRANT SELECT ON SCHEMA finance_data_catalog.customer_data TO ROLE DataAnalysts;
   ```

2. **Row-Level Security**
   ```sql
   -- Example implementation might involve using filters or views.
   ```

### Task 5: Data Governance Best Practices

1. **Create Data Quality Rules**
   ```sql
   -- Ensuring non-negative transaction amounts
   ALTER TABLE finance_data_catalog.transaction_data.transactions
   ADD CONSTRAINT check_amount_non_negative CHECK (TransactionAmount >= 0);

   -- Email format validation
   -- Specific to platform and implementation
   ```

2. **Validate Data Governance**
   ```sql
   -- Run queries to check governance rules and logging
   ```

### Task 6: Data Lifecycle Management

1. **Implement Time Travel**
   ```sql
   -- Example command for Delta Lake Time Travel
   DESCRIBE HISTORY finance_data_catalog.transaction_data.transactions;
   ```

2. **Run a Vacuum Operation**
   ```sql
   VACUUM finance_data_catalog.transaction_data.transactions;
   ```

---

## Advanced Data Governance and Security Using Unity Catalog

### Task 1: Set Up Multi-Tenant Data Architecture Using Unity Catalog

1. **Create a New Catalog**
   ```sql
   CREATE CATALOG corporate_data_catalog;
   ```

2. **Create Schemas for Each Department**
   ```sql
   CREATE SCHEMA corporate_data_catalog.sales_data;
   CREATE SCHEMA corporate_data_catalog.hr_data;
   CREATE SCHEMA corporate_data_catalog.finance_data;
   ```

3. **Create Tables in Each Schema**
   ```sql
   CREATE TABLE corporate_data_catalog.sales_data.sales (
     SalesID INT,
     CustomerID INT,
     SalesAmount DECIMAL(10,2),
     SalesDate DATE
   );

   CREATE TABLE corporate_data_catalog.hr_data.employees (
     EmployeeID INT,
     EmployeeName STRING,
     Department STRING,
     Salary DECIMAL(10,2)
   );

   CREATE TABLE corporate_data_catalog.finance_data.invoices (
     InvoiceID INT,
     VendorID INT,
     InvoiceAmount DECIMAL(10,2),
     PaymentDate DATE
   );
   ```

### Task 2: Enable

 Data Discovery for Cross-Departmental Data

1. **Enable Cross-Departmental Queries**
   ```sql
   SELECT * FROM corporate_data_catalog.sales_data.sales;
   SELECT * FROM corporate_data_catalog.hr_data.employees;
   SELECT * FROM corporate_data_catalog.finance_data.invoices;
   ```

2. **Create Cross-Departmental Views**
   ```sql
   CREATE VIEW corporate_data_catalog.cross_dept_view AS
   SELECT s.SalesID, e.EmployeeName, i.InvoiceAmount
   FROM corporate_data_catalog.sales_data.sales s
   JOIN corporate_data_catalog.hr_data.employees e
   ON s.CustomerID = e.EmployeeID
   JOIN corporate_data_catalog.finance_data.invoices i
   ON s.SalesID = i.InvoiceID;
   ```

### Task 3: Implement Data Lineage and Data Auditing

1. **Track Data Lineage**
   ```sql
   CREATE OR REPLACE VIEW corporate_data_catalog.lineage AS
   SELECT s.SalesID, e.EmployeeName, i.InvoiceAmount
   FROM corporate_data_catalog.sales_data.sales s
   JOIN corporate_data_catalog.hr_data.employees e
   ON s.CustomerID = e.EmployeeID
   JOIN corporate_data_catalog.finance_data.invoices i
   ON s.SalesID = i.InvoiceID;
   ```

2. **Audit Data Access**
   ```sql
   -- Enable audit logging
   ```

### Task 4: Data Access Control and Security

1. **Define Security Policies**
   ```sql
   CREATE ROLE DataScientists;
   GRANT SELECT ON SCHEMA corporate_data_catalog.sales_data TO ROLE DataScientists;

   CREATE ROLE FinanceTeam;
   GRANT SELECT ON SCHEMA corporate_data_catalog.finance_data TO ROLE FinanceTeam;
   ```

2. **Row-Level Security**
   ```sql
   -- Implement row-level security policies if supported
   ```

### Task 5: Data Governance Best Practices

1. **Create Data Quality Rules**
   ```sql
   -- Example rules and constraints
   ```

2. **Monitor Data Governance Compliance**
   ```sql
   -- Run queries and checks to ensure compliance
   ```

### Task 6: Optimize and Clean Up Delta Tables

1. **Optimize Tables**
   ```sql
   OPTIMIZE corporate_data_catalog.sales_data.sales;
   ```

2. **Clean Up Delta Tables**
   ```sql
   VACUUM corporate_data_catalog.sales_data.sales;
   ```

---

## Building a Secure Data Platform with Unity Catalog

### Task 1: Set Up Unity Catalog for Multi-Domain Data Management

1. **Create a Unified Catalog**
   ```sql
   CREATE CATALOG global_data_catalog;
   ```

2. **Create Domain-Specific Schemas**
   ```sql
   CREATE SCHEMA global_data_catalog.customer_domain;
   CREATE SCHEMA global_data_catalog.product_domain;
   CREATE SCHEMA global_data_catalog.sales_domain;
   ```

3. **Create Tables in Each Schema**
   ```sql
   CREATE TABLE global_data_catalog.customer_domain.customers (
     CustomerID INT,
     CustomerName STRING,
     Email STRING
   );

   CREATE TABLE global_data_catalog.product_domain.products (
     ProductID INT,
     ProductName STRING,
     Category STRING
   );

   CREATE TABLE global_data_catalog.sales_domain.sales (
     SalesID INT,
     ProductID INT,
     CustomerID INT,
     SalesAmount DECIMAL(10,2)
   );
   ```

### Task 2: Data Discovery and Classification

1. **Explore Metadata Across Domains**
   ```sql
   SHOW TABLES IN global_data_catalog.customer_domain;
   SHOW TABLES IN global_data_catalog.product_domain;
   SHOW TABLES IN global_data_catalog.sales_domain;
   ```

2. **Classify Data**
   ```sql
   -- Tag sensitive data
   ```

### Task 3: Data Lineage and Auditing

1. **Track Data Lineage**
   ```sql
   CREATE OR REPLACE VIEW global_data_catalog.data_lineage AS
   SELECT c.CustomerName, p.ProductName, s.SalesAmount
   FROM global_data_catalog.sales_domain.sales s
   JOIN global_data_catalog.customer_domain.customers c
   ON s.CustomerID = c.CustomerID
   JOIN global_data_catalog.product_domain.products p
   ON s.ProductID = p.ProductID;
   ```

2. **Audit Data Usage**
   ```sql
   -- Enable and configure audit logging
   ```

### Task 4: Implement Fine-Grained Access Control

1. **Set Up Role-Based Access Control**
   ```sql
   CREATE ROLE SalesTeam;
   GRANT SELECT ON SCHEMA global_data_catalog.sales_domain TO ROLE SalesTeam;

   CREATE ROLE DataGovernanceTeam;
   GRANT SELECT, INSERT, UPDATE ON SCHEMA global_data_catalog.customer_domain TO ROLE DataGovernanceTeam;
   ```

2. **Implement Row-Level Security**
   ```sql
   -- Example of row-level security policies
   ```

### Task 5: Data Governance and Quality Enforcement

1. **Create Data Quality Rules**
   ```sql
   -- Ensure data quality with constraints and checks
   ```

2. **Monitor Data Quality**
   ```sql
   -- Query and validate data quality
   ```

### Task 6: Performance Optimization and Data Cleanup

1. **Optimize Delta Tables**
   ```sql
   OPTIMIZE global_data_catalog.sales_domain.sales;
   ```

2. **Clean Up Delta Tables**
   ```sql
   VACUUM global_data_catalog.sales_domain.sales;
   ```

---

## Databricks Notebooks and Pipelines

### Task 1: Raw Data Ingestion

1. **Create a Notebook for Data Ingestion**
   ```python
   # Notebook for raw data ingestion
   raw_data_path = "/mnt/raw_data/sales_data.csv"
   df = spark.read.format("csv").option("header", "true").load(raw_data_path)
   df.write.format("delta").save("/mnt/delta/sales_data")
   ```

2. **Run the Notebook**

### Task 2: Data Cleaning

1. **Create a Notebook for Data Cleaning**
   ```python
   # Notebook for data cleaning
   df = spark.read.format("delta").load("/mnt/delta/sales_data")
   df_clean = df.dropna().filter(df.SalesAmount > 0)
   df_clean.write.format("delta").mode("overwrite").save("/mnt/delta/clean_sales_data")
   ```

2. **Run the Notebook**

### Task 3: Data Transformation

1. **Create a Notebook for Data Transformation**
   ```python
   # Notebook for data transformation
   df_clean = spark.read.format("delta").load("/mnt/delta/clean_sales_data")
   df_transformed = df_clean.withColumn("SalesAmountUSD", df_clean.SalesAmount * 1.1)
   df_transformed.write.format("delta").mode("overwrite").save("/mnt/delta/transformed_sales_data")
   ```

2. **Run the Notebook**

### Task 4: Create a Pipeline to Execute Notebooks

1. **Create a New Pipeline in Databricks**

2. **Add Notebooks to the Pipeline**

3. **Configure Pipeline Triggers and Dependencies**

4. **Run and Monitor the Pipeline**

### Bonus Task: Error Handling

1. **Add Error Handling to Notebooks**
   ```python
   try:
       df = spark.read.format("csv").option("header", "true").load(raw_data_path)
       df.write.format("delta").save("/mnt/delta/sales_data")
   except Exception as e:
       print(f"Error: {e}")
   ```

2. **Monitor Pipeline Logs for Errors**

---

## Additional Tasks

### Task 1: Customer Data Ingestion

1. **Create a Notebook for Customer Data Ingestion**
   ```python
   # Ingest customer data
   customer_data_path = "/mnt/raw_data/customer_data.csv"
   df_customers = spark.read.format("csv").option("header", "true").load(customer_data_path)
   df_customers.write.format("delta").save("/mnt/delta/customer_data")
   ```

2. **Run the Notebook**

### Task 2: Data Cleaning

1. **Create a Notebook for Customer Data Cleaning**
   ```python
   # Clean customer data
   df_customers = spark.read.format("delta").load("/mnt/delta/customer_data")
   df_customers_clean = df_customers.dropna().filter(df_customers.Email.isNotNull())
   df_customers_clean.write.format("delta").mode("overwrite").save("/mnt/delta/clean_customer_data")
   ```

2. **Run the Notebook**

### Task 3: Data Aggregation

1. **Create a Notebook for Customer Data Aggregation**
   ```python
   # Aggregate customer data
   df_customers_clean = spark.read.format("delta").load("/mnt/delta/clean_customer_data")
   df_agg = df_customers_clean.groupBy("Country").count()
   df_agg.write.format("delta").mode("overwrite").save("/mnt/delta/agg_customer_data")
   ```

2. **Run the Notebook**

### Task 4: Pipeline Creation

1. **Create a New Pipeline for Customer Data**

2. **Add Notebooks for Ingestion, Cleaning, and Aggregation**

3. **Configure and Run the Pipeline**

### Task 5: Data Validation

1. **Create a Notebook for Data Validation**
   ```python
   # Validate customer data
   df_agg = spark.read.format("delta").load("/mnt/delta/agg_customer_data")
   df_agg.show()
   ```

2. **Run the Notebook**

### Task 1: Product Inventory Data Ingestion

1. **Create a Notebook for Product Inventory Data Ingestion**
   ```python
   # Ingest product inventory data
   product_data_path = "/mnt/raw

_data/product_data.csv"
   df_products = spark.read.format("csv").option("header", "true").load(product_data_path)
   df_products.write.format("delta").save("/mnt/delta/product_data")
   ```

2. **Run the Notebook**

### Task 2: Product Data Cleaning

1. **Create a Notebook for Product Data Cleaning**
   ```python
   # Clean product data
   df_products = spark.read.format("delta").load("/mnt/delta/product_data")
   df_products_clean = df_products.dropna().filter(df_products.ProductName.isNotNull())
   df_products_clean.write.format("delta").mode("overwrite").save("/mnt/delta/clean_product_data")
   ```

2. **Run the Notebook**

### Task 3: Product Data Aggregation

1. **Create a Notebook for Product Data Aggregation**
   ```python
   # Aggregate product data
   df_products_clean = spark.read.format("delta").load("/mnt/delta/clean_product_data")
   df_agg_products = df_products_clean.groupBy("Category").count()
   df_agg_products.write.format("delta").mode("overwrite").save("/mnt/delta/agg_product_data")
   ```

2. **Run the Notebook**

### Task 4: Pipeline Creation

1. **Create a New Pipeline for Product Data**

2. **Add Notebooks for Ingestion, Cleaning, and Aggregation**

3. **Configure and Run the Pipeline**

### Task 5: Data Validation

1. **Create a Notebook for Data Validation**
   ```python
   # Validate product data
   df_agg_products = spark.read.format("delta").load("/mnt/delta/agg_product_data")
   df_agg_products.show()
   ```

2. **Run the Notebook**

