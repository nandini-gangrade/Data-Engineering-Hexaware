# Data Governance using Unity Catalog

### Task 1: Set Up Unity Catalog Objects with Multiple Schemas

#### 1. Create a Catalog
```sql
CREATE CATALOG finance_data_catalog;
```

#### 2. Create Multiple Schemas
```sql
CREATE SCHEMA finance_data_catalog.customer_data;
CREATE SCHEMA finance_data_catalog.transaction_data;
```

#### 3. Create Tables in Each Schema
```sql
-- For transaction_data schema
CREATE TABLE finance_data_catalog.transaction_data.transactions (
    TransactionID INT,
    CustomerID INT,
    TransactionAmount DECIMAL(10, 2),
    TransactionDate DATE
);

-- For customer_data schema
CREATE TABLE finance_data_catalog.customer_data.customers (
    CustomerID INT,
    CustomerName STRING,
    Email STRING,
    Country STRING
);
```

---

### Task 2: Data Discovery Across Schemas

#### 1. Explore Metadata
```sql
-- Retrieve metadata for tables in both schemas
SHOW TABLES IN finance_data_catalog.customer_data;
SHOW TABLES IN finance_data_catalog.transaction_data;
```

#### 2. Data Profiling
```sql
-- Example of data profiling queries
SELECT AVG(TransactionAmount) AS AvgTransactionAmount FROM finance_data_catalog.transaction_data.transactions;
SELECT COUNT(DISTINCT Country) AS UniqueCountries FROM finance_data_catalog.customer_data.customers;
```

#### 3. Tagging Sensitive Data
```sql
-- Apply tags to sensitive columns (example syntax may vary)
ALTER TABLE finance_data_catalog.customer_data.customers 
    ADD TAG sensitive_column('Email');

ALTER TABLE finance_data_catalog.transaction_data.transactions 
    ADD TAG sensitive_column('TransactionAmount');
```

---

### Task 3: Implement Data Lineage and Auditing

#### 1. Track Data Lineage
```sql
-- Merging data to generate a comprehensive view
CREATE OR REPLACE VIEW finance_data_catalog.comprehensive_view AS
SELECT 
    t.TransactionID, 
    c.CustomerName, 
    t.TransactionAmount, 
    t.TransactionDate 
FROM 
    finance_data_catalog.transaction_data.transactions t
JOIN 
    finance_data_catalog.customer_data.customers c 
ON 
    t.CustomerID = c.CustomerID;

-- Trace data lineage (may require specific Unity Catalog commands)
```

#### 2. Audit User Actions
```sql
-- Enable audit logs (consult Unity Catalog documentation for specific commands)
```

---

### Task 4: Access Control and Permissions

#### 1. Set Up Roles and Groups
```sql
-- Create roles
CREATE ROLE DataEngineers;
CREATE ROLE DataAnalysts;

-- Assign permissions
GRANT ALL PRIVILEGES ON SCHEMA finance_data_catalog.customer_data TO DataEngineers;
GRANT ALL PRIVILEGES ON SCHEMA finance_data_catalog.transaction_data TO DataEngineers;

GRANT SELECT ON SCHEMA finance_data_catalog.customer_data TO DataAnalysts;
GRANT SELECT ON finance_data_catalog.transaction_data.transactions TO DataAnalysts; -- restricted access
```

#### 2. Row-Level Security
```sql
-- Implement row-level security (syntax may vary)
CREATE ROW ACCESS POLICY high_value_policy AS 
    (TransactionAmount > 10000); -- Example threshold
```

---

### Task 5: Data Governance Best Practices

#### 1. Create Data Quality Rules
```sql
-- Example quality checks
SELECT * FROM finance_data_catalog.transaction_data.transactions 
WHERE TransactionAmount < 0;

SELECT * FROM finance_data_catalog.customer_data.customers 
WHERE Email NOT LIKE '%_@__%.__%';
```

#### 2. Validate Data Governance
```sql
-- Check lineage and audit logs
```

---

### Task 6: Data Lifecycle Management

#### 1. Implement Time Travel
```sql
-- Access historical versions (example syntax)
SELECT * FROM finance_data_catalog.transaction_data.transactions VERSION AS OF 0; -- Adjust version as needed
```

#### 2. Run a Vacuum Operation
```sql
VACUUM finance_data_catalog.transaction_data.transactions;
VACUUM finance_data_catalog.customer_data.customers;
```
