# Bonus Task: Error Handling 

### Mini Project: Building a Secure Data Platform with Unity Catalog and Delta Lake Pipelines

---

## **Task 1: Set Up Unity Catalog for Multi-Domain Data Management**

### **1. Create a New Catalog:**
- **Objective:** Create a catalog named `enterprise_data_catalog`.
    ```sql
    CREATE CATALOG enterprise_data_catalog;
    ```

### **2. Create Domain-Specific Schemas:**
- **Objective:** Set up domain-specific schemas within the catalog.
    ```sql
    USE CATALOG enterprise_data_catalog;

    CREATE SCHEMA marketing_data;
    CREATE SCHEMA operations_data;
    CREATE SCHEMA it_data;
    ```

### **3. Create Tables in Each Schema:**
- **Objective:** Create tables for each domain-specific schema.

    **Marketing Data Table:**
    ```sql
    CREATE TABLE marketing_data.campaigns (
        CampaignID INT,
        CampaignName STRING,
        Budget DOUBLE,
        StartDate DATE
    );
    ```

    **Operations Data Table:**
    ```sql
    CREATE TABLE operations_data.orders (
        OrderID INT,
        ProductID INT,
        Quantity INT,
        ShippingStatus STRING
    );
    ```

    **IT Data Table:**
    ```sql
    CREATE TABLE it_data.incidents (
        IncidentID INT,
        ReportedBy STRING,
        IssueType STRING,
        ResolutionTime DOUBLE
    );
    ```

---

## **Task 2: Data Discovery and Classification**

### **1. Search for Data Across Schemas:**
- **Objective:** List all tables in the catalog and search for specific columns (e.g., `Budget`, `ResolutionTime`).
    ```sql
    SHOW TABLES FROM enterprise_data_catalog;
    
    -- Search for tables based on column types
    SELECT * FROM enterprise_data_catalog.INFORMATION_SCHEMA.COLUMNS
    WHERE COLUMN_NAME IN ('Budget', 'ResolutionTime');
    ```

### **2. Tag Sensitive Information:**
- **Objective:** Tag sensitive columns like `Budget` and `ResolutionTime`.
    ```sql
    ALTER TABLE marketing_data.campaigns ALTER COLUMN Budget SET TAG 'sensitive';
    ALTER TABLE it_data.incidents ALTER COLUMN ResolutionTime SET TAG 'sensitive';
    ```

### **3. Data Profiling:**
- **Objective:** Perform basic profiling on marketing budgets and operational shipping statuses.
    ```sql
    SELECT AVG(Budget) FROM marketing_data.campaigns;
    SELECT COUNT(*) FROM operations_data.orders WHERE ShippingStatus = 'Shipped';
    ```

---

## **Task 3: Data Lineage and Auditing**

### **1. Track Data Lineage Across Schemas:**
- **Objective:** Link marketing and operations data, tracking lineage.
    ```sql
    SELECT o.OrderID, m.CampaignName, o.ProductID
    FROM operations_data.orders o
    JOIN marketing_data.campaigns m ON o.ProductID = m.CampaignID;
    ```

    Use Unity Catalog for tracking lineage automatically.

### **2. Enable and Analyze Audit Logs:**
- **Objective:** Enable audit logging for the IT schema and review access logs.
    ```sql
    -- Enable audit logs
    ALTER SCHEMA it_data SET AUDIT LOGGING = 'ENABLED';
    
    -- Query logs for access analysis
    SELECT * FROM INFORMATION_SCHEMA.AUDIT_LOGS
    WHERE schema_name = 'it_data';
    ```

---

## **Task 4: Implement Fine-Grained Access Control**

### **1. Create User Roles and Groups:**
- **Objective:** Set up roles and assign schema-level permissions.
    ```sql
    CREATE ROLE MarketingTeam;
    CREATE ROLE OperationsTeam;
    CREATE ROLE ITSupportTeam;

    -- Grant access to schemas
    GRANT SELECT ON SCHEMA marketing_data TO ROLE MarketingTeam;
    GRANT SELECT ON SCHEMA operations_data TO ROLE OperationsTeam;
    GRANT UPDATE ON SCHEMA it_data TO ROLE ITSupportTeam;
    ```

### **2. Implement Column-Level Security:**
- **Objective:** Restrict access to the `Budget` column.
    ```sql
    GRANT SELECT (CampaignID, CampaignName, StartDate) ON TABLE marketing_data.campaigns TO ROLE OperationsTeam;
    ```

### **3. Row-Level Security:**
- **Objective:** Restrict access to rows in operations_data based on department.
    ```sql
    -- Row-level security rule
    CREATE VIEW operations_data.secure_orders AS
    SELECT * FROM operations_data.orders
    WHERE department IN (SELECT department FROM user_department);
    ```

---

## **Task 5: Data Governance and Quality Enforcement**

### **1. Set Data Quality Rules:**
- **Objective:** Implement data validation for each schema.
    ```sql
    -- Ensure budget is positive
    ALTER TABLE marketing_data.campaigns ADD CONSTRAINT chk_budget CHECK (Budget > 0);
    
    -- Ensure valid shipping status
    ALTER TABLE operations_data.orders ADD CONSTRAINT chk_shipping CHECK (ShippingStatus IN ('Pending', 'Shipped', 'Delivered'));

    -- Ensure resolution time is non-negative
    ALTER TABLE it_data.incidents ADD CONSTRAINT chk_resolution CHECK (ResolutionTime >= 0);
    ```

### **2. Apply Delta Lake Time Travel:**
- **Objective:** Roll back the operations schema if required.
    ```sql
    -- View Delta Lake history
    DESCRIBE HISTORY operations_data.orders;
    
    -- Restore to a previous version
    RESTORE TABLE operations_data.orders VERSION AS OF 3;
    ```

---

## **Task 6: Performance Optimization and Data Cleanup**

### **1. Optimize Delta Tables:**
- **Objective:** Optimize frequent queries on Delta tables.
    ```sql
    OPTIMIZE operations_data.orders ZORDER BY (ShippingStatus);
    OPTIMIZE it_data.incidents ZORDER BY (IssueType);
    ```

### **2. Vacuum Delta Tables:**
- **Objective:** Run VACUUM to clean up old versions.
    ```sql
    VACUUM operations_data.orders RETAIN 168 HOURS;
    ```

---

## **Additional Tasks: Pipeline for Data Ingestion, Cleaning, and Transformation**

### **Pipeline for Weather Data:**

1. **Raw Data Ingestion:**
   - Read CSV data into Delta table.
   ```python
   df = spark.read.format("csv").option("header", "true").load("/path/to/weather.csv")
   df.write.format("delta").save("/delta/weather_raw")
   ```

2. **Data Cleaning:**
   - Remove rows with missing values.
   ```python
   cleaned_df = df.na.drop()
   cleaned_df.write.format("delta").save("/delta/weather_cleaned")
   ```

3. **Data Transformation:**
   - Calculate average temperature per city.
   ```python
   transformed_df = cleaned_df.groupBy("City").agg(avg("Temperature").alias("AvgTemperature"))
   transformed_df.write.format("delta").save("/delta/weather_transformed")
   ```

4. **Pipeline Creation:**
   - Create a pipeline to execute the notebooks in sequence and handle errors.

   ```python
   try:
       # Run raw data ingestion
       dbutils.notebook.run("weather_ingestion", timeout_seconds=300)
       # Run data cleaning
       dbutils.notebook.run("weather_cleaning", timeout_seconds=300)
       # Run data transformation
       dbutils.notebook.run("weather_transformation", timeout_seconds=300)
   except Exception as e:
       dbutils.fs.put("/logs/error_log.txt", str(e), overwrite=True)
   ```
