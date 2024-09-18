# Mini Project Using Unity Catalog and Data Governance

## Objective
Develop a mini project using Unity Catalog to demonstrate key data governance capabilities such as Data Discovery, Data Audit, Data Lineage, and Access Control.

---

## Part 1: Setting Up the Environment

### Task 1: Create a Metastore
Set up a Unity Catalog metastore that will act as the central location to manage all catalogs and schemas.

```sql
-- Create a Unity Catalog Metastore
-- This step is done via the Databricks admin console
-- Navigate to Unity Catalog > Create Metastore
```

### Task 2: Create Department-Specific Catalogs
Create separate catalogs for the following departments:

1. **Marketing**
2. **Engineering**
3. **Operations**

```sql
CREATE CATALOG marketing;
CREATE CATALOG engineering;
CREATE CATALOG operations;
```

### Task 3: Create Schemas for Each Department
Inside each catalog, create specific schemas to store different types of data:

- **Marketing Catalog:**
  - Schemas: `ads_data`, `customer_data`
  
- **Engineering Catalog:**
  - Schemas: `projects`, `development_data`
  
- **Operations Catalog:**
  - Schemas: `logistics_data`, `supply_chain`

```sql
-- Marketing Catalog
CREATE SCHEMA marketing.ads_data;
CREATE SCHEMA marketing.customer_data;

-- Engineering Catalog
CREATE SCHEMA engineering.projects;
CREATE SCHEMA engineering.development_data;

-- Operations Catalog
CREATE SCHEMA operations.logistics_data;
CREATE SCHEMA operations.supply_chain;
```

---

## Part 2: Loading Data and Creating Tables

### Task 4: Prepare Datasets
Create CSV files for each schema with sample data.

#### Marketing - Ads Data

`marketing_ads_data.csv`

```csv
ad_id,impressions,clicks,cost_per_click
1,1000,50,0.5
2,2000,100,0.4
3,1500,75,0.45
```

#### Engineering - Projects

`engineering_projects.csv`

```csv
project_id,project_name,start_date,end_date
1,Project Alpha,2024-01-01,2024-06-30
2,Project Beta,2024-02-01,2024-07-31
3,Project Gamma,2024-03-01,2024-08-31
```

#### Operations - Logistics

`operations_logistics.csv`

```csv
shipment_id,origin,destination,status
1,New York,Los Angeles,Delivered
2,Chicago,Seattle,In Transit
3,Houston,San Francisco,Pending
```

### Task 5: Create Tables from the Datasets
Load the datasets into their respective schemas as tables.

1. **Create a table for `ads_data` in the Marketing catalog:**

   ```sql
   CREATE TABLE marketing.ads_data.ads (
       ad_id INT,
       impressions INT,
       clicks INT,
       cost_per_click FLOAT
   );
   ```

2. **Create a table for `projects` in the Engineering catalog:**

   ```sql
   CREATE TABLE engineering.projects.projects (
       project_id INT,
       project_name STRING,
       start_date DATE,
       end_date DATE
   );
   ```

3. **Create a table for `logistics` in the Operations catalog:**

   ```sql
   CREATE TABLE operations.logistics_data.logistics (
       shipment_id INT,
       origin STRING,
       destination STRING,
       status STRING
   );
   ```

### Load Data into Tables

```sql
-- Load data into ads_data table
COPY INTO marketing.ads_data.ads
FROM 'path_to_csv/marketing_ads_data.csv'
FORMAT CSV;

-- Load data into projects table
COPY INTO engineering.projects.projects
FROM 'path_to_csv/engineering_projects.csv'
FORMAT CSV;

-- Load data into logistics table
COPY INTO operations.logistics_data.logistics
FROM 'path_to_csv/operations_logistics.csv'
FORMAT CSV;
```

---

## Part 3: Data Governance Capabilities

### Data Access Control

#### Task 6: Create Roles and Grant Access
Create specific roles for each department and grant access to the relevant catalogs and schemas.

```sql
-- Create Roles
CREATE ROLE marketing_role;
CREATE ROLE engineering_role;
CREATE ROLE operations_role;

-- Grant Access
GRANT USAGE ON CATALOG marketing TO ROLE marketing_role;
GRANT USAGE ON SCHEMA marketing.ads_data TO ROLE marketing_role;

GRANT USAGE ON CATALOG engineering TO ROLE engineering_role;
GRANT USAGE ON SCHEMA engineering.projects TO ROLE engineering_role;

GRANT USAGE ON CATALOG operations TO ROLE operations_role;
GRANT USAGE ON SCHEMA operations.logistics_data TO ROLE operations_role;
```

#### Task 7: Configure Fine-Grained Access Control
Set up fine-grained access control to ensure users can only access data relevant to their role.

```sql
-- Example of Fine-Grained Access Control
-- Marketing Role can access only customer-related data
GRANT SELECT ON TABLE marketing.customer_data TO ROLE marketing_role;

-- Engineering Role can access only project data
GRANT SELECT ON TABLE engineering.projects TO ROLE engineering_role;
```

### Data Lineage

#### Task 8: Enable and Explore Data Lineage
Enable data lineage for the tables created and examine how data lineage traces data origins and transformations.

```sql
-- Data lineage is explored through the Databricks UI
-- Navigate to Catalog Explorer to view the data lineage
```

### Data Audit

#### Task 9: Monitor Data Access and Modifications
Set up audit logging to track data access and modifications.

```sql
-- Audit logging setup is done through the Databricks admin console
-- Navigate to Audit Logs to view access patterns and modifications
```

### Data Discovery

#### Task 10: Explore Metadata in Unity Catalog
Explore the metadata of the tables created and document information such as table schema, number of rows, and table properties.

```sql
-- Example of exploring table metadata
DESCRIBE TABLE marketing.ads_data.ads;
DESCRIBE TABLE engineering.projects.projects;
DESCRIBE TABLE operations.logistics_data.logistics;
```
