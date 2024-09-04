New! Keyboard shortcuts … Drive keyboard shortcuts have been updated to give you first-letters navigation
import numpy as np
import pandas as pd

# ### Exercise 1: Creating DataFrame from Scratch
# 1. Create a DataFrame with the following columns: "Product", "Category", "Price", and "Quantity". Use the following data:
#    - Product: ['Laptop', 'Mouse', 'Monitor', 'Keyboard', 'Phone']
#    - Category: ['Electronics', 'Accessories', 'Electronics', 'Accessories', 'Electronics']
#    - Price: [80000, 1500, 20000, 3000, 40000]
#    - Quantity: [10, 100, 50, 75, 30]
# 2. Print the DataFrame.
data={ "Product":['Laptop', 'Mouse', 'Monitor', 'Keyboard', 'Phone'],
    "Category":['Electronics', 'Accessories', 'Electronics', 'Accessories', 'Electronics'],
    "Price":[80000, 1500, 20000, 3000, 40000],
    "Quantity":[10, 100, 50, 75, 30]}
df=pd.DataFrame(data)
print(df)

### Exercise 2: Basic DataFrame Operations
# 1. Display the first 3 rows of the DataFrame.
# 2. Display the column names and index of the DataFrame.
# 3. Display a summary of statistics (mean, min, max, etc.) for the numeric columns in the DataFrame.
print(df.head(3))
print(df.info())
print(df.describe())


# ### Exercise 3: Selecting Data
# 1. Select and display the "Product" and "Price" columns.
# 2. Select rows where the "Category" is "Electronics" and print them.
print(df[["Product","Price"]])
filer_df=df[df["Category"]=="Electronics"]
print(filer_df)

### Exercise 4: Filtering Data
# 1. Filter the DataFrame to display only the products with a price greater than 10,000.
# 2. Filter the DataFrame to show only products that belong to the "Accessories" category and have a quantity greater than 50.
filer_df_price=df[df["Price"]>=10000]
print(filer_df_price)
filer_df_accessories=df[(df["Category"]=="Accessories") & (df["Quantity"]>=50)]
print(filer_df_accessories)

### Exercise 5: Adding and Removing Columns
# 1. Add a new column "Total Value" which is calculated by multiplying "Price" and "Quantity".
# 2. Drop the "Category" column from the DataFrame and print the updated DataFrame.
df["Total Value"]=df["Price"]*df["Quantity"]
print(df)
df=df.drop(columns=["Category"])
print(df)

### Exercise 6: Sorting Data
# 1. Sort the DataFrame by "Price" in descending order.
# 2. Sort the DataFrame by "Quantity" in ascending order, then by "Price" in descending order (multi-level sorting).
print(df.sort_values(by="Price",ascending=False))
print(df.sort_values(by=["Quantity","Price"],ascending=[True,False]))

### Exercise 7: Grouping Data
# 1. Group the DataFrame by "Category" and calculate the total quantity for each category.
# 2. Group by "Category" and calculate the average price for each category.
df["Category"]=['Electronics', 'Accessories', 'Electronics', 'Accessories', 'Electronics']
grouped_df=df.groupby("Category")["Quantity"].sum()
grouped_df_average=df.groupby("Category")["Quantity"].mean()
print(grouped_df)
print(grouped_df_average)

### Exercise 8: Handling Missing Data
# 1. Introduce some missing values in the "Price" column by assigning None to two rows.
# 2. Fill the missing values with the mean price of the available products.
# 3. Drop any rows where the "Quantity" is less than 50.
df.loc[0, "Price"] = None
df.loc[4, "Price"] = None
mean_price=df["Price"].mean()
# print(mean_price)
df["Price"]=df["Price"].fillna(mean_price)
print(df)
df.drop(columns=["Price"])
df=df[df["Quantity"]>=50]
print(df)

### Exercise 9: Apply Custom Functions
# 1. Apply a custom function to the "Price" column that increases all prices by 5%.
# 2. Create a new column "Discounted Price" that reduces the original price by 10%.
def increase_price(price):
    return price*1.05
df["Price"]=df["Price"].apply(increase_price)
def discount_price(price):
    return price*0.9
df["Discounted Price"]=df["Price"].apply(discount_price)
print(df)

### Exercise 10: Merging DataFrames
# 1. Create another DataFrame with columns "Product" and "Supplier", and merge it with the original DataFrame based on the "Product" column.
data={
    "Product":['Mouse', 'Monitor', 'Keyboard'],
    "Supplier":['Market','Market','Market']}
df_new=pd.DataFrame(data)
df_new.merge(df)
df_merge=pd.merge(df,df_new,on="Product",how="left")
print(df_merge)


# Exercise-11: Pivot tables
# Create a pivot table that shows the total quantity of products
# for each category and product combination
pivot_table = df.pivot_table(values='Quantity', index='Category', columns='Product', aggfunc='sum')
print(pivot_table)

### Exercise 12: Concatenating DataFrames
# 1. Create two separate DataFrames for two different stores with the same columns ("Product", "Price", "Quantity").
# 2. Concatenate these DataFrames to create a combined inventory list.

data_store1 = {
    'Product': ['Laptop', 'Monitor', 'Mouse'],
    'Price': [1000, 700, 300],
    'Quantity': [5, 10, 15]}
df_store1 = pd.DataFrame(data_store1)
data_store2 = {
    'Product': ['Headphones', 'Charger', 'Smartphone'],
    'Price': [100, 50, 800],
    'Quantity': [60, 20, 8]
}

df_store2 = pd.DataFrame(data_store2)
df_combined=pd.concat([df_store1,df_store2],ignore_index=True)
print(df_combined)

### Exercise 13: Working with Dates
# 1. Create a DataFrame with a "Date" column that contains the last 5 days starting from today.
# 2. Add a column "Sales" with random values for each day.
# 3. Find the total sales for all days combined.
from datetime import datetime,timedelta
dates = [datetime.today() - timedelta(days=i) for i in range(5)]
sales = [2500, 3400, 4100, 2900, 4700]
df_dates = pd.DataFrame({'Date': dates, 'Sales': sales})
print(df_dates)
# Find the total sales for all days combined
total_sales = df_dates['Sales'].sum()
print(f'Total Sales: {total_sales}')

### Exercise 14: Reshaping Data with Melt
# 1. Create a DataFrame with columns "Product", "Region", "Q1_Sales", "Q2_Sales".
# 2. Use pd.melt() to reshape the DataFrame so that it has columns "Product", "Region", "Quarter", and "Sales".
df_sales=pd.DataFrame({
    'Product': ['Laptop','Phone','Monitor'],
    'Region': ['North', 'South', 'West'],
    'Q1_Sales': [15000, 30000, 25000],
    'Q2_Sales': [18000, 32000, 27000]
})
# Using pd.melt() to reshape the DataFrame
df_melted = pd.melt(df_sales,
            id_vars=['Product', 'Region'],
            value_vars=['Q1_Sales', 'Q2_Sales'],
            var_name='Quarter', value_name='Sales')
print(df_melted)

### *Exercise 15: Reading and Writing Data*
#1. Read the data from a CSV file named products.csv into a DataFrame.
#2. After performing some operations (e.g., adding a new column or modifying values), write the DataFrame back to a new CSV file named updated_products.csv.

# Read data from a CSV file named products.csv into a DataFrame
df_csv = pd.read_csv('products.csv')
print(df_csv)

# Add a new column or modify values
df_csv['Discounted Price'] = df_csv['Price'] * 0.9

# Write the DataFrame back to a new CSV file named updated_products.csv
df_csv.to_csv('updated_products.csv', index=False)

### Exercise 16: Renaming Columns
# 1. Given a DataFrame with columns "Prod", "Cat", "Price", "Qty", rename the columns to "Product", "Category", "Price", and "Quantity".
# 2. Print the renamed DataFrame.

data = {
    "Prod": ["A", "B", "C"],
    "Cat": ["X", "Y", "Z"],
    "Price": [100, 200, 300],
    "Qty": [10, 20, 30]
}
df = pd.DataFrame(data)
df.columns = ["Product", "Category", "Price", "Quantity"]
print(df)


### Exercise 17: Creating a MultiIndex DataFrame
# 1. Create a DataFrame using a MultiIndex (hierarchical index) with two levels: "Store" and "Product". The DataFrame should have columns "Price" and "Quantity", representing the price and quantity of products in different stores.
# 2. Print the MultiIndex DataFrame.

index = pd.MultiIndex.from_tuples([('Store1', 'Product A'), ('Store1', 'Product B'),
                                   ('Store2', 'Product A'), ('Store2', 'Product B')],
                                  names=['Store', 'Product'])
data = {
    "Price": [100, 150, 200, 250],
    "Quantity": [50, 75, 100, 125]
}
df = pd.DataFrame(data, index=index)
print(df)


### Exercise 18: Resample Time-Series Data
# 1. Create a DataFrame with a "Date" column containing a range of dates for the past 30 days and a "Sales" column with random values.
# 2. Resample the data to show the total sales by week.

dates = pd.date_range(start='2024-08-01', end='2024-08-30')
sales = [250, 300, 150, 400, 500, 600, 700, 200, 450, 350,
         800, 900, 500, 750, 650, 300, 550, 450, 600, 700,
         800, 250, 500, 600, 700, 800, 900, 1000, 950, 850]
df_time = pd.DataFrame({'Date': dates, 'Sales': sales})
# Resample the data to show total sales by week
df_weekly_sales = df_time.set_index('Date').resample('W').sum()
print(df_weekly_sales)


### Exercise 19: Handling Duplicates
# 1. Given a DataFrame with duplicate rows, identify and remove the duplicate rows.
# 2. Print the cleaned DataFrame.

data = {
    "Product": ["A", "B", "C", "A"],
    "Price": [100, 200, 300, 100],
    "Quantity": [50, 75, 100, 50]
}
df = pd.DataFrame(data)

# Remove duplicate rows based on all columns
df = df.drop_duplicates()
print(df)


### Exercise 20: Correlation Matrix
# 1. Create a DataFrame with numeric data representing different features (e.g., "Height", "Weight", "Age", "Income").
# 2. Compute the correlation matrix for the DataFrame.
# 3. Print the correlation matrix.
data = { "Height": [170, 180, 165, 175],
    "Weight": [70, 80, 65, 75],
    "Age": [25, 30, 28, 27],
    "Income": [50000, 60000, 45000, 55000]}
df = pd.DataFrame(data)

# Compute the correlation matrix
correlation_matrix = df.corr()

print(correlation_matrix)


### Exercise 21: Cumulative Sum and Rolling Windows
# 1. Create a DataFrame with random sales data for each day over the last 30 days.
# 2. Calculate the cumulative sum of the sales and add it as a new column "Cumulative Sales".
# 3. Calculate the rolling average of sales over the past 7 days and add it as a new column "Rolling Avg".

dates = pd.date_range(start='2024-08-01', periods=30)
sales = [200, 300, 400, 250, 150, 350, 450, 500, 550, 600,
         650, 700, 750, 800, 850, 900, 950, 1000, 1050, 1100,
         1150, 1200, 1250, 1300, 1350, 1400, 1450, 1500, 1550, 1600]
df_sales = pd.DataFrame({'Date': dates, 'Sales': sales})
# Calculate the cumulative sum
df_sales['Cumulative Sales'] = df_sales['Sales'].cumsum()
# Calculate the rolling average of sales over the past 7 days
df_sales['Rolling Avg'] = df_sales['Sales'].rolling(window=7).mean()
print(df_sales)

### Exercise 22: String Operations
# 1. Create a DataFrame with a column "Names" containing values like "John Doe", "Jane Smith", "Sam Brown".
# 2. Split the "Names" column into two separate columns: "First Name" and "Last Name".
# 3. Convert the "First Name" column to uppercase.

data = {"Names": ["John Doe", "Jane Smith", "Sam Brown"]}
df = pd.DataFrame(data)
df[["First Name", "Last Name"]] = df["Names"].str.split(expand=True)
df["First Name"] = df["First Name"].str.upper()
print(df)


# Exercise-23: Conditional Selections with np.where
# Create a DataFrame with employee data
df_employee = pd.DataFrame({
    'Employee': ['John', 'Jane', 'Sam', 'Emily'],
    'Age': [45, 34, 29, 41],
    'Department': ['HR', 'Finance', 'IT', 'Marketing']})
# Create a new column "Status" using conditional selections
df_employee['Status'] = ['Senior' if age >= 40 else 'Junior' for age in df_employee['Age']]
print(df_employee)


### Exercise 24: Slicing DataFrames
# 1. Given a DataFrame with data on "Products", "Category", "Sales", and "Profit", slice the DataFrame to display:
#    - The first 10 rows.
#    - All rows where the "Category" is "Electronics".
#    - Only the "Sales" and "Profit" columns for products with sales greater than 50,000.

data = {
    "Products": ["Laptop", "Smartphone", "TV", "Camera", "Headphones"],
    "Category": ["Electronics", "Electronics", "Electronics", "Electronics", "Electronics"],
    "Sales": [100000, 80000, 70000, 50000, 20000],
    "Profit": [30000, 20000, 15000, 10000, 5000]}
df = pd.DataFrame(data)
first_10_rows = df.head(10)
electronics_category = df[df["Category"] == "Electronics"]
high_sales_data = df[df["Sales"] > 50000][["Sales", "Profit"]]
print("First 10 rows:")
print(first_10_rows)
print("\nElectronics category:")
print(electronics_category)
print("\nHigh sales data:")
print(high_sales_data)


### Exercise 25: Concatenating DataFrames Vertically and Horizontally
# 1. Create two DataFrames with identical columns "Employee", "Age", "Salary", but different rows (e.g., one for employees in "Store A" and one for employees in "Store B").
# 2. Concatenate the DataFrames vertically to create a combined DataFrame.
# 3. Now create two DataFrames with different columns (e.g., "Employee", "Department" and "Employee", "Salary") and concatenate them horizontally based on the common "Employee" column.

df1 = pd.DataFrame({
    "Employee": ["Alice", "Bob", "Charlie"],
    "Age": [30, 35, 28],
    "Salary": [50000, 60000, 45000]})
df2 = pd.DataFrame({
    "Employee": ["David", "Emily", "Frank"],
    "Age": [25, 32, 40],
    "Salary": [40000, 55000, 70000]})
combined_df = pd.concat([df1, df2])
print("Vertically concatenated DataFrame:")
print(combined_df)
df3 = pd.DataFrame({
"Employee": ["Alice", "Bob", "Charlie"],
"Department": ["Sales", "HR", "IT"]})
df4 = pd.DataFrame({
    "Employee": ["Alice", "Bob", "Charlie"],
    "Salary": [50000, 60000, 45000]})
merged_df = pd.merge(df3, df4, on="Employee")
print("\nHorizontally concatenated DataFrame:")
print(merged_df)


### Exercise 26: Exploding Lists in DataFrame Columns
# 1. Create a DataFrame with a column "Product" and a column "Features" where each feature is a list (e.g., ["Feature1", "Feature2"]).
# 2. Use the explode() method to create a new row for each feature in the list, so each product-feature pair has its own row.
data = {
    "Product": ["Product A", "Product B"],
    "Features": [["Feature1", "Feature2"], ["Feature3", "Feature4"]]
}
df = pd.DataFrame(data)

# Use the explode() method to create a new row for each feature
exploded_df = df.explode("Features")
print(exploded_df)

### *Exercise 27: Using .map() and .applymap()*
# 1. Given a DataFrame with columns "Product", "Price", and "Quantity", use .map() to apply a custom function to increase "Price" by 10% for each row.
# 2. Use .applymap() to format the numeric values in the DataFrame to two decimal places.
data = { "Product": ["Item1", "Item2", "Item3"],
    "Price": [100, 120, 150],
    "Quantity": [5, 8, 12]}
df = pd.DataFrame(data)
def increase_price(price):
    return price * 1.1
df["Price"] = df["Price"].map(increase_price)
df = df.applymap(lambda x: round(x, 2) if isinstance(x, float) else x)
print(df)

### *Exercise 28: Combining groupby() with apply()*
# 1. Create a DataFrame with "City", "Product", "Sales", and "Profit".
# 2. Group by "City" and apply a custom function to calculate the profit margin (Profit/Sales) for each city.

data = {"City": ["New York", "Los Angeles", "Chicago", "New York", "Los Angeles"],
    "Product": ["Product A", "Product B", "Product C", "Product D", "Product E"],
    "Sales": [10000, 8000, 12000, 5000, 7000],
    "Profit": [3000, 2500, 4000, 1500, 2000]}
df = pd.DataFrame(data)
def calculate_profit_margin(group):
    return group["Profit"].sum() / group["Sales"].sum()
profit_margins = df.groupby("City").apply(calculate_profit_margin)
print(profit_margins)

#Exercise 29
import json
# Create three different DataFrames from different sources (CSV, JSON, Dictionary)
df_csv = pd.read_csv('products.csv')
json_data = '{"Product": ["Laptop", "Mouse"], "Supplier": ["Supplier A", "Supplier B"]}'
df_json = pd.read_json(json_data)

df_dict = pd.DataFrame({
    "Product": ["Laptop", "Mouse", "Monitor"],
    "Quantity": [10, 100, 50]
})

# Merge the DataFrames based on a common column and create a consolidated report
df_merged = pd.merge(df_csv, df_json, on="Product", how="left")
df_final = pd.merge(df_merged, df_dict, on="Product", how="left")
print(df_final)


#Exercise 30
# Create a large DataFrame with 1 million rows
large_data = {
    "Transaction ID": range(1, 1000001),
    "Customer": ['Customer ' + str(i) for i in range(1, 1000001)],
    "Product": ['Product ' + str(i % 100) for i in range(1, 1000001)],
    "Amount": np.random.randint(100, 10000, size=1000000),
    "Date": pd.date_range(start='2023-01-01', periods=1000000, freq='T')}

df_large = pd.DataFrame(large_data)
# Split the DataFrame into smaller chunks (e.g., 100,000 rows each)
chunks = [df_large.iloc[i:i + 100000] for i in range(0, len(df_large), 100000)]

# Perform a simple analysis on each chunk (e.g., total sales) and combine the results
total_sales = sum(chunk['Amount'].sum() for chunk in chunks)
print(f"Total Sales: {total_sales}")
