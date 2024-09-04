import pandas as pd

# Creating a new dataset
data = {
    "Employee_ID": [101, 102, 103, 104, 105, 106],
    "Name": ["Rajesh", "Meena", "Suresh", "Anita", "Vijay", "Neeta"],
    "Department": ["HR", "IT", "Finance", "IT", "Finance", "HR"],
    "Age": [29, 35, 45, 32, 50, 28],
    "Salary": [70000, 85000, 95000, 64000, 120000, 72000],
    "City": ["Delhi", "Mumbai", "Bangalore", "Chennai", "Delhi", "Mumbai"]
}

df = pd.DataFrame(data)
print("Original DataFrame:")
print(df)

# Exercise 1: Rename Columns
df.rename(columns={"Salary": "Annual Salary", "City": "Location"}, inplace=True)
print("\nDataFrame after renaming columns:")
print(df)

# Exercise 2: Drop Columns
df.drop(columns=["Location"], inplace=True)
print("\nDataFrame after dropping 'Location' column:")
print(df)

# Exercise 3: Drop Rows
df = df[df["Name"] != "Suresh"]
print("\nDataFrame after dropping the row where 'Name' is 'Suresh':")
print(df)

# Exercise 4: Handle Missing Data
df.loc[df["Name"] == "Meena", "Annual Salary"] = None
mean_salary = df["Annual Salary"].mean()
df["Annual Salary"].fillna(mean_salary, inplace=True)
print("\nDataFrame after handling missing data:")
print(df)

# Exercise 5: Create Conditional Columns
df["Seniority"] = df["Age"].apply(lambda x: "Senior" if x >= 40 else "Junior")
print("\nDataFrame after adding 'Seniority' column:")
print(df)

# Exercise 6: Grouping and Aggregation
grouped_df = df.groupby("Department")["Annual Salary"].mean().reset_index()
print("\nGrouped DataFrame by 'Department' with average salary:")
print(grouped_df)
