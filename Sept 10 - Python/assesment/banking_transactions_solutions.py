# Banking Transactions Data Solutions

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, max, col, count, when

# Initialize Spark session
spark = SparkSession.builder.appName('BankingTransactions').getOrCreate()

# Load the dataset
data = spark.read.csv("/content/banking_transactions.csv", header=True, inferSchema=True)

# Exercise 1
# 1. Calculate the Total Deposit and Withdrawal Amounts
# Group the data by transaction_type and calculate the total amounts for both deposits and withdrawals.
total_amounts = data.groupBy("transaction_type").agg(sum("amount").alias("total_amount"))
print("Total deposit and withdrawal amounts:")
total_amounts.show()

# Exercise 2
# 2. Filter Transactions Greater Than $3,000
# Filter the dataset to show only transactions where the amount is greater than $3,000.
transactions_gt_3000 = data.filter(data["amount"] > 3000)
print("Transactions greater than $3,000:")
transactions_gt_3000.show()

# Exercise 3
# 3. Find the Largest Deposit Made
# Identify the transaction with the highest deposit amount.
largest_deposit = data.filter(data["transaction_type"] == "Deposit").orderBy(col("amount").desc()).limit(1)
print("Largest deposit made:")
largest_deposit.show()

# Exercise 4
# 4. Calculate the Average Transaction Amount for Each Transaction Type
# Group the data by transaction_type and calculate the average amount for deposits and withdrawals.
avg_amount_per_type = data.groupBy("transaction_type").agg(avg("amount").alias("avg_amount"))
print("Average transaction amount for each type:")
avg_amount_per_type.show()

# Exercise 5
# 5. Find Customers Who Made Both Deposits and Withdrawals
# Identify customers who have made at least one deposit and one withdrawal.
deposit_customers = data.filter(data["transaction_type"] == "Deposit").select("customer_id").distinct()
withdrawal_customers = data.filter(data["transaction_type"] == "Withdrawal").select("customer_id").distinct()
both_transactions_customers = deposit_customers.intersect(withdrawal_customers)
print("Customers who made both deposits and withdrawals:")
both_transactions_customers.show()

# Exercise 6
# 6. Calculate the Total Amount of Transactions per Day
# Group the data by transaction_date and calculate the total amount of all transactions for each day.
total_amount_per_day = data.groupBy("transaction_date").agg(sum("amount").alias("total_amount"))
print("Total amount of transactions per day:")
total_amount_per_day.show()

# Exercise 7
# 7. Find the Customer with the Highest Total Withdrawal
# Calculate the total amount withdrawn by each customer and identify the customer with the highest total withdrawal.
total_withdrawal_per_customer = data.filter(data["transaction_type"] == "Withdrawal") \
    .groupBy("customer_id").agg(sum("amount").alias("total_withdrawal")) \
    .orderBy(col("total_withdrawal").desc()).limit(1)
print("Customer with the highest total withdrawal:")
total_withdrawal_per_customer.show()

# Exercise 8
# 8. Calculate the Number of Transactions for Each Customer
# Group the data by customer_id and calculate the total number of transactions made by each customer.
transactions_per_customer = data.groupBy("customer_id").agg(count("transaction_id").alias("num_transactions"))
print("Number of transactions for each customer:")
transactions_per_customer.show()

# Exercise 9
# 9. Find All Transactions That Occurred on the Same Day as a Withdrawal Greater Than $1,000
# Filter the data to show all transactions that occurred on the same day as a withdrawal of more than $1,000.
withdrawal_gt_1000 = data.filter(data["transaction_type"] == "Withdrawal").filter(data["amount"] > 1000).select("transaction_date")
same_day_transactions = data.join(withdrawal_gt_1000, "transaction_date")
print("Transactions that occurred on the same day as a withdrawal greater than $1,000:")
same_day_transactions.show()

# Exercise 10
# 10. Create a New Column to Classify Transactions as "High" or "Low" Value
# Add a new column transaction_value that classifies a transaction as "High" if the amount is greater than $5,000, otherwise classify it as "Low."
data = data.withColumn("transaction_value", when(data["amount"] > 5000, "High").otherwise("Low"))
print("New column to classify transactions:")
data.show()
