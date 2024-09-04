New! Keyboard shortcuts … Drive keyboard shortcuts have been updated to give you first-letters navigation
import json
import csv

# Exercise 1: Reading a JSON File
with open('data.json', 'r') as file:
    data = json.load(file)
    print(data)

# Exercise 2: Writing to a JSON File
profile = {
    "name": "Jane Smith",
    "age": 28,
    "city": "Los Angeles",
    "hobbies": ["Photography", "Traveling", "Reading"]
}
with open('profile.json', 'w') as file:
    json.dump(profile, file, indent=4)

# Exercise 3: Converting CSV to JSON
students = []
with open('students.csv', 'r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        students.append(row)
with open('students.json', 'w') as json_file:
    json.dump(students, json_file, indent=4)

# Exercise 4: Converting JSON to CSV
with open('data.json', 'r') as json_file:
    data = json.load(json_file)
with open('data.csv', 'w', newline='') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(data.keys())
    csv_writer.writerow(data.values())

# Exercise 5: Nested JSON Parsing
with open('books.json', 'r') as file:
    books_data = json.load(file)
    for book in books_data["books"]:
        print(book["title"])
