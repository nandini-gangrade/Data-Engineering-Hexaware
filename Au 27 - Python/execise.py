New! Keyboard shortcuts … Drive keyboard shortcuts have been updated to give you first-letters navigation
# Exercise 1: List Operations
numbers = [1, 2, 3, 4, 5]
numbers.append(6)
numbers.remove(3)
numbers.insert(0, 0)
print(numbers)

# Exercise 2: Tuple Operations
coordinates = (10.0, 20.0, 30.0)
print(coordinates[1])
try:
    coordinates[2] = 40.0
except TypeError as e:
    print(e)

# Exercise 3: Set Operations
fruits = {"apple", "banana", "cherry"}
fruits.add("orange")
fruits.remove("banana")
print("Cherry is in the set" if "cherry" in fruits else "Cherry is not in the set")
citrus = {"orange", "lemon", "lime"}
print(fruits | citrus)
print(fruits & citrus)

# Exercise 4: Dictionary Operations
person = {"name": "John", "age": 30, "city": "New York"}
print(person["name"])
person["age"] = 31
person["email"] = "john@example.com"
del person["city"]
print(person)

# Exercise 5: Nested Dictionary
school = {
    "Alice": {"Math": 90, "Science": 85},
    "Bob": {"Math": 78, "Science": 92},
    "Charlie": {"Math": 95, "Science": 88}
}
print(school["Alice"]["Math"])
school["David"] = {"Math": 80, "Science": 89}
school["Bob"]["Science"] = 95
print(school)

# Exercise 6: List Comprehension
numbers = [1, 2, 3, 4, 5]
squared_numbers = [n**2 for n in numbers]
print(squared_numbers)

# Exercise 7: Set Comprehension
squared_set = {n**2 for n in [1, 2, 3, 4, 5]}
print(squared_set)

# Exercise 8: Dictionary Comprehension
cubed_dict = {n: n**3 for n in range(1, 6)}
print(cubed_dict)

# Exercise 9: Combining Collections
keys = ["name", "age", "city"]
values = ["Alice", 25, "Paris"]
combined_dict = dict(zip(keys, values))
print(combined_dict)

# Exercise 10: Count Word Occurrences
sentence = "the quick brown fox jumps over the lazy dog the fox"
word_counts = {}
for word in sentence.split():
    word_counts[word] = word_counts.get(word, 0) + 1
print(word_counts)

# Exercise 11: Unique Elements in Two Sets
set1 = {1, 2, 3, 4, 5}
set2 = {4, 5, 6, 7, 8}
print(set1 | set2)
print(set1 & set2)
print(set1 - set2)

# Exercise 12: Tuple Unpacking
info_tuple = ("Alice", 25, "Paris")
name, age, city = info_tuple
print(name, age, city)

# Exercise 13: Frequency Counter with Dictionary
text = "hello world"
frequency_dict = {}
for char in text:
    if char != ' ':
        frequency_dict[char] = frequency_dict.get(char, 0) + 1
print(frequency_dict)

# Exercise 14: Sorting a List of Tuples
students = [("Alice", 90), ("Bob", 80), ("Charlie", 85)]
sorted_students = sorted(students, key=lambda student: student[1], reverse=True)
print(sorted_students)
