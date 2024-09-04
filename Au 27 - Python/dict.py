New! Keyboard shortcuts … Drive keyboard shortcuts have been updated to give you first-letters navigation
# Exercise 1: Create a Dictionary
person = {"Name": "Alice", "Age": 25, "City": "New York"}
print(person)

# Exercise 2: Access Dictionary Elements
print(person["City"])

# Exercise 3: Add and Modify Elements
person["email"] = "alice@example.com"
person["Age"] = 26
print(person)

# Exercise 4: Remove Elements
person.pop("City")
print(person)

# Exercise 5: Check if a Key Exists
if "email" in person:
    print("The key 'email' exists in the dictionary.")
else:
    print("The key 'email' does not exist in the dictionary.")

if "phone" in person:
    print("The key 'phone' exists in the dictionary.")
else:
    print("The key 'phone' does not exist in the dictionary.")

# Exercise 6: Check if a Key Exists (This is a repeat, keeping it the same as Exercise 5)
if "email" in person:
    print("The key 'email' exists in the dictionary.")
else:
    print("The key 'email' does not exist in the dictionary.")

if "phone" in person:
    print("The key 'phone' exists in the dictionary.")
else:
    print("The key 'phone' does not exist in the dictionary.")

# Exercise 7: Nested Dictionary
employees = {
    101: {"name": "Bob", "job": "Engineer"},
    102: {"name": "Sue", "job": "Designer"},
    103: {"name": "Tom", "job": "Manager"}
}
print(employees[102])

employees[104] = {"name": "Linda", "job": "HR"}
print(employees)

# Exercise 8: Dictionary Comprehension
squares = {num: num ** 2 for num in range(1, 6)}
print(squares)

# Exercise 9: Merge Two Dictionaries
dict1 = {"a": 1, "b": 2}
dict2 = {"c": 3, "d": 4}
dict1.update(dict2)
print(dict1)

# Exercise 10: Default Dictionary Values
letters_to_numbers = {"a": 1, "b": 2, "c": 3}
print(letters_to_numbers.get("b"))  # Retrieves the value of key "b"
print(letters_to_numbers.get("d", 0))  # Returns 0 if the key "d" is not found

# Exercise 11: Dictionary from Two Lists
keys = ["name", "age", "city"]
values = ["Eve", 29, "San Francisco"]
combined_dict = dict(zip(keys, values))
print(combined_dict)

# Exercise 12: Count Occurrences of Words
sentence = "the quick brown fox jumps over the lazy dog the fox"
words = sentence.split()
word_count = {word: words.count(word) for word in words}
print(word_count)
