# Exercise 1: Create a List
fruits = ["apple", "banana", "cherry", "date", "elderberry"]
print("Exercise 1: Create a List")
print(fruits)

# Exercise 2: Access List Elements
print("\nExercise 2: Access List Elements")
print(fruits[0])  # First item
print(fruits[-1])  # Last item
print(fruits[1])  # Second item
print(fruits[3])  # Fourth item

# Exercise 3: Modify a List
print("\nExercise 3: Modify a List")
fruits[1] = "blueberry"
print(fruits)

# Exercise 4: Add and Remove Elements
print("\nExercise 4: Add and Remove Elements")
fruits.append("fig")
fruits.append("grape")
fruits.remove("apple")
print(fruits)

# Exercise 5: Slice a List
print("\nExercise 5: Slice a List")
first_three_fruits = fruits[:3]
print(first_three_fruits)

# Exercise 6: Find List Length
print("\nExercise 6: Find List Length")
print(len(fruits))

# Exercise 7: List Concatenation
print("\nExercise 7: List Concatenation")
vegetables = ["carrot", "broccoli", "spinach"]
food = fruits + vegetables
print(food)

# Exercise 8: Loop Through a List
print("\nExercise 8: Loop Through a List")
for fruit in fruits:
    print(fruit)

# Exercise 9: Check for Membership
print("\nExercise 9: Check for Membership")
if "cherry" in fruits:
    print("Cherry is in the fruits list.")
else:
    print("Cherry is not in the fruits list.")

if "mango" in fruits:
    print("Mango is in the fruits list.")
else:
    print("Mango is not in the fruits list.")

# Exercise 10: List Comprehension
print("\nExercise 10: List Comprehension")
fruit_lengths = [len(fruit) for fruit in fruits]
print(fruit_lengths)

# Exercise 11: Sort a List
print("\nExercise 11: Sort a List")
fruits.sort()
print(fruits)

fruits.sort(reverse=True)
print(fruits)

# Exercise 12: Nested Lists
print("\nExercise 12: Nested Lists")
nested_list = [fruits[:3], fruits[-3:]]
print(nested_list[1][0])  # First element of the second list

# Exercise 13: Remove Duplicates
print("\nExercise 13: Remove Duplicates")
numbers = [1, 2, 2, 3, 4, 4, 4, 5]
unique_numbers = list(set(numbers))
print(unique_numbers)

# Exercise 14: Split and Join Strings
print("\nExercise 14: Split and Join Strings")
words = "hello, world, python, programming".split(", ")
joined_string = " ".join(words)
print(joined_string)
