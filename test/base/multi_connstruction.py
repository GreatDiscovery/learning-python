import unittest


class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def say_hello(self):
        print("Hello, my name is", self.name, "and I'm", self.age, "years old.")

    def __str__(self):
        return f"Name: {self.name}, Age: {self.age}"


class Student(Person):
    def __init__(self, name, age, grade):
        super().__init__(name, age)
        self.grade = grade

    def study(self):
        print("I'm studying in grade", self.grade)


class TestPerson1(unittest.TestCase):
    def test1(self):
        s1 = Student("Alice", 20, 1)
        s2 = Student("Bob", 19, 2)
        print(s1)  # 输出：Name: Alice, Age: 20, Grade: 1
        print(s2)  # 输出：Name: Bob, Age: 19, Grade: 2
        print(s1.study())
        print(s2.study())
