from typing import TypeVar

"""
TypeVar 是 Python 类型系统里的"类型变量"——你可以把它理解成"给类型起的占位符"，类比泛型（generics）里的 T、U。
"""
T = TypeVar("T")  # T 是一个类型占位符


def first(items: list[T]) -> T:
    return items[0]


print(type(first([1, 2, 3])))  # T 被推断为 int，返回 int
print(type(first(["a", "b"])))  # T 被推断为 str，返回 str
