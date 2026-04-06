import unittest
import ast

'''
ast.parse(expression, mode='eval') 是 Python 里用来**把一段表达式字符串解析成抽象语法树（AST, Abstract Syntax Tree）**的函数。
Expression(
  body=BinOp(
    left=Constant(value=1),
    op=Add(),
    right=BinOp(
      left=Constant(value=2),
      op=Mult(),
      right=Constant(value=3)
    )
  )
)
代码字符串
   ↓
parse
   ↓
AST
   ↓
解释执行 / 编译优化
'''


class EvalTest(unittest.TestCase):
    def test_eval(self):
        expression = "1 + 2 * 3"
        node = ast.parse(expression, mode='eval')
        print(ast.dump(node, indent=2))
