import unittest
import torch
from torch import nn


class TestPytorchFunc(unittest.TestCase):
    def test_tensor(self):
        # 矩阵：2 行 3 列
        A = torch.tensor([[1., 2., 3.],
                          [4., 5., 6.]])
        B = A.transpose(0, 1)
        print(B)
        assert torch.equal(B, torch.tensor([[1, 4], [2, 5], [3, 6]]))

    def test_transpose(self):
        # 转置
        x = torch.randn(2, 3, 4)  # 形状 (2, 3, 4)，维度下标 0, 1, 2
        print(x)

        y = x.transpose(0, 1)  # 互换维度 0 和 1
        # 结果形状：(3, 2, 4)
        print(y)

        z = x.transpose(-2, -1)  # 互换倒数第 2 个和最后 1 个维度（即 1 和 2）
        # 结果形状：(2, 4, 3)
        print(z)

        assert z.shape == (2, 4, 3)

    def test_matmul(self):
        A = torch.tensor([[1., 2., 3.], ])
        B = torch.tensor([[4.], [5.], [6.], ])
        C = torch.matmul(A, B)
        print(C)
        assert torch.equal(C, torch.tensor([[32]]))
        assert C[0, 0] == 32

    def test_softmax(self):
        A = torch.tensor([[1., 2., 3.], ])
        B = A.softmax(dim=-1)
        print(B)
        assert B[0][0] + B[0][1] + B[0][2] == 1

    def test_linear(self):
        # 定义线性层
        linear = nn.Linear(3, 2)

        # 构造一个输入 (batch_size=1, 3维)
        x = torch.tensor([[1.0, 2.0, 3.0]])

        # 前向计算
        y = linear(x)

        print("输入 x:")
        print(x)

        print("\n权重 W:")
        print(linear.weight)

        print("\n偏置 b:")
        print(linear.bias)

        print("\n输出 y:")
        print(y)

    def test_view(self):
        # 在不改变数据的情况下，重新解释张量的形状,元素总数必须一样。
        # PyTorch 的 .view() 要求张量是 contiguous，否则会报错，所以必须先 .contiguous()。
        x = torch.arange(6)
        print("原始 x:")
        print(x)
        print("shape:", x.shape)
        assert x.shape[0] == 6

        y = x.view(2, 3)
        print("\nview 之后:")
        print(y)
        print("shape:", y.shape)
        assert y.shape == (2, 3)

        z = x.view(-1, 3)
        assert z.shape == (2, 3)

