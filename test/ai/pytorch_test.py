import unittest
import torch


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
        assert B[0][0]+B[0][1]+B[0][2] == 1
