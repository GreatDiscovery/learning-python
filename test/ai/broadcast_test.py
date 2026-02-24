import unittest

import torch


class TestBroadcast(unittest.TestCase):
    def test_broadcast(self):
        # 广播 = 自动扩展维度为1的轴，使两个张量可以进行逐元素运算。
        """"
        从右往左对齐维度，维度为 1 的轴可以被“拉伸”，比如(1,3)可以变为（1000，3）
        """
        x = torch.tensor([[1, 2, 3],
                          [4, 5, 6]])

        y = torch.tensor([10, 20, 30])
        z = x + y
        print(z)
        assert torch.equal(z, torch.tensor([[11, 22, 33],
                                            [14, 25, 36]]))
