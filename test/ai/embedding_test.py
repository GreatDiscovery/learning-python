import numpy as np


# 和数组查表的区别？从 训练目标 看，它是一个 可学习的查表：
#
# 这个 embedding_matrix 不是固定的，
#
# 而是在训练过程中 被优化算法（如梯度下降）不断更新 的。
if __name__ == '__main__':
    # 假设我们有 5 个词（word_id 0~4）
    vocab_size = 5
    embedding_dim = 3

    # 初始化 embedding 矩阵（每行是一个词的向量）
    embedding_matrix = np.random.randn(vocab_size, embedding_dim)

    print("Embedding 矩阵：")
    print(embedding_matrix)

    # 假设我们要查第 2 个词（word_id = 2）
    word_id = 2

    # 方法 1：用 one-hot * embedding 矩阵 的乘法形式
    one_hot = np.zeros(vocab_size)
    one_hot[word_id] = 1
    embedding_vector_1 = one_hot @ embedding_matrix

    # 方法 2：直接查表（更高效的做法）
    embedding_vector_2 = embedding_matrix[word_id]

    print("\n通过 one-hot * E 计算得到的 embedding：", embedding_vector_1)
    print("通过直接索引得到的 embedding：", embedding_vector_2)
