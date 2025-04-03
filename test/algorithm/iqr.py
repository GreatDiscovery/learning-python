# 用来识别异常数据，四分位距和3σ算法， https://blog.csdn.net/qq_66726657/article/details/139454588


import numpy as np
import matplotlib.pyplot as plt


def find_outliers_iqr(data):
    Q1 = np.percentile(data, 25)
    Q3 = np.percentile(data, 75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    outliers = [x for x in data if x < lower_bound or x > upper_bound]
    return outliers


def find_outliers_zscore(data, threshold_num=3.0):
    mean_value = np.mean(data)
    std_dev = np.std(data)
    threshold = threshold_num * std_dev

    outliers = [x for x in data if abs((x - mean_value) / std_dev) > threshold]
    return outliers

def main():
    # 示例数据
    data = [100, 150, 200, 250, 300, 350, 400, 450, 500, 1000]

    # 计算四分位距
    Q1 = np.percentile(data, 25)  # 第一四分位数
    Q3 = np.percentile(data, 75)  # 第三四分位数
    IQR = Q3 - Q1  # 四分位距
    print("第一四分位数:", Q1)
    print("第三四分位数:", Q3)
    print("四分位距:", IQR)
    print("异常值范围:", (Q1 - 1.5 * IQR, Q3 + 1.5 * IQR))
    print("四分位异常值：", find_outliers_iqr(data))

    # 应用3σ原则识别异常值
    mean = np.mean(data)
    std_dev = np.std(data)
    threshold = 3 * std_dev
    outliers = [x for x in data if abs(x - mean) > threshold]
    print("3σ异常值:", find_outliers_zscore(data))

    # 可视化
    # 箱型图
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    plt.boxplot(data)
    plt.xlabel('数据')
    plt.title('数据和异常值')
    plt.show()

def boxplot():
    # 生成示例数据
    data = np.random.normal(loc=50, scale=10, size=100)  # 均值50，标准差10，100个样本

    # 绘制箱型图
    plt.boxplot(data, vert=True, patch_artist=True, showfliers=True)

    # 设置标题
    plt.title("Box Plot Example")
    plt.show()

if __name__ == '__main__':
    main()
    boxplot()
