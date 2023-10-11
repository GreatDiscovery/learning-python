from datetime import datetime

import jwt

if __name__ == '__main__':
    # 设置令牌的密钥和算法
    key = 'your_secret_key'
    algorithm = 'HS256'

    # 创建要编码的数据（payload）
    payload = {
        'user_id': 123,
        'username': 'example_user',
        'exp': datetime(2023, 8, 25)  # 设置令牌过期时间
    }

    # 编码生成令牌
    token = jwt.encode(payload, key, algorithm=algorithm)

    # 输出生成的令牌
    print(token)
