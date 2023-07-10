from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import Pool

# sqlalchemy用法
# 连接数据库
engine = create_engine('mysql+pymysql://user:password@host:port/dbname?charset=utf8mb4&parseTime=True&loc=Local')
pool = Pool(engine)

# 创建数据表
meta = MetaData()
table = Table('table_name', meta, Column('id', Integer, primary_key=True), Column('name', String),
              Column('age', Integer))
meta.create_all(engine)

# 插入数据
data = {'name': 'test', 'age': 18}
session = sessionmaker(bind=pool)
session = session()
cursor = session.execute("INSERT INTO table_name (name, age) VALUES (%s, %s)", (data['name'], data['age']))
session.close()

# 查询数据
cursor = session.execute("SELECT * FROM table_name")
rows = cursor.fetchall()
for row in rows:
    print(row)

# 更新数据
cursor = session.execute("UPDATE table_name SET name=%s WHERE id=%s", (data['name'], data['id']))
session.close()

# 删除数据
cursor = session.execute("DELETE FROM table_name WHERE id=%s", (data['id']))
session.close()