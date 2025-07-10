import pymysql

conn = pymysql.connect(
    host='localhost',
    user='root',
    password='your_password'
)
print("Connected successfully")
conn.close()
