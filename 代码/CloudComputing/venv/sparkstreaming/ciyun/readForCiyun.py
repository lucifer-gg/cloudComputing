#coding=utf-8

from pyspark.sql import SparkSession
import pymongo
MONGO_URL = 'localhost'
MONGO_DB = 'meituanComment'
MONGO_TABLE = 'comment'

client = pymongo.MongoClient(MONGO_URL)
db = client[MONGO_DB]
timePointList = ['2018-10-01 00:00:00','2018-11-01 00:00:00','2018-12-01 00:00:00','2019-01-01 00:00:00','2019-02-01 00:00:00','2019-03-01 00:00:00',
                 '2019-04-01 00:00:00','2019-05-01 00:00:00','2019-06-01 00:00:00','2019-07-01 00:00:00','2019-08-01 00:00:00','2019-09-01 00:00:00',
                 '2019-10-09 00:00:00']

catagrory = [
    '火锅',
    '自助餐',
    '小吃快餐',
    '日韩料理',
    '西餐',
    '烧烤烤肉',
    '东北菜',
    '川湘菜',
    '江浙菜',
    '香锅烤鱼',
    '粤菜',
    '中式烧烤/烤串',
    '西北菜',
    '京菜鲁菜',
    '云贵菜',
    '东南亚菜',
    '海鲜',
    '素食',
    '台湾/客家菜',
    '创意菜',
    '汤/粥/炖菜',
    '蒙菜',
    '新疆菜',
    '其他美食',
    '聚餐宴请']


def read_from_mongo():
    for i in range(1,13):
        tp = get_time_point(i)
        myquery = {"kind":"火锅","commentTime": {"$lt": tp.get('end'), "$gte": tp.get('start')}}
        cursor = db.comments.find(myquery).sort([('commentTime', 1)])
        s = generate_str(cursor)
        file_path = 'D://test//comments//testComment' + str(i) + '.txt'
        create__file(file_path, s)

def get_time_point(i):
    tp = {
        'start':timePointList[i-1],
        'end':timePointList[i]
    }

    return tp


#生成要写入文件的字符串
def generate_str(cursor):
    s = ''
    while True:
        try:
            a_comment = str(cursor.next())
            s += a_comment
            s += '\n'
        except Exception:
            s = s[:-1]
            break
    return s

#创建文件
#file_path：文件路径
#msg：即要写入的内容
def create__file(file_path,msg):
    f=open(file_path,"a",encoding='utf-8')
    f.write(msg)
    print('the message has been written into file!')
    f.close()


def main():
    read_from_mongo()

if __name__=='__main__':
    main()