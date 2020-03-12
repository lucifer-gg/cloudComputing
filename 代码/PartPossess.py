#coding=utf-8

from pyspark.sql import SparkSession
import pymongo
import time
MONGO_URL = 'localhost'
MONGO_DB = 'meituanComment'
MONGO_TABLE = 'connection'

client = pymongo.MongoClient(MONGO_URL)
db = client[MONGO_DB]

catagrory = {
    17:'火锅',
    40:'自助餐',
    36:'小吃快餐',
    28:'日韩料理',
    35:'西餐',
    54:'烧烤烤肉',
    20003:'东北菜',
    55:'川湘菜',
    56:'江浙菜',
    20004:'香锅烤鱼',
    57:'粤菜',
    400:'中式烧烤/烤串',
    58:'西北菜',
    59:'京菜鲁菜',
    60:'云贵菜',
    62:'东南亚菜',
    63:'海鲜',
    217:'素食',
    227:'台湾/客家菜',
    228:'创意菜',
    229:'汤/粥/炖菜',
    232:'蒙菜',
    233:'新疆菜',
    24:'其他美食',
    395:'聚餐宴请'
}

shop_list = []

def read_from_mongo():
    cursor = db['connection'].find({'userId':{'$ne':0}})        #排除匿名用户，匿名用户的ID为0
    s = ''
    while True:
        try:
            data = cursor.next()
            title = data.get('title')
            userId = data.get('userId')
            cmtTime = data.get('commentTime')
            kind = data.get('kind')
            if kind == '烧烤烤肉':
                print('恭喜你跑完了一种食物！')
                break
            if kind!='火锅' and kind !='自助餐' and kind!='小吃快餐' and kind!='日韩料理':
                s += find_connection(title, userId, cmtTime)
            shop_list.append(title)
        except Exception:
            print('all connection is found!')
            break
    create__file('./out.txt',s)


#13位时间戳字符串转换为时间字符串，格式为2017-03-10 12：33：37
def timeStamp(timeNum):
    timeNum = int(timeNum)
    timeStamp = float(timeNum)
    timeArray = time.localtime(timeStamp)
    otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    return otherStyleTime


# 将时间字符串转换为10位时间戳，时间字符串默认为2017-10-01 13:37:04格式
def date_to_timestamp(date, format_string="%Y-%m-%d %H:%M:%S"):
    time_array = time.strptime(date, format_string)
    time_stamp = int(time.mktime(time_array))
    return time_stamp


#找到被同一顾客评论过的店铺组合
def find_connection(title,userId,cmtTime):
    shop = db['poiId'].find_one({'title':title})
    poiId = shop.get('poiId')       #第一家店铺的id
    cateId = shop.get('cateId')     #第一家店铺的种类id
    kind = catagrory.get(cateId)     #第一家店铺的种类名
    time = date_to_timestamp(cmtTime)
    start = time - 86400
    end = time + 86400
    start_stamp = timeStamp(start)
    end_stamp = timeStamp(end)

    s = ''
    #用户id相同，第二家店的店名不可以和第一家相同，也不能出现在已经处理过的店铺列别中，评论时间相隔在一天之内
    cursor1 = db['connection'].find({'userId':userId,'title':{'$ne':title,'$nin':shop_list},'commentTime':{"$lt": end_stamp, "$gte": start_stamp}})
    List = [title]    #排除重复数据——完全一样的两条数据
    while True:
        try:
            data = cursor1.next()
            title2 = data.get('title')      #第二家店名
            cmtTime2 = data.get('commentTime')      #顾客在第二家店的评论时间

            shop2 = db['poiId'].find_one({'title': title2})
            poiId2 = shop2.get('poiId')  # 第二家店铺的id
            cateId2 = shop2.get('cateId')  # 第二家店铺的种类id
            kind2 = catagrory.get(cateId2)  # 第二家店铺的种类名

            iden = str(userId)+title2       #iden作为connection的id,若出现过就排除。可以筛选掉重复数据
            if iden not in List:
                s += str(cateId)
                s += ' '
                s += kind
                s += ' '
                s += str(poiId)
                s += ' '
                s += title
                s += ' '

                s += str(cateId2)
                s += ' '
                s += kind2
                s += ' '
                s += str(poiId2)
                s += ' '
                s += title2
                s += '\n'
                List.append(iden)
                print(cateId,kind,poiId,title,cateId2,kind2,poiId2, title2)

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