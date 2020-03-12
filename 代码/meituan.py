from pyquery import PyQuery as pq
from config import *
from urllib.parse import urlencode
import requests
from lxml.etree import XMLSyntaxError
from requests.exceptions import ConnectionError
import pymongo
import time

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


headers1 = {
    'Accept': 'application/json',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Connection': 'keep-alive',
    'Cookie': '_lxsdk_cuid=16afe05c2acc8-0e1d0aa16bab43-e323069-144000-16afe05c2adc8; ci=55; rvct=55; _hc.v=b5820ce4-15e1-ad97-90a8-b3e14af19bfb.1569396765; mtcdn=K; lsu=; client-id=21a6dad5-50ac-40ae-a6fe-1636f005f64c; uuid=029f895b06374077b7c5.1570667037.1.0.0; _lx_utm=utm_source%3DBaidu%26utm_medium%3Dorganic; _lxsdk=16afe05c2acc8-0e1d0aa16bab43-e323069-144000-16afe05c2adc8; lat=32.023686; lng=118.785994; u=886156736; n=%E5%B1%BF%E5%87%892017; lt=hsW_d5bsxqXgZ-CZU3aRX7cuGWYAAAAAPQkAAN3L9u5D9_rHfai1gbiuc-9hry1i1IP3iMC_eDzyRUApT7Z3IsecX6Hk7R5HT8B0gA; token2=hsW_d5bsxqXgZ-CZU3aRX7cuGWYAAAAAPQkAAN3L9u5D9_rHfai1gbiuc-9hry1i1IP3iMC_eDzyRUApT7Z3IsecX6Hk7R5HT8B0gA; unc=%E5%B1%BF%E5%87%892017; __mta=251516601.1569396408974.1570696909447.1570709015208.22; firstTime=1570709555379; _lxsdk_s=16db56b730c-fad-0ea-dc8%7C%7C142',
    'Host': 'nj.meituan.com',
    'Referer': 'https://nj.meituan.com/meishi/c17/',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36'
}

headers = {
    'Accept': 'application/json',
    'Referer': 'https://www.meituan.com/meishi/162920588/',
    'Sec-Fetch-Mode': 'cors',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36'
}

def get_proxy():
    return requests.get("http://127.0.0.1:5010/get/").json()

def delete_proxy(proxy):
    requests.get("http://127.0.0.1:5010/delete/?proxy={}".format(proxy))


def getCommentHtml(poiId,offset):
    # ....
    retry_count = 5
    proxy = get_proxy().get("proxy")
    url = 'https://www.meituan.com/meishi/api/poi/getMerchantComment?uuid=029f895b06374077b7c5.1570667037.1.0.0&platform=1&partner=126&originUrl=https%3A%2F%2Fwww.meituan.com%2Fmeishi%2F193845047%2F&riskLevel=1&optimusCode=10&id='+str(poiId)+'&userId=886156736&offset='+str(offset)+'&pageSize=10&sortType=1'
    while retry_count > 0:
        try:
            html = requests.get(url, headers=headers, proxies={"http": "http://{}".format(proxy)}).json()
            # 使用代理访问
            return html
        except Exception:
            retry_count -= 1
    # 出错5次, 删除代理池中代理
    delete_proxy(proxy)
    return None

#爬取一家店铺
def possess_a_shop(poiId,kind,title):
    for offset in range(0,10000,10):
        html = getCommentHtml(poiId, str(offset))
        commentList = html.get('data').get('comments')
        overTime = False
        try:
            for item in commentList:
                comment = item.get('comment')
                commentTime = item.get('commentTime')
                if checkTime(commentTime) == False:
                    overTime = True #跳出外层循坏
                    break
                commentTime = timeStamp(commentTime)
                star = item.get('star')
                data = {
                    "kind": kind,
                    'title': title,
                    "comment": comment,
                    "commentTime": commentTime,
                    "star": star
                }
                print(data)
                save_to_mongo(data)
        except TypeError:
            print('there is no more comments in this shop!')
            break
        if overTime:
            break
        print('Start crawling next page!')



#以一年为基准
def checkTime(timeNum):
    timeNum = int(timeNum)
    ts = 1538535282133
    if timeNum>=ts:
        return True
    return False


#13位时间戳字符串转换为时间字符串，格式为2017-03-10 12：33：37
def timeStamp(timeNum):
    timeNum = int(timeNum)
    timeStamp = float(timeNum/1000)
    timeArray = time.localtime(timeStamp)
    otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    return otherStyleTime


#爬取评论的入口
def get_pidId(poiId,cateId,title):
    kindN = catagrory.get(cateId)
    possess_a_shop(poiId, kindN,title)


#把一条评论的字典存到mongodb
def save_to_mongo(data):
    if db['comments'].insert_one(data):
        print('Saved to Mongo', data['kind'])
    else:
        print('Saved to Mongo Failed', data['kind'])


#获取店铺id列表所在页面的html
def getShopHtml(cateId,page):
    # ....
    retry_count = 5
    proxy = get_proxy().get("proxy")
    url = 'https://nj.meituan.com/meishi/api/poi/getPoiList?cityName=%E5%8D%97%E4%BA%AC&cateId='+str(cateId)+'&areaId=0&sort=&dinnerCountAttrId=&page='+str(page)+'&userId=886156736&uuid=029f895b06374077b7c5.1570667037.1.0.0&platform=1&partner=126&originUrl=https%3A%2F%2Fnj.meituan.com%2Fmeishi%2Fc17%2F&riskLevel=1&optimusCode=10&_token=eJxVjktzokAUhf9Lb6XsB2%2BrsgCESISJtKIkqSwQWoHwUGijMjX%2FfTpVzmJW3znnnlt1foPez8EMI2QiJIFv1oMZwFM01YAE%2BCAuqo50rCoGUkxDAtn%2FmaqJp32%2FnYPZh64akk7I509Ahf%2FAqqxJhqZ8Sg9JhCSK9AOw90UFFJyfhhmEbTVtWMkvaTvNugYKPRQlzLAOxQwg6s1G1AW%2FHkwf5P98KHaL7lAeW6HYy3Vb9ZxboxvRBaeD48LXAsovvmN3WWkp2dWLg%2FuWrmp%2FMZS1RQ8FcYli8bgyiXtuuoM3vt6GxFnIYT14cuVhZB0nxi%2B2L8kGfrfwwvHkLUKneN5%2F1Yfm7SJHVFWpvV%2Bx9XvvpdF6LBemp%2BwY8ZiG9WVu76hVsaQITZqftcQP8LOTDrqJneTayzvtTOw88Yx7mZDs2mJFjScpq7OMuEGfB5U6Ru8r2mXzINpNLnOuVxt%2BO4d3a1RON2elxexI681zZxc0XS5dAtF4X1fhE%2FjzF%2F%2BOkAA%3D'
    while retry_count > 0:
        try:
            html = requests.get(url, headers=headers1, proxies={"http": "http://{}".format(proxy)}).json()
            # 使用代理访问
            return html
        except Exception:
            retry_count -= 1
    # 出错5次, 删除代理池中代理
    delete_proxy(proxy)
    return None


#爬取一个种类所有页面的店铺id
def possess_a_kind(cateId,pCount):
    for i in range(1,pCount):
        html = getShopHtml(cateId, i)
        shopList = html.get('data').get('poiInfos')
        for item in shopList:
            poiId = item.get('poiId')
            title = item.get('title')
            poiIdInfo = {
                'poiId':poiId,
                'cateId':cateId,
                'title':title
            }
            save_poiId_to_mongo(poiIdInfo)
            print(poiIdInfo)
    print('start crawling next page of this kind of food!')


#爬取所有的商铺id，并保存到mongodb
def crawl_poiId():
    #一次性爬取会报错，所以分成三个批次
    pageCount = {
        17: 68,
        40: 14,
        36: 67,
        28: 29,
        35: 29,
        54: 42,
        20003: 3
    }

    pageCount2 = {
        55: 33,
        56: 67,
        20004: 16,
        57: 9,
        400: 31,
        58: 1,
        59: 4,
        60: 2,
        62: 3,
        63: 24
    }

    pageCount3 = {
        217: 1,
        227: 2,
        228: 9,
        229: 9,
        232: 1,
        233: 2,
        24: 43,
        395: 60
    }

    catagroryList = list(pageCount3.keys())
    for i in catagroryList:
        pageC = pageCount3.get(i)
        possess_a_kind(i, pageC)


def main():
    read_poiId()

#从mongodb读poiId数据
def read_poiId():
    cursor = db['poiId'].find()
    while True:
        try:
            data = cursor.next()
            poiId = data.get('poiId')
            cateId = data.get('cateId')
            title = data.get('title')
            get_pidId(poiId,cateId,title)
        except Exception:
            print('all poiId is read!')
            break


#把爬取的poiId,cateId,title保存成字典，存到mongodb
def save_poiId_to_mongo(data):
    if db['poiId'].insert_one(data):
        print('Saved to Mongo', data['poiId'])
    else:
        print('Saved to Mongo Failed', data['poiId'])


if __name__=='__main__':
    main()
