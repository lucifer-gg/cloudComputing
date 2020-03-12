#coding=utf-8
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from graph import *




#用流处理的方式,读取文件中实时写入的.txt文件
def streaming_possess():
    sc = SparkContext(appName='myApp')
    ssc = StreamingContext(sc, 1)   #每10秒钟检查一遍文件
    lines = ssc.textFileStream('file:///D://test//comments')
    calSubSum2(lines)
    ssc.start()    #监听文件写入
    ssc.awaitTermination()


#计算一段时间内，各种类型美食的评论数量
#输入RDD集合，输出('kind',sum)键值对
def calSubSum(lines):
    kindName = lines.map(lambda word: (word.split(':')[1].split('"')[1], 1))
    kind = lines.map(lambda line: line.split(',')[1])
    kindName.reduceByKey(lambda a, b: a + b).foreach(print)


#计算一段时间内，各种类型美食的评论数量
#流计算的版本，输出('kind',sum)键值对
def calSubSum2(lines):
    kind = lines.map(lambda line: line.split(',')[1])
    kindName = kind.map(lambda word: (word.split(':')[1].split("'")[1], 1))
    sum = kindName.reduceByKey(lambda a, b: a + b).repartition(1).foreachRDD(trFunc)

def trFunc(rdd):
    ss = rdd.collect()
    showFunc(ss)

def main():
    streaming_possess()
    print('All work has been done!')


if __name__=='__main__':
    main()