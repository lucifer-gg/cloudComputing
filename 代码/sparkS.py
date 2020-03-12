#coding=utf-8
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pic import *




#用流处理的方式,读取文件中实时写入的.txt文件
def streaming_possess():
    sc = SparkContext(appName='myApp')
    ssc = StreamingContext(sc, 1)   #每10秒钟检查一遍文件
    lines = ssc.textFileStream('file:///D://test//comments')
    calSubSum(lines)
    ssc.start()    #监听文件写入
    ssc.awaitTermination()


#输入是一个月关于某一种实物的的所有记录
#筛选出所有的评论信息
def calSubSum(lines):
    kind = lines.map(lambda line: line.split(":")[4])
    kindName = kind.map(lambda word: word.split(", 'commentTime'")[0]).flatMap(lambda word: word.split("'")).filter(lambda w:len(w)>0)
    sum = kindName.repartition(1).foreachRDD(trFunc)

def trFunc(rdd):
    ss = rdd.collect()
    if len(ss)>0:
        func(ss)
    else:
        print('listening...')

def main():
    streaming_possess()
    print('All work has been done!')


if __name__=='__main__':
    main()