##对于NLP（自然语言处理）来说，分词是一步重要的工作,这里使用jieba分词
##对你输入的文章进行分词然后统计等等操作
import jieba
import json
##导入用于用于制作词云图的wordcloud
from wordcloud import WordCloud,ImageColorGenerator,STOPWORDS
from matplotlib import pyplot as plt
from PIL import Image
import numpy as np
import re



def func(cList):
    ##打开刚刚的info.txt,并且把得到的句柄内容复制给content

    '''这里读每个种类评论所在的文件，改一下地址就好'''
    word = ""
    for cmt in cList:
        word += cmt

    analyze_word(word)


def analyze_word(word):
    ##然后使用jieba模块进行对文本分词整理
    resultword = re.sub("[A-Za-z0-9\[\`\~\!\@\#\$\^\&\*\(\)\=\|\{\}\'\:\;\'\,\[\]\.\<\>\/\?\~\。\@\#\\\&\*\%]", "", word)
    wordlist_after_jieba = jieba.cut(resultword)
    wl_space_split = " ".join(wordlist_after_jieba)

    # 设置停用词
    sw = set(STOPWORDS)
    sw.add("还有")
    sw.add("然后")
    sw.add("但是")
    sw.add("这个")
    sw.add("一个")
    sw.add("感觉")
    sw.add("真是")
    sw.add("所以")
    sw.add("而且")
    sw.add("就是")

    ##font_path
    ##使用worldCloud模块对刚刚整理好的分词信息进行处理.
    ##max_font_size参数是可以调整部分当个词语最大尺寸
    ##max_words是最大可以允许多少个词去组成这个词云图
    ##height高度,width宽度,
    ##background_color背景颜色
    #images = Image.open("D:\\Game\\ab.jpg")
    #maskImages = np.array(images)
    # mask=maskImages
    wc = WordCloud(scale=4, font_path="msyh.ttc", background_color="black", max_words=100, max_font_size=200,
                   stopwords=sw,
                   width=1500, height=1500, random_state=30, collocations=False).generate(wl_space_split)
    ##使用matplotlib的pyplot来进行最后的渲染出图.
    plt.imshow(wc)
    ##目标文件另存为这个名录下
    wc.to_file('wolfcodeTarget.png')
