import warnings
import numpy as np
import json
import matplotlib.pyplot as plt
warnings.filterwarnings("ignore",".*GUI is implemented.*")

timePointList = [10,11,12,1,2,3,4,5,6,7,8,9]
year = [2018,2019]
i = 0

def graph(x,y,year,month):
    plt.clf()
    plt.barh(np.arange(0, 22),y)
    plt.title(str(year)+'年'+str(month) + '月', fontproperties='simhei')
    plt.yticks(np.arange(0, 22), x, fontproperties='simhei')
    plt.pause(0.4)



def showFunc(list):
    global i
    print("Listening...")
    if i>2:
        y = year[1]
    else:
        y = year[0]
    m = timePointList[i]
    a = []
    b = []
    for item in list:
        a.append(item[0])
        b.append(item[1])
    if len(a)>0 and len(b)>0 and i<12:
        graph(a, b, y, m)
        plt.show()
        i += 1


