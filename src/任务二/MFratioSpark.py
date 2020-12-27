#!/usr/bin/env python
# -*-conding:utf-8-*-

from operator import add
from pyspark.sql import SparkSession
import sys
import pandas as pd

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("sexRatio")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])#读取log文件

    userBuy = lines.map(lambda x:x.split(",")).filter(lambda x:x[6] == '2' and x[5] == '1111')\
                 .map(lambda x:x[0]).distinct().collect()# 所有购买了的userID的list

    user = spark.read.text(sys.argv[2]).rdd.map(lambda r: r[0])#读取info文件
    userSex = user.map(lambda x:x.split(",")).filter(lambda x:(x[0] in userBuy) and (x[2] == '0' or x[2] == '1'))\
            .map(lambda x:('male',1) if x[2] == '1' else ('female', 1))\
                      .reduceByKey(add)


    counts = userSex.collect()

    # output = counts.collect() #这样以来似乎就已经是list了
    # output = output.top(3)
    for (sex, count) in counts:
        print(sex, count)
    # counts.saveAsTextFile("output")
    a = pd.DataFrame(counts)
    a.to_excel("PycharmProjects/FBD-Final/output/男女比例res.xlsx")

    spark.stop()
