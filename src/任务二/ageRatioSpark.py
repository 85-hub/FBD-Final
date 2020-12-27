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
    userBuy = lines.map(lambda x: x.split(",")).filter(lambda x: x[6] == '2' and x[5] == '1111')\
                 .map(lambda x: x[0]).distinct().collect()# 所有购买了的userID的list

    user = spark.read.text(sys.argv[2]).rdd.map(lambda r: r[0])#读取info文件
    userAge = user.map(lambda x:x.split(",")).filter(lambda x: (x[0] in userBuy) and '0' < x[1] < '9')\
            .map(lambda x: ((x[1]),1))\
                      .reduceByKey(add)

    counts = userAge.collect()

    for (age, count) in counts:
        print(age, count)
    a = pd.DataFrame(counts)
    a.to_excel("PycharmProjects/FBD-Final/output/年龄段统计res.xlsx")

    spark.stop()
