#!/usr/bin/env python
# -*-conding:utf-8-*-

from operator import add
from pyspark.sql import SparkSession
import sys
import pandas as pd

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("test2")\
        .getOrCreate()
    # print("1")
    user = spark.read.text(sys.argv[2]).rdd.map(lambda r: r[0])
    userYoung = user.map(lambda x:x.split(",")).filter(lambda x:'0'<x[1]<'4')\
        .map(lambda x: x[0]).collect()


    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])#transformation？？

    counts = lines.map(lambda x:x.split(",")).filter(lambda x:'0'<x[6]<'4' and x[5] == '1111' and (x[0] in userYoung))\
                 .map(lambda x:(x[3],1))\
                      .reduceByKey(add)

    counts = counts.takeOrdered(100, key=lambda x:-x[1])  # 这样似乎已经是list了

    # output = counts.collect() #这样以来似乎就已经是list了
    # output = output.top(3)
    for (word, count) in counts:
        print(word, count)
    # counts.saveAsTextFile("output")
    a = pd.DataFrame(counts)
    a.to_excel("PycharmProjects/FBD-Final/output/任务一/最受年轻人喜爱商家100res.xlsx")


    # counts.saveAsTextFile("PycharmProjects/FBD-Final/src/outputSeller")
    spark.stop()
