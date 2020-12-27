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

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])#transformation？？

    counts = lines.map(lambda x:x.split(",")).filter(lambda x:'0'<x[6]<'4' and x[5] == '1111')\
                 .map(lambda x:(x[1],1))\
                      .reduceByKey(add)

    counts = counts.takeOrdered(100, key=lambda x:-x[1])  # 这样似乎已经是list了

    # output = counts.collect() #这样以来似乎就已经是list了
    # output = output.top(3)
    for (word, count) in counts:
        print(word, count)
    # counts.saveAsTextFile("output")
    a = pd.DataFrame(counts)
    a.to_excel("PycharmProjects/FBD-Final/output/任务一/最受欢迎商品100result.xlsx")



    # counts.saveAsTextFile("PycharmProjects/FBD-Final/src/outputSeller")
    spark.stop()
