from pyspark.sql import SparkSession
from random import random
from operator import add
import sys

spark = SparkSession.builder.appName('Calculate Pi').getOrCreate()

accuracy = int(sys.argv[1]) if len(sys.argv) > 1 else 50000

partitions = 2
n = accuracy * partitions

def f(_):
    x = random() * 2 - 1
    y = random() * 2 - 1
    return 1 if x ** 2 + y ** 2 <= 1 else 0

count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
print('Pi is about {0}'.format(4.0 * count / n))
spark.stop()
