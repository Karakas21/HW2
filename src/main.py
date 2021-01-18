from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, LongType, IntegerType, StringType
from pyspark.sql.functions import col, lit, split
import sys


def computeDF(df):
    """ 
    This method counts each log rank(priority) groupping them by rank,hour, day and month

    Attributes:
    __________
    df : Spark DataFrame
        initiat DataFrame

    Returns
    _______
    df : Spark DataFrame
        resulting DataFrame
    """
    df = df.withColumn('month', split(df.date, ' ')[0]) \
     .withColumn('day', split(df.date, ' ')[1]) \
     .withColumn('time', split(df.date, ' ')[2])

    df = df.withColumn('hour', split(df.time, ':')[0])

    columns_to_drop = ['time', 'date']
    df = df.drop(*columns_to_drop)

    df = df.groupBy('month', 'day', 'hour', 'rank').count()

    df = df.sort('month','day', 'hour', 'rank')
    return df

def main(argv):
    fileFrom = argv[0]
    fileTo = argv[1]

    spark = SparkSession \
        .builder \
        .appName("lab2").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") 

    schema = StructType() \
      .add("date",StringType(),True) \
      .add("rank",IntegerType(),True) \
      .add("extraInfo",StringType(),True)

    initDF = spark.read.format('csv').option('header', True).schema(schema).load(fileFrom)
    initDF.printSchema()
    initDF.show(truncate=False)

    resultDF = computeDF(initDF)
    resultDF.show(truncate=False)
    resultDF.write.csv(fileTo)

if __name__ == "__main__":
    print(sys.argv)
    main(sys.argv[1:])
