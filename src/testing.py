from main import computeDF
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, lit

def are_dfs_equal(df1, df2):
    if df1.collect() != df2.collect():
        return False
    return True

def test():
    spark = SparkSession \
            .builder \
            .appName("tests-lab2").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    testData =  [("Nov 15 12:55:55",1,"Smith"),
      ("Nov 15 13:55:55",2,"dfgg"),
      ("Nov 15 13:55:55",1,"Williams"),
      ("Nov 15 14:55:55",2,"Jones"),
      ("Nov 15 14:55:55",3,"Brown",),
      ("Nov 14 13:55:55",2,"dfgg"),
      ("Nov 15 13:55:55",1,"Williams"),
      ("Nov 15 14:55:55",2,"Jones")
    ]

    expectedResult = [
        ("Nov","14","13",2,1),
        ("Nov","15","12",1,1),
        ("Nov","15","13",1,2),
        ("Nov","15","13",2,1),
        ("Nov","15","14",2,2),
        ("Nov","15","14",3,1)
    ]

    columns1 = ["date","rank","extraInfo"]
    df = spark.createDataFrame(data=testData, schema = columns1)

    columns2 = ["month","day","hour","rank", "count"]
    expectedDF = spark.createDataFrame(data=expectedResult, schema = columns2)


    computedDF = computeDF(df)
    computedDF.show()
    computedDF.printSchema()
    expectedDF.show()
    expectedDF.printSchema()

    if are_dfs_equal(computedDF, expectedDF):
        print('TEST OK! Dataframes are equal!')
    else:
        print('BAD TEST! Dataframes are not equal!')

test()
