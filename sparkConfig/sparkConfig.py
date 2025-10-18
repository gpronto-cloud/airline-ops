def getSpark(spark=None):
    if spark is not None:
        return spark
    #print("getSpark called")
    if spark is None:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        #print(f"Spark: {spark}")

    return spark