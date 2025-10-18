from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col
from pyspark.sql.functions import ltrim


def check_null_value(df:DataFrame,columnName:str) -> DataFrame:
    df_new=df.withColumn(f"{columnName}_validity",when(col(columnName).isNotNull(),True).otherwise(False))

    return df_new

def check_blank_value(df:DataFrame,columnName:str) -> DataFrame:
    df_new=df.withColumn(f"{columnName}_validity",when(ltrim(col(columnName)) != '',True).otherwise(False))

    return df_new

def check_totalcount_datavalidity(df:DataFrame, columnName:str,cnt_:int=0) -> int:
    df_new=df.withColumn(f"{columnName}_validity",when(ltrim(col(columnName)) != '',True).otherwise(False))
    #if cnt > cnt_:
    #    return False
    #else:
    #    return True
    return df_new.count()
