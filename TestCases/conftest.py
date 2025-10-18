import pytest
import pandas as pd
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession


#@pytest.fixture(params=[
#    pd.DataFrame({'col': ['']}),        # empty string
#    pd.DataFrame({'col': ['   ']}),     # whitespace
#    pd.DataFrame({'col': [np.nan]}),    # null value
#]))

@pytest.fixture(scope="function")
def create_spark_session():
    spark = SparkSession.builder \
                    .master("local[*]") \
                    .appName("test") \
                    .getOrCreate()

    yield spark
    spark.stop()

@pytest.fixture
def sample_dfSchema():
    return StructType([
                    StructField("activity_period", StringType(), False)
                    ,StructField("activity_period_start_dateS", StringType(), False)
                    ,StructField("operating_airline", StringType(), False)
                    ,StructField("operating_airline_iata_code", StringType(), False)
                    ,StructField("published_airline", StringType(), False)
                    ,StructField("published_airline_iata_code", StringType(), False)	
                    ,StructField("geo_summary", StringType(), False)
                    ,StructField("geo_region", StringType(), False)
                    ,StructField("landing_aircraft_type", StringType(), False)
                    ,StructField("aircraft_body_type", StringType(), False)
                    ,StructField("aircraft_manufacturer", StringType(), False)
                    ,StructField("aircraft_model", StringType(), False)
                    ,StructField("aircraft_version", StringType(), False)
                    ,StructField("landing_vount", StringType(), False)
                    ,StructField("total_landed_weight", StringType(), False)
                    ,StructField("data_as_of", StringType(), False)
                    ,StructField("data_loaded_at", StringType(), False)
                ]
            )


@pytest.fixture(scope="function") 
def read_data_file(create_spark_session,sample_dfSchema,request):
    filename,format=request.param
    df=create_spark_session.read.format(format).schema(sample_dfSchema).load(filename)
    yield df
    #return df