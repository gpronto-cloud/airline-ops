import pytest
from pyspark.sql import SparkSession
from validations.validations import check_null_value
from validations.validations import check_totalcount_datavalidity
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.errors.exceptions.base import PySparkValueError
from pyspark.sql.functions import col
from pathlib import Path

class TestValidations:
    def setup_method(self):
        self.spark = SparkSession.builder \
                    .master("local[*]") \
                    .appName("test") \
                    .getOrCreate()


    def teardown_method(self):
        self.spark.stop()



    #@pytest.mark.testcase("TC006")
    #@pytest.mark.parametrize("read_data_file",[("C:/Users/gaura/Documents/databricks_package/airline_ops/data/Air_Traffic_Landings_Statistics.csv","CSV")],indirect=True)
    #def test_check_totalrows(self,read_data_file):
    #    df=read_data_file
    #    assert (check_totalcount_datavalidity(df,20))
        #print(f"Session active: {df.sparkSession.sparkContext._jsc is not None}")
        #print(f"DF session: {df.sparkSession}")
        #print(df.count())
        #assert(df.count()==1)
        #assert (df.count() == 48)

    # Dataframe data missing
    @pytest.mark.testcase("TC001")
    @pytest.mark.parametrize("read_data_file",[("C:/Users/gaura/Documents/databricks_package/airline_ops/data/Air_Traffic_Landings_Statistics_empty.csv","CSV")],indirect=True)
    def test_check_null_value_negative(self,read_data_file):
        df=read_data_file
        with pytest.raises(TypeError,match= "missing.*required"):
            check_null_value(df)

    # Schema mismatch with the data passed in dataframe
    @pytest.mark.testcase("TC002")
    def test_check_validation_schema_mismatch(self):
        schemSt1=StructType([
                    StructField("activity_period", StringType(), False)
                    ,StructField("activity_period_start_dateS", StringType(), False)
                    ,StructField("operating_airline", StringType(), False)
                    ,StructField("operating_airline_iata_code", StringType(), False)
                    ,StructField("published_airline", StringType(), False)
                    ,StructField("published_airline_iata_code", StringType(), False)	
                    ,StructField("geo_summary", StringType(), False)
                    ,StructField("geo_region", StringType(), False)
                ]
            )

        with pytest.raises(PySparkValueError,match="FIELD_STRUCT_LENGTH_MISMATCH"): 
            df=self.spark.createDataFrame([('199907','01/07/1999','ATA Airlines','TZ','ATA Airlines','TZ','Domestic','US','Passenger','Narrow Body','Boeing','727','200','4','618000','20/09/2025 13:01','22/09/2025 15:11')
                                       ,('199907','01/07/1999','ATA Airlines','TZ','ATA Airlines','TZ','Domestic','US','Passenger','Narrow Body','Boeing','757','','78','15444000','20/09/2025 13:01','22/09/2025 15:11')
                                       ,('199907','01/07/1999','ATA Airlines','TZ','ATA Airlines','TZ','Domestic','US','Passenger','Wide Body','Lockheed','L1011','0','71','25418000','20/09/2025 13:01','22/09/2025 15:11')
                                       ,('199907','01/07/1999','ATA Airlines','TZ','ATA Airlines','TZ','Domestic','US','Passenger','Wide Body','Lockheed','L1011','100','1','368000','20/09/2025 13:01','22/09/2025 15:11')
                                       ,('199907','01/07/1999','Aeroflot Russian International Airlines','','Aeroflot Russian International Airlines','','International','Europe','Passenger','Wide Body','Boeing','767','0','9','2879955','20/09/2025 13:01','22/09/2025 15:11')
                                       ,('199907','01/07/1999','Aeroflot Russian International Airlines','','Aeroflot Russian International Airlines','','International','Europe','Passenger','Wide Body','Ilyushin','IL-96','','4','1543220','20/09/2025 13:01','22/09/2025 15:11')
                                    ],schemSt1
                                    )
            check_null_value(df)


    @pytest.mark.testcase("TC003")
    @pytest.mark.parametrize("read_data_file",[("C:/Users/gaura/Documents/databricks_package/airline_ops/data/Air_Traffic_Landings_Statistics_activityperiod_null.csv","CSV")],indirect=True)
    def test_check_blank_value_positive(self,read_data_file):
        df=read_data_file
        assert check_null_value(df,"activity_period")

    #with pytest.raises(ValueError,"Error message"):
    @pytest.mark.testcase("TC004")
    @pytest.mark.parametrize("read_data_file",[("C:/Users/gaura/Documents/databricks_package/airline_ops/data/Air_Traffic_Landings_Statistics.csv","CSV")],indirect=True)
    def test_check_null_value_positive(self,read_data_file):
        df=read_data_file
        assert check_null_value(df,"activity_period")

    @pytest.mark.testcase("TC006")
    @pytest.mark.parametrize("read_data_file",[("C:/Users/gaura/Documents/databricks_package/airline_ops/data/Air_Traffic_Landings_Statistics.csv","CSV")],indirect=True)
    def test_check_totalrows(self,read_data_file):
        df=read_data_file
        #assert (df.count()==1)
        assert (check_totalcount_datavalidity(df,"activity_period",21))