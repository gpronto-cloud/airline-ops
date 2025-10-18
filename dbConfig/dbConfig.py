def read_data_from_file(path:str,format:str='parquet',schemaSt:str=None):

    #print ('hello')
    from airline_ops.sparkConfig.sparkConfig import getSpark

    #dbfs:/FileStore/tables/Air_Traffic_Landings_Statistics.csv
    spark=getSpark()

    sp=spark.read.format(format)
    if schemaSt is not None:
        sp = sp.schema(schemaSt)
    
    #df=spark.read.format(format).schema(schemaSt).load(path)        
    df = sp.load(path)

    return df

'''
def write_data_into_db(df:,format:str='parquet',schemaSt:str=None):

    #print ('hello')
    from airline_ops.sparkConfig.sparkConfig import getSpark

    #dbfs:/FileStore/tables/Air_Traffic_Landings_Statistics.csv
    spark=getSpark()

    sp=spark.read.format(format)
    if schemaSt is not None:
        sp = sp.schema(schemaSt)
    
    #df=spark.read.format(format).schema(schemaSt).load(path)
    try:
        df = sp.load(path)
    except Exception as e:
        raise e
    
    return df'''