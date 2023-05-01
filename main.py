from pyspark.sql import SparkSession

def create_session():
    # Create a Spark Session
    spark = SparkSession.builder.appName("Seek-Test").getOrCreate()
    return spark

def load_data_and_validate(spark):
    '''
    # Create a dataframe from multiple json files
    # Print the schema
    # Get record count
    '''

    # Path to the directory containing the JSON files
    input_files = "test_data/*"
    df = spark.read.json(input_files)
    df.printSchema()
    df.count()


if __name__ == '__main__':
    spark_sess = create_session()
    load_data_and_validate(spark_sess)
    
