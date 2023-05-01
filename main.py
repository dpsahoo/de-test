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
    return df

def create_exploded_df(orig_df):
    '''
    # Create an exploded/expanded dataframe to perform further queries. 
    # Needed to normalise 'jobHistory' array column 
    '''
    # explode jobHistory array column
    df_exploded = df.selectExpr("id", "profile.firstName", "profile.lastName", "explode(profile.jobHistory) as jobHistory")
    return df_exploded




if __name__ == '__main__':
    # Base functions
    spark_sess = create_session()
    df = load_data_and_validate(spark_sess)
    df_exploded = create_exploded_df(df)

    # Business queries



