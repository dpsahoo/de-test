from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, avg, asc, desc, max, when, col, year, count, current_date, row_number, asc
from pyspark.sql.window import Window

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


def avg_salary(df_exploded):
    # What is the average salary for each profile? Display the first 10 results, ordered by lastName in descending order.
    # compute average salary for each profile
    # calculate the average salary across the whole dataset
    df_avg_salary = df_exploded.groupBy("id", "firstName", "lastName").agg(avg("jobHistory.salary").alias("avg_salary"))

    # display first 10 results ordered by lastName in descending order
    print("Average salary for each profile:")
    df_avg_salary.orderBy("lastName", ascending=False).show(10, truncate=False)

    avg_salary = df_exploded.select(avg("jobHistory.salary")).collect()[0][0]
    print("Average salary across the whole dataset: {}".format(avg_salary))


def top_bottom_jobs(df_exploded):
    '''Get top 5 and bottom 5 jobs on average'''

    # Using the exploded dataframe, group by 'title' and calculate the average salary
    df_avg_salary = df_exploded.groupBy("jobHistory.title") \
                            .agg(avg("jobHistory.salary").alias("avg_salary")) \
                            .orderBy("avg_salary", ascending=False)

    top_5 = df_avg_salary.limit(5)
    bottom_5 = df_avg_salary.orderBy("avg_salary").limit(5)

    # Print the results
    print("Top 5 paying jobs:")
    top_5.show()
    print("Bottom 5 paying jobs:")
    bottom_5.show()


def current_most_money_maker(df_exploded):
    '''Get the profile making the money currrently'''

    # Create a new dataframe using 'df_exploded' with an added column 'currentSalary' and populate with salary where toDate is null.
    df_with_current_salary = df_exploded.withColumn("currentSalary", 
                                            when(col("jobHistory.toDate").isNull(),
                                                col("jobHistory.salary")).otherwise(None))

    # Group by the profile i.e. id, firstName, lastName and get the max currentSalary
    df_max_salary = df_with_current_salary.groupBy("id", "firstName", "lastName").agg(max("currentSalary").alias("maxSalary"))

    print('Profile making most money currently:')
    df_max_salary.orderBy(desc("maxSalary"), desc("lastName"), desc("firstName")).limit(1).show(truncate=False)

def most_popular_job_2019(df_exploded):
    ''' What was the most popular job title that started in 2019?'''

    most_popular_job_2019 = df_exploded.filter(year('jobHistory.fromDate') == 2019) \
    .groupBy('jobHistory.title') \
    .agg(count('jobHistory.title').alias('count')) \
    .orderBy(desc('count'), 'jobHistory.title')  

    # most_popular_job_2019.show(truncate=False)
    print('The most popular job title that started in 2019 is:', most_popular_job_2019.select('title').first()[0] )


def num_people_working(df_exploded):
    '''Get number of people currently working'''
    num_currently_working = df_exploded.filter(df_exploded.jobHistory.toDate.isNull() | (df_exploded.jobHistory.toDate >= current_date())).count()

    print('Total number of people currently working: ', num_currently_working)


def person_latest_job(df_exploded):
    '''For each person, list only their latest job'''
    # Define a Window specification, partition by 'id' and order desc by fromDate
    # Use row_number() function to assign a row number to each job history record for each person

    # Window spec
    w = Window.partitionBy("id").orderBy(desc("jobHistory.fromDate"))

    df_with_row_num = df_exploded.withColumn("row_num", row_number().over(w))

    # Filter the dataframe to keep only the rows where row_num = 1
    latest_jobs_df = df_with_row_num.filter("row_num = 1")

    # Select only the relevant columns and display the first 10 results
    print('Latest job for each person:')
    # latest_jobs_df.select("id", "firstName", "lastName", "jobHistory.title", "jobHistory.fromDate").orderBy("desc(lastName)", "asc(firstName)").show(10, truncate=False)
    # latest_jobs_df.select("id", "firstName", "lastName", "jobHistory.title", "jobHistory.fromDate").show(10, truncate=False)
    latest_jobs_df.select("id", "firstName", "lastName", "jobHistory.title", "jobHistory.fromDate").orderBy(desc("lastName"), asc("firstName")).show(10, truncate=False)


def  max_salary(df_exploded):
    '''
    # For each person, list their highest paying job along with their first name, last name, salary and the year they made this salary. 
    # Store the results in a dataframe, and then print out 10 results
    '''

    # group by id, firstname,lastname and get the max salary
    max_salary_df = df_exploded.groupBy("id", "firstName", "lastName") \
        .agg( \
            max("jobHistory.salary").alias("max_salary"), \
            year(max("jobHistory.fromDate")).alias("year"), \
            max("jobHistory.title").alias("jobTitle") ) \
        .orderBy("lastName", "firstName") \
        .limit(10)

    print('Highest paying job for each person: ')
    max_salary_df.show(truncate=False)
    return max_salary_df


def max_salary_parquet(max_salary_df):
    ''' 
    Write out the last result in parquet format, compressed, partitioned by the year of their highest paying job
    '''
    print('Writing max_salary_df into a parquet file format')
    max_salary_df.write.partitionBy("year").parquet("max_salary.parquet", compression="gzip", mode="overwrite" )


if __name__ == '__main__':
    # Base functions
    spark_sess = create_session()
    df = load_data_and_validate(spark_sess)
    df_exploded = create_exploded_df(df)

    # Business queries
    avg_salary(df_exploded)
    top_bottom_jobs(df_exploded)
    current_most_money_maker(df_exploded)
    most_popular_job_2019(df_exploded)
    num_people_working(df_exploded)
    person_latest_job(df_exploded)
    max_salary_df = max_salary(df_exploded)
    max_salary_parquet(max_salary_df)










