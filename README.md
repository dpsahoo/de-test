# Data Engineering test - Instructions to run the PySpark code 


## 1 DOWNLOAD THE TEST DATA FILE 
NOTE: IGNORE THIS STEP. The downloading and unzip of the file is handled in Step3 and Step4.
Download the zipped test file into your application root directory and unzip it.

### `unzip -d /dest/directory/ test-data.zip`

Source: 
### `https://coding-challenge-public.s3.ap-southeast-2.amazonaws.com/test-data.zip`
note: This is password protected. PLease use the password supplied in the mail.

The unzipped .json files will be in /test-data folder


## 2. The main code is in main.py


## 3. Update the ZIP file password in "Dockerfile"

Update ZIP_PASSWORD env variable in Dockerfile before running next steps.

`ENV ZIP_PASSWORD="<Enter the ZIP file password here>"`

## 4. Using the Dockerfile

   a. You can build the Docker image by navigating to the directory containing the Dockerfile and running the following command:
   ###  `docker build -t seeksparkjob .`

b. This will build the Docker image and tag it with the name "seeksparkjob". Then, you can run the Docker container using the following command
###  `docker run -it seeksparkjob`

This will run the container and execute the spark-submit command specified in the Dockerfile, which will run the main.py script.

The output of all the required queries(except last) will be printed to the terminal.

## 4. Parquet file

The result of the last query will be stored in parquet format as requested.
The folder will be named "max_salary.parquet"
