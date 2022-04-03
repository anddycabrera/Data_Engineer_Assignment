<div class="cell markdown">

# Data Engineer Assignment: Using Spark

</div>

<div class="cell markdown">

This take-home assignment evaluates your skills using spark as well as
AWS services like S3. You DO NOT need an AWS account since all services
will be mock-up locally using moto library so you will always work
locally with a docker container and without incurring expenses. By
running a docker container you will have all you need to do the
assignment.

</div>

<div class="cell markdown">

## Assignment Instructions

</div>

<div class="cell markdown">

You are expected to fill in functions that would complete this
assignment. All of the necessary helper code is included in this
notebook. Any cell that does not require you to submit code cannot be
modified. For example: Assert statement unit test cells. Cells that have
"**\# YOUR CODE HERE**" are the ONLY ones you will need to alter. DO NOT
change the names of functions. The assignment contains **3 tasks** that
you have to complete. We estimate that the time to complete this task
will be between **30 minutes** to **1 hour** depending on your
experience.

</div>

<div class="cell markdown">

Our recommendation is that you first jump to answer cells (search for
"YOUR CODE HERE") so that you have an idea of what you need to do. After
doing that read the notebook as a whole so that you understand the data
and the context. You need to decide for yourself how much of the context
is sufficient to come up with the answer.

</div>

<div class="cell markdown">

Note: During the assignment when you run some cells you will notice an
output with some messages. This is NOT an error, this is the result of
running the cell. You can distinguish the error from the output because
the outputs use to have a pink background and the error white
background.

</div>

<div class="cell markdown">

## Deliverable

</div>

<div class="cell markdown">

When you complete the assignment you need to save the Jupyter notebook
with your answers and send it to us. If you cannot complete all the
tasks you can send us the Jupyter notebook in any way. We will evaluate
you with the answers you provided even if you complete only one part of
them.

</div>

<div class="cell markdown">

### Requirements And How to Start This Assignment

</div>

<div class="cell markdown">

You will need Docker installed and running. [Docker
Website](https://www.docker.com/).

  - Create a folder and place the files **Dockerfile** and
    **Data\_Engineer\_Assignment.ipynb** there.
  - Open the terminal and go(navigate) to the folder location where you
    saved the files.
  - In the terminal run this command to build the docker image. This can
    take up to 10 minutes to complete: `docker build -t
    spark_assignment:1.0. .`
  - When docker finishes building the image, run this command in your
    terminal: `docker run -it --init -p 8888:8888 --name spark
    spark_assignment:1.0.`
  - Then open this link to open Jupyter Notebook and start the
    assignment. Follow the instructions in the Jupiter Notebook:
    <http://127.0.0.1:8888/tree>
  - You are all set. Good luck\!

</div>

<div class="cell markdown">

## Set AWS Credentials To Work Locally

These are fake AWS credentials to work locally.

</div>

<div class="cell code">

``` python
import os
os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
os.environ['AWS_SECURITY_TOKEN'] = 'testing'
os.environ['AWS_SESSION_TOKEN'] = 'testing'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
```

</div>

<div class="cell markdown">

## Create a Spark Context and Import Dependencies

Now we initialize the spark session with the necessary configurations.

</div>

<div class="cell code">

``` python
import signal
import subprocess
import boto3

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name

S3_MOCK_ENDPOINT = "http://127.0.0.1:5000"
 
spark_session = SparkSession.builder.getOrCreate()
sc = spark_session.sparkContext

sc.setLogLevel('OFF')

sc.setSystemProperty("fs.s3a.bucket.nightly.aws.credentials.provider", "dynamic")

hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.access.key", "mock")
hadoop_conf.set("fs.s3a.secret.key", "mock")
hadoop_conf.set("fs.s3a.endpoint", S3_MOCK_ENDPOINT)
```

</div>

<div class="cell markdown">

## Set Up Moto Server To Access AWS Locally

We start the moto server for AWS s3 resource locally. Don't run it twice
or you will get an error that the server is running already.

</div>

<div class="cell code">

``` python
# setup moto server
process = subprocess.Popen(
    "moto_server", stdout=subprocess.PIPE,
    shell=True, preexec_fn=os.setsid
)
```

</div>

<div class="cell markdown">

## Set AWS Services Variables

Set up AWS variables for region and bucket name.

</div>

<div class="cell code">

``` python
AWS_REGION = "us-east-1"
BUCKET_NAME = "assignment-bucket"
```

</div>

<div class="cell markdown">

## Create AWS S3 Bucket

Running the below cell you will create an s3 bucket which you will use
during the assignment. The name of the bucket is stored in the variable
'BUCKET\_NAME'

</div>

<div class="cell code">

``` python
# create s3 connection, bucket and s3 url's
s3_conn = boto3.resource('s3', region_name=AWS_REGION, endpoint_url=S3_MOCK_ENDPOINT)  

# create s3 bucket 
s3_conn.create_bucket(Bucket=BUCKET_NAME) 
```

</div>

<div class="cell markdown">

## Set a Source and Destination key

The below cell contains the source and destination paths that you will
use later to read and write into the s3 bucket. Please note that the key
is partitioned by date with the format yyyy/mm/dd. Working with this
partition will be important during the assignment.

</div>

<div class="cell code">

``` python
s3_source = [
            "s3a://{}/{}".format(BUCKET_NAME, "data/raw-zone/domain/2022/03/23/"), 
            "s3a://{}/{}".format(BUCKET_NAME, "data/raw-zone/domain/2022/03/25/")
            ]         
s3_destination = "s3a://{}/{}".format(BUCKET_NAME, "data/raw-zone/datalake/") 
```

</div>

<div class="cell markdown">

## Preparing Data

Now we create a data frame with some data that you will use later to be
processed during the assignment. The data is compose by the "Op",
"sys\_change\_timestamp", "id", "name", "created\_at", "updated\_at",
"sys\_data\_source" columns. The meaning of this columns is not
important for the assignment so don't worry to understand it.

</div>

<div class="cell code">

``` python
# create source dataframe and write the dataframe as csv to s3
values =   [("I",
            "2022-03-24 21:00:24.803897",
            "d608fbdb-e975-4b80-89ff-a52c8c4ccca4",
            "Frederick Melendez",
            "2022-03-24 21:00:24.801188",
            "2022-03-24 21:00:24.801194",
            "domain"), 
          
            ("I",
             "2022-03-24 21:00:24.8038654",
             "284bf2dd-c310-4a21-8a4b-eedc20fea675",
             "John Doe",
             "2022-03-24 21:00:24.801188",
             "2022-03-24 21:00:24.801194",
             "domain")]

columns = ["Op", 
           "sys_change_timestamp",
           "id",
           "name",
           "created_at",
           "updated_at",
           "sys_data_source"]

# create spark dataframe
df = spark_session.createDataFrame(values, columns)    
df.show()
```

</div>

<div class="cell markdown">

## Write csv files into the s3

Now let's write the data frame created above into our s3 bucket in csv
format. After this step your first task will start.

</div>

<div class="cell code">

``` python
# Write csv files into the s3
def write_to_s3(s3_source):    
    for path in s3_source:    
        df.write.option("header",True).csv(path)    
```

</div>

<div class="cell code">

``` python
# write to s3 for each path in the list
write_to_s3(s3_source)
```

</div>

<div class="cell markdown">

## Task 1 - S3 Data Verification

</div>

<div class="cell markdown">

Now is the time for your first task. In the below function
"get\_s3\_object()" you have to complete the code to retrieve all
objects for the s3 bucket we created and saved previously in CSV format
files. You have to use AWS SDK for Python (Boto3) to access AWS s3
services and store them in the variable "s3\_client" that we created
below. Use this variable to get all objects in the s3 bucket and return
as "respond". Basically, your task is to return the s3 respond to be
used in the next cell to list all objects. Please note that "s3\_client"
is complete and you just need to use it to get the information from the
s3.

</div>

<div class="cell code">

``` python
## Insert your answer in this cell. DO NOT CHANGE THE NAME OF THE FUNCTION.
def get_s3_object():
    
    s3_client = boto3.client('s3', region_name=AWS_REGION, endpoint_url=S3_MOCK_ENDPOINT)
    
    # YOUR CODE HERE
    
    raise NotImplementedError("Your code should be implement and delete this line of code")
    
    return response
```

</div>

<div class="cell markdown">

Now let's list all obejets:

</div>

<div class="cell code">

``` python
response = get_s3_object()

# Output the bucket names
files = response.get("Contents")
for file in files:
    print(f"file_name: {file['Key']}, size: {file['Size']}")  
```

</div>

<div class="cell markdown">

## Unit Tests

This unit test will help you to know if your answers have been submitted
correctly. If you are familiarized with **assert** in python you know
that if you don't get an exception with the assert message that means
your answer is correct.

</div>

<div class="cell code">

``` python
assert get_s3_object().get("Contents") != None, "Incorrect return value: There are not object for the s3 bucket"
```

</div>

<div class="cell code">

``` python
assert isinstance(get_s3_object(), dict) == True, "Incorrect return value: Function should return a dictionary"
```

</div>

<div class="cell markdown">

## Read data

Now let's prepare for task 2. The below cell will load the s3 data into
a spark data frame that you will use in task 2.

</div>

<div class="cell code">

``` python
df = spark_session.read.format("csv") \
      .option("recursiveFileLookup","true") \
      .option("header", "true") \
      .option("sep", ",") \
      .load("s3a://{}/{}".format(BUCKET_NAME, "data/raw-zone/domain"))

df.show()
```

</div>

<div class="cell markdown">

## Task 2 - Add Partitions

You have to add to the data frame created above 3 new columns: year,
month, and day in addition to the existing ones. You will get the values
or content of these 3 new columns in the s3 partition folder in the
format of "yyyy/mm/dd". For example in the path
"data/raw-zone/domain/2022/03/23/", the 2022/03/23 is the value for your
new columns in the data frame. You will use as value 2022 to create the
column year, 03 for the column month, and 23 for the column day. Example
of the result:

</div>

<div class="cell markdown">

| year | month | day |
| ---- | ----- | --- |
| 2022 | 03    | 23  |

</div>

<div class="cell code">

``` python
## Insert your answer in this cell. DO NOT CHANGE THE NAME OF THE FUNCTION.
def map_function(x):
    # create colunms form path format: year/month/day
    
    # YOUR CODE HERE
    raise NotImplementedError("Your code should be implement and delete this line of code")
    
    return Op,sys_change_timestamp,id,name,created_at,updated_at,sys_data_source,filename,year, month, day
```

</div>

<div class="cell markdown">

The code below will use your answer in the function above.

</div>

<div class="cell code">

``` python
# add file name and path 
df_fn = df.withColumn("filename", input_file_name())

# map using the map_function
df_partions = df_fn.rdd.map(lambda x: map_function(x)).toDF \
(["Op","sys_change_timestamp","id","name","created_at","updated_at","sys_data_source","filename","year","month","day"])
```

</div>

<div class="cell markdown">

## Unit Test

</div>

<div class="cell code">

``` python
assert df_partions.schema.simpleString().find("day") > 0, "Incorrect Return Value: Value obtained does not match"
```

</div>

<div class="cell code">

``` python
assert df_partions.schema.simpleString().find("month") > 0, "Incorrect Return Value: Value obtained does not match"
```

</div>

<div class="cell code">

``` python
assert df_partions.schema.simpleString().find("year") > 0, "Incorrect Return Value: Value obtained does not match"
```

</div>

<div class="cell markdown">

## Task 3 - Write Data to S3 Partitioned In Parquet Format

This is the last task. Here you have to complete the below function to
write the data that you created using the folder partitions into an s3
bucket in parquet format. This data need to be saved partitioned by
"year", "month", and "day" using spark. Example:
year=2022/month=03/day=23

</div>

<div class="cell code" data-scrolled="true">

``` python
## Insert your answer in this cell. DO NOT CHANGE THE NAME OF THE FUNCTION.
def write_to_s3_parquet(data):
    # YOUR CODE HERE
    
    raise NotImplementedError("Your code should be implement and delete this line of code")
```

</div>

<div class="cell code">

``` python
write_to_s3_parquet(df_partions)
```

</div>

<div class="cell code">

``` python
df_q = spark_session.sql("select * from parquet.`{}year=2022/month=03/day=23`".format(s3_destination))
```

</div>

<div class="cell markdown">

## Unit Test

</div>

<div class="cell code">

``` python
assert (not df_q.rdd.isEmpty() == True), "The dataframe is empty"
```

</div>
