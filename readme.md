## Project Title - Twitter ETL with Spark structured streaming

## Project Description 

### Reading twitter data which is published into kafka topic through twitter streaming,
### and applying the transformations to perform data cleansing and storing the transformed data into
### Cassandra table.

## Dependencies required:

Spark-Core - 3.0.1
Spark-Sql - 3.0.1
Spark-Streaming - 3.0.1
scalatest - 3.0.8
Spark-streaming-kafka-0-10 - 3.0.1
Spark-sql-kafka-0-10 - 3.0.1
spark-cassandra-connector - 3.0.0
joda-time - 2.3

## Plugins Used

Assembly - 0.15.0

## How To 

    First Clone this https://github.com/cheluvesha/Twitter_Streaming_Twitter4j.git
    repositary which performs the streaming of twitter data and publish that data into kafka.
    
    After that clone this repositary,
    to create jar make use of sbt and use this sbt assembly command to create fat jar, 
    sbt assembly which will include all dependencies into jar file but spark dependencies has 
    to be in provided scope (spark dependencies are no need to include in the jar files)

    after creating jar submit your application with spark-submit as
    /opt/spark/bin/spark-submit --class com.twitterETL.TwitterETL --master local[*] target/scala-2.12/Twitter_ETL_Analysis-assembly-0.1.jar 
    twitter tweets ./Resources/twitterSchema.json
    
    jar file which takes 3 arguments as an input
    args(0) - keyspace name
    args(1) - table name
    args(2) - sample twitter json file 
     