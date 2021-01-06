package com.twitterETL

import com.Utility.UtilityClass
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object TwitterETL {
  val sparkSession: SparkSession =
    UtilityClass.createSparkSessionObj("Twitter ETL")
  val logger: Logger = Logger.getLogger(getClass.getName)
  sparkSession.udf.register("removeWords", remove)

  /** *
    * Reads Data From Kafka Topic
    * @param broker String
    * @param topic  String
    * @return DataFrame
    */
  def readDataFromKafka(broker: String, topic: String): DataFrame = {
    logger.info("Reading Data From Kafka Topic")
    try {
      val kafkaDF = sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
      kafkaDF
    } catch {
      case nullPointerException: NullPointerException =>
        logger.error(nullPointerException.printStackTrace())
        throw new Exception("Passed Fields Are Null")
    }
  }

  /** *
    * Extracts Schema From Twitter Sample Json File
    * @param filePath String
    * @return StructType
    */
  def extractSchemaFromTwitterData(filePath: String): StructType = {
    logger.info("Extracting Schema From Twitter Json File")
    try {
      val twitterData = sparkSession.read
        .json(filePath)
        .toDF()
      twitterData.schema
    } catch {
      case nullPointerException: NullPointerException =>
        logger.error(nullPointerException.printStackTrace())
        throw new Exception("Can not create a Path from a null string")
      case fileNotFoundException: org.apache.spark.sql.AnalysisException =>
        logger.error(fileNotFoundException.printStackTrace())
        throw new Exception("Twitter Sample file not exist")
    }
  }

  /** *
    * Casting, Applying  Schema and Selecting Required Columns From The Kafka DataFrame
    * @param kafkaDF DataFrame
    * @param schema  StructType
    * @return
    */
  def processKafkaDataFrame(
      kafkaDF: DataFrame,
      schema: StructType
  ): DataFrame = {
    logger.info("Processing The Kafka DataFrame")
    try {
      val twitterStreamDF = kafkaDF
        .selectExpr("CAST(value AS STRING) as jsonData")
        .select(from_json(col("jsonData"), schema).as("data"))
        .select(col("data.retweeted_status") as "tweet")
      val tweetDF =
        twitterStreamDF.select(col("tweet.text") as "tweet_string")
      tweetDF
    } catch {
      case sparkAnalysisException: org.apache.spark.sql.AnalysisException =>
        logger.error(sparkAnalysisException.printStackTrace())
        throw new Exception("Unable to Execute Query")
    }
  }

  /** *
    * Writes DataFrame into MYSQL table
    * @param hashTagDF DataFrame
    */
  def writeDataFrame(
      hashTagDF: DataFrame,
      keySpace: String,
      table: String
  ): Unit = {
    logger.info("Writing the Streaming context DataFrame")
    val query = hashTagDF.writeStream
      .outputMode("append")
      .foreachBatch((outPutDF: DataFrame, batchId: Long) => {
        writeToCassandra(outPutDF, batchId, keySpace, table)
      })
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()
    query.awaitTermination(60000)
  }

  /***
    * Writing Data to Cassandra database
    * @param outputDF DataFrame
    * @param batchId Long
    * @param keySpace String
    * @param table String
    */
  def writeToCassandra(
      outputDF: DataFrame,
      batchId: Long,
      keySpace: String,
      table: String
  ): Unit = {
    logger.info(
      "Writing the DataFrame into Cassandra DB for the batch: " + batchId
    )
    try {
      outputDF.show(false)
      outputDF.write
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", keySpace)
        .option("table", table)
        .mode("append")
        .save()
    } catch {
      case exception: Exception =>
        logger.error(exception.printStackTrace())
        throw new Exception("Unable to write data to cassandra table")
    }
  }

  /** *
    * UDF for removing unwanted words from hashtag field
    * @return String
    */
  def remove: String => String =
    (words: String) => {
      var removeText: String = null
      if (words != null) {
        removeText = words
          .replaceAll("""(\b\w*RT)|[^a-zA-Z0-9\s\.\,\!,\@]""", "")
          .replaceAll("(http\\S+)", "")
          .replaceAll("(@\\w+)", "")
          .replaceAll("(Bla)", "")
          .replaceAll("\\s{2,}", " ")
      } else {
        removeText = "null"
      }
      removeText
    }

  /** *
    * Removing the unwanted words from Hashtags field by applying UDF
    * @param tweetTextDF DataFrame
    * @return DataFrame
    */
  def removeUnwantedWords(tweetTextDF: DataFrame): DataFrame = {
    logger.info("Removing the unwanted words from tweet field")
    try {
      tweetTextDF.createTempView("remove_words")
      val removedWordsDF = sparkSession.sql(
        """select removeWords(tweet_string) as text from remove_words"""
      )
      removedWordsDF
    } catch {
      case sparkAnalysisException: org.apache.spark.sql.AnalysisException =>
        logger.error(sparkAnalysisException.printStackTrace())
        throw new Exception("Unable to Execute Query")
    }
  }

  // Entry Point to the Application
  def main(args: Array[String]): Unit = {
    val broker = System.getenv("BROKER")
    val topic = System.getenv("TOPIC")
    val keySpace = args(0)
    val table = args(1)
    val kafkaDF = readDataFromKafka(broker, topic)
    val schema = extractSchemaFromTwitterData(
      args(2)
    )
    val tweetDF = processKafkaDataFrame(kafkaDF, schema)
    val removeWordsDF = removeUnwantedWords(tweetDF)
    writeDataFrame(removeWordsDF, keySpace, table)

  }
}
