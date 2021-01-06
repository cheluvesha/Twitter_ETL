package com.Utility

import org.apache.spark.sql.SparkSession

object UtilityClass {

  /***
    * Creates SparkSession object
    * @param appName String
    * @return SparkSession
    */
  def createSparkSessionObj(appName: String): SparkSession = {
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(appName)
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .config(
        "spark.sql.extensions",
        "com.datastax.spark.connector.CassandraSparkExtensions"
      )
      .config(
        "spark.sql.catalog.lh",
        "com.datastax.spark.connector.datasource.CassandraCatalog"
      )
      .config("spark.streaming.stopGracefullyOnShutdown", value = true)
      .getOrCreate()
    sparkSession
  }
}
