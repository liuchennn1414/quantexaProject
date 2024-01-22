package org.learnSpark.application
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object Question1 {
  def totalFlightsPerMonth(spark: SparkSession, flightData: Dataset[readCSV.flightData]): Dataset[(String, Long)] = {
    flightData
      .withColumn("month", date_format($"date", "MM"))
      .groupBy("month")
      .agg(count("flightId").as("totalFlights"))
      .as[(String, Long)]
  }
}
