package org.learnSpark.application
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object Question1 {
  // Find the DISTINCT number of flight per month
  import Main.spark.implicits._
  def totalFlightsPerMonth(flightData: Dataset[Main.flightData]): Dataset[(String, Long)] = {
    // constraint / assumption: assuming that only one year of data is available. If more than one year, we will need to include the year information. Here we are keeping only the month also to follow the required output format.
    val output = flightData
      .withColumn("month", date_format($"date", "MM"))
      .groupBy("month")
      .agg(countDistinct("flightId").as("totalFlights"))
      .orderBy("month")
      .as[(String, Long)] // output a dataset
    output
  }
}
