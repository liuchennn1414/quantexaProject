package org.learnSpark.application
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object Question1 {
  // Find the DISTINCT number of flight per month
  def totalFlightsPerMonth(flightData: Dataset[Main.flightData])(implicit spark: SparkSession): Dataset[(String, Long)] = {
    import spark.implicits._
    val output = flightData
      .withColumn("month", date_format($"date", "MM"))
      .groupBy("month")
      .agg(countDistinct("flightId").as("totalFlights"))
      .orderBy("month")
      .as[(String, Long)] // output a dataset
    output
  }
}
