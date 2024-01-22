package org.learnSpark.application
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object Question2 {
  def top100FrequentFlyers(spark: SparkSession, flightData: Dataset[main.flightData], passengers: Dataset[main.passengers]): Dataset[(String, Long, String, String)] = {
    import spark.implicits._
    // need to think if this is the best approach? what about scalability?
    val topFreq = flightData
      .groupBy("passengerId")
      .agg(count("flightId").as("NumberOfFlights"))
      .orderBy($"NumberOfFlights".desc)
      .limit(100)
      .as[(String, Long)]

    val output = topFreq
      // we use joinWith instead of join, as it returns a dataSet of tuples
      .joinWith(passengers, topFreq("passengerId") === passengers("passengerId"), "left_outer")
      // we convert the tuple (tb1, tb2) to a new tuple
      .map { case (topFreqRow, passengersRow) =>
        (topFreqRow._1, topFreqRow._2, passengersRow.firstName, passengersRow.lastName)
      }
    // NEED TO RENAME! CURRENTLY STILL WRONG NAMING CONVENTION
    output
  }
}
