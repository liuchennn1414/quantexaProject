package org.learnSpark.application
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object Question2 {
  /* passengerFrequency:
  A complete record of each passenger's frequency of taking flights, arrange in descending order
  It is stored separately as a function on its own as we can foresee this could be a useful function that is reusable.
   */
  def passengerFrequency(flightData: Dataset[Main.flightData])(implicit spark: SparkSession): Dataset[(String, Long)] ={
    import spark.implicits._
    val Freq = flightData
      .groupBy("passengerId")
      .agg(countDistinct("flightId").as("numberOfFlights"))
      .orderBy($"numberOfFlights".desc)
      .as[(String, Long)]
    Freq
  }

  // Actual answer for Qn2, where we use passengerFrequency(), extract the top 100 answer and
  def top100FrequentPassengers(flightData: Dataset[Main.flightData], passengers: Dataset[Main.passengers])(implicit spark: SparkSession): Dataset[(String, Long, String, String)] = {
    import spark.implicits._
    val Freq = passengerFrequency(flightData)(spark)
    val topFreq = Freq
      .limit(100)
      .as[(String, Long)] // storing this as a dataset

    val output = topFreq
      .joinWith(passengers, topFreq("passengerId") === passengers("passengerId"), "left_outer") // we use joinWith instead of join, as it returns a dataSet of tuples instead of dataframe
      .map { case (topFreqRow, passengersRow) =>
        (topFreqRow._1, topFreqRow._2, passengersRow.firstName, passengersRow.lastName) // we convert the tuple (tb1, tb2) to a new tuple
      }
      .toDF("passengerId", "numberOfFlights", "firstName", "lastName") // to do renaming, might need to convert to Dataframe first
      .as[(String, Long, String, String)] // then convert back to dataset
    output
  }
}
