package org.learnSpark.application
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

object Question2 {
  import Main.spark.implicits._
  /* passengerFrequency:
  A complete record of each passenger's frequency of taking flights, arrange in descending order
  It is stored separately as a function on its own as we can foresee this could be a useful function that is reusable.
   */
  def passengerFrequency(flightData: Dataset[Main.flightData]): Dataset[(String, Long)] ={
    val Freq = flightData
      .groupBy("passengerId")
      .agg(countDistinct("flightId").as("numberOfFlights"))
      .orderBy($"numberOfFlights".desc)
      .as[(String, Long)]
    Freq
  }

  // Actual answer for Qn2, where we use passengerFrequency(), extract the top 100 answer and
  def top100FrequentPassengers(flightData: Dataset[Main.flightData], passengers: Dataset[Main.passengers]): Dataset[(String, Long, String, String)] = {
    val Freq = passengerFrequency(flightData)
    val topFreq = Freq
      .limit(100)
      .as[(String, Long)] // storing this as a dataset

    val output = topFreq
      .join(broadcast(passengers.withColumnRenamed("passengerId", "passengerId2")), $"passengerId" === $"passengerId2", "left_outer") // broadcast join, as we assume passengers table will be much smaller than flightData
      .select(
        topFreq("passengerId").as("passengerId"),
        topFreq("numberOfFlights").as("numberOfFlights"),
        passengers("firstName"),
        passengers("lastName")
      ) // renaming
      .as[(String, Long, String, String)] // convert to dataset

    output
  }
}
