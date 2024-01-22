package org.learnSpark.application
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders
import java.sql.Date


object Question4 {
  case class joinFlight(passenger1Id: Int, passenger2Id: Int, flightId: Int, date: Date)
  case class commonFlightCount(passenger1Id: Int, passenger2Id: Int, commonCount: Long, from: Date, to: Date)

  def joinedFlight(flightData: Dataset[main.flightData]): Dataset[joinFlight] = {
    val joinedFlights = flightData.as("f1")
      // join based on flightId, passenger id 1 < 2 to avoid duplicates & cross product with self
      .join(flightData.as("f2"), col("f1.flightId") === col("f2.flightId") && col("f1.passengerId") < col("f2.passengerId"))
      .select(col("f1.passengerId").alias("passenger1Id"),
        col("f2.passengerId").alias("passenger2Id"),
        col("f1.flightId"),
        col("f1.date")
      ).as[joinFlight](Encoders.product[joinFlight])

    joinedFlights
  }

  def commonFlight(flightData: Dataset[main.flightData]): Dataset[commonFlightCount] = {
    val filteredCommonFlight = joinedFlight(flightData)
      .groupBy("passenger1Id","passenger2Id")
      .agg(count("*").alias("commonCount"), min("date").alias("from"), max("date").alias("to"))
      .as[commonFlightCount](Encoders.product[commonFlightCount])
    filteredCommonFlight
  }

  def output(flightData: Dataset[main.flightData]): DataFrame = {
    val commonFlightDS = commonFlight(flightData)
    val output = commonFlightDS
      .filter(col("commonCount") > 3)
      .orderBy(col("commonCount").desc)
      .select("passenger1Id", "passenger2Id", "commonCount")

    output
  }

  def flownTogether(flightData: Dataset[main.flightData], atLeastNTimes: Int, from: Date, to: Date): Dataset[commonFlightCount] = {
    val filteredCommonFlight = joinedFlight(flightData)
    val result = filteredCommonFlight
      .filter(col("date").between(from,to))
      .groupBy("passenger1Id","passenger2Id")
      .agg(count("*").alias("commonCount"), min("date").alias("from"), max("date").alias("to"))
      .filter(col("commonCount") > atLeastNTimes)
      .orderBy(col("commonCount").desc)
      .as[commonFlightCount](Encoders.product[commonFlightCount])
    result
  }

}
