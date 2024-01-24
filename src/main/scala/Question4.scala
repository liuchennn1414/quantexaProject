package org.learnSpark.application
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import java.sql.Date


object Question4 {

  import Main.spark.implicits._

  // create two new case class which could be used later in this Qn
  case class joinedFlightData(passenger1Id: Int, passenger2Id: Int, flightId: Int, date: Date)
  case class flightTogetherData(passenger1Id: Int, passenger2Id: Int, flightsTogetherCount: Long, from: Date, to: Date)

  // flightData cross join itself to obtain pairwise customer information
  def joinedFlight(flightData: Dataset[Main.flightData]): Dataset[joinedFlightData] = {
    val joinedFlightDataset = flightData.as("f1")
      // cross join based on flightId, passenger id 1 < 2 to avoid duplicates / cross product with self
      .join(flightData.as("f2"), col("f1.flightId") === col("f2.flightId") && col("f1.passengerId") < col("f2.passengerId"))
      .select(col("f1.passengerId").alias("passenger1Id"),
        col("f2.passengerId").alias("passenger2Id"),
        col("f1.flightId"),
        col("f1.date")
      ).as[joinedFlightData]
    joinedFlightDataset
  }

  // For Bonus Qn -> Modified from and to to be optional input so that this function can be reused.
  def flownTogetherCount(flightData: Dataset[Main.flightData], atLeastNTimes: Int, fromDate: Option[Date] = None, toDate: Option[Date] = None): Dataset[flightTogetherData] = {
    val joinedFlightDataset = joinedFlight(flightData)

    // we want to ensure this function can be reused even for Qn4's original output, hence, if the user do not input a value for date, we set date range to be the full range in the dataset
    val minDate = fromDate.getOrElse {
      val minDateRow = joinedFlightDataset.agg(min("date")).head() // if no input, choose the minimum date value
      minDateRow.getAs[Date](0)
    }

    val maxDate = toDate.getOrElse {
      val maxDateRow = joinedFlightDataset.agg(max("date")).head() // if no input, choose the maximum date value
      maxDateRow.getAs[Date](0)
    }

    val result = joinedFlightDataset
      .filter(col("date").between(minDate,maxDate)) // first filter the data to the range required
      .groupBy("passenger1Id","passenger2Id")
      //assuming that flightId is unique for each flight, thus, a passenger can only be on a flight for once.
      .agg(count("*").alias("flightsTogetherCount"), min("date").alias("from"), max("date").alias("to")) // within this date range, find the number of shared flight
      .filter(col("flightsTogetherCount") > atLeastNTimes)
      .orderBy(col("flightsTogetherCount").desc)
      .as[flightTogetherData]
    result
  }

  def output(flightData: Dataset[Main.flightData]): Dataset[(Int, Int, Long)] = {

    val flightTogetherData = flownTogetherCount(flightData,3) // no input of date, so default is to choose the full range of date

    val output = flightTogetherData // since the output is already in the desired format, we just need to select the required columns
      .select("passenger1Id", "passenger2Id", "flightsTogetherCount")
      .as[(Int, Int, Long)]
    output
  }

}
