package org.learnSpark.application
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.log4j.{Level, Logger}

import java.sql.Date
import java.text.SimpleDateFormat


object Main {

  // Initialising Spark session
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  implicit val spark = SparkSession.builder().appName(name = "quantexaProject").master(master = "local").getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  // Create flight and passenger class

  case class flightData(passengerId: Int, flightId: Int, from: String, to: String, date: Date)
  case class passengers(passengerId: Int, firstName: String, lastName: String)

  val flightSchema = StructType(Array(
    StructField("passengerId", IntegerType, true),
    StructField("flightId", IntegerType, true),
    StructField("from", StringType, true),
    StructField("to", StringType, true),
    StructField("date", DateType, true)
  ))

  val passengerSchema = StructType(Array(
    StructField("passengerId", IntegerType, true),
    StructField("firstName", StringType, true),
    StructField("lastName", StringType, true)
  ))

  // Read CSV files

  val flightDataSet = ReadCSV.readFlight("src/resources/flightData.csv")

  val passengersDataSet = ReadCSV.readPassenger("src/resources/passengers.csv")

  // main function
  def main(args: Array[String]): Unit = {

    if (args.nonEmpty) {
      // give input to main to trigger flownTogether
      val functionArgs = args.tail
      val atLeastNTimes = functionArgs(0).toInt
      val fromDateString = functionArgs.lift(1)
      val toDateString = functionArgs.lift(2)
      println(s"Output for flownTogether($atLeastNTimes,$fromDateString,$toDateString): ")
      val output = flownTogether(atLeastNTimes,fromDateString,toDateString)
      output.show(5)

    } else { // no additional input then just give first 4
      // Question 1
      val Q1Ans = Question1.totalFlightsPerMonth(flightDataSet)
      println("------ Question 1 ------")
      Q1Ans.show(5)

      // Question 2
      val Q2Ans = Question2.top100FrequentPassengers(flightDataSet, passengersDataSet)
      println("------ Question 2 ------")
      Q2Ans.show(5)

      // Question 3
      val Q3Ans = Question3.output(flightDataSet)
      println("------ Question 3 ------")
      Q3Ans.show(5)

      // Question 4
      val Q4Ans = Question4.output(flightDataSet)
      println("------ Question 4 ------")
      Q4Ans.show(5)
    }

    // Stop the Spark session
    spark.stop()
  }

  // Additional Qn from Q4
  // Allow users to input string only and perform the conversion to date
  def flownTogether(atLeastNTimes: Int, fromDateString: Option[String], toDateString: Option[String]): Dataset[Question4.flightTogetherData] = {
    // Parse the strings to obtain java.util.Date objects
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val fromDate = fromDateString.map(d => new java.sql.Date(dateFormat.parse(d).getTime))
    val toDate = toDateString.map(d => new java.sql.Date(dateFormat.parse(d).getTime))
    //val fromDate = Option(dateFormat.parse(fromDateString)).map(d => new java.sql.Date(d.getTime))
    //val toDate = Option(dateFormat.parse(toDateString)).map(d => new java.sql.Date(d.getTime))

    val Bonus = Question4.flownTogetherCount(flightDataSet, atLeastNTimes, fromDate, toDate)
    Bonus
  }
}
