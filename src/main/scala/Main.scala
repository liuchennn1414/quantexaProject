package org.learnSpark.application
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.log4j.{Level, Logger}

import java.sql.Date
import java.text.SimpleDateFormat

object Main{

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

  def main(args: Array[String]): Unit = {
    // Initialising Spark session
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName(name = "quantexaProject").master(master = "local").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Read CSV files

    val flightData = ReadCSV.readFlight(spark, "src/resources/flightData.csv")

    val passengersData = ReadCSV.readPassenger(spark, "src/resources/passengers.csv")

    /*
    // Question 1
    val Q1Ans = Question1.totalFlightsPerMonth(flightData)(spark)
    println("------ Question 1 ------")
    Q1Ans.show(5)
    /*
    Q1Ans.coalesce(1)
      .write
      .format("csv")
      .option("header", "true") // Write header in the CSV file
      .mode("overwrite") // Overwrite the file if it already exists
      .save("/Users/liuchen/Desktop/quantexaProject/src/output/Question1.csv")
     */


    // Question 2
    val Q2Ans = Question2.top100FrequentPassengers(flightData, passengersData)(spark)
    println("------ Question 2 ------")
    Q2Ans.show(5)
    /*
    Q2Ans.coalesce(1)
      .write
      .format("csv")
      .option("header", "true") // Write header in the CSV file
      .mode("overwrite") // Overwrite the file if it already exists
      .save("/Users/liuchen/Desktop/quantexaProject/src/output/Question2.csv")
     */

    // Question 3
    val Q3Ans = Question3.output(flightData)(spark)
    println("------ Question 3 ------")
    Q3Ans.show(5)
    /*
    Q3Ans.coalesce(1)
      .write
      .format("csv")
      .option("header", "true") // Write header in the CSV file
      .mode("overwrite") // Overwrite the file if it already exists
      .save("/Users/liuchen/Desktop/quantexaProject/src/output/Question3.csv")
     */


    // Question 4
    val Q4Ans = Question4.output()(spark, flightData)
    println("------ Question 4 ------")
    Q4Ans.show(5)
    /*
    Q4Ans.coalesce(1)
      .write
      .format("csv")
      .option("header", "true") // Write header in the CSV file
      .mode("overwrite") // Overwrite the file if it already exists
      .save("/Users/liuchen/Desktop/quantexaProject/src/output/Question4.csv")
     */


    // Q4 - Bonus Question
    println("------ Bonus Question ------")
    // Example date strings
    val fromDateString = "2017-01-01"
    val toDateString = "2017-04-30"

    // Define the date format
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

    // Parse the strings to obtain java.util.Date objects

    val fromDate: Option[Date] = Option(dateFormat.parse(fromDateString)).map(d => new java.sql.Date(d.getTime))

    val toDate: Option[Date] = Option(dateFormat.parse(toDateString)).map(d => new java.sql.Date(d.getTime))

    val Bonus = Question4.flownTogether(6,fromDate, toDate)(spark,flightData)

    Bonus.show(5)

     */

    // Stop the Spark session
    spark.stop()
  }
}
