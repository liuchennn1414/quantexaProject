import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_list, concat, udf}
import org.learnSpark.application.Main

val spark = SparkSession.builder().appName( name = "Test").master(master = "local").getOrCreate()

val sampleData = Seq(
  (148, Seq("co", "ir", "uk", "au", "nl", "uk", "cn")),
  (463, Seq("uk", "pk", "uk", "ca", "iq", "tk")),
  // Add more sample data...
)

println(sampleData)
val sampleDF = spark.createDataFrame(sampleData).toDF("passengerId", "routes")
println(sampleDF)


def getLongestNonUKChain(route:Seq[String]): Int = {
  // User Defined Function to count consecutive cities without visiting UK
  val (maxLength, currentLength, visitedSet) = route.foldLeft((0,0,Set.empty[String])) {
    case((maxLength, currentLength,visitedSet), city) =>
      if (city.toLowerCase() != "uk") { // if the city is not UK
        if (visitedSet.contains(city.toLowerCase())){ // and if the city has been visited before within this run
          println("Repeated country, no update")
          (maxLength, currentLength, visitedSet) // do not update anything
        }else { // if the city is not UK and have not been visited before within this run
          println("Add city")
          (Math.max(maxLength, currentLength + 1), currentLength + 1, visitedSet + city.toLowerCase()) // update the max length and current length
        }
      }else{
        println("UK, restarting the run")
        (Math.max(maxLength, currentLength),0,Set.empty[String]) // clean up the visited list if visited UK
      }
  }
  println(maxLength)
  maxLength
}

val countRun = udf(getLongestNonUKChain _)

// Test the UDF with the sample data
val testResult = sampleDF.withColumn("LongestRun",countRun(col("routes")))
// testResult.show(5)

////////////////////////////////////////////////////////////////
/*
val passengerRoute = flightData
  .orderBy("passengerId","date","flightId") // if there are more than one flight on one day, assume it follows flightId order
  .groupBy("passengerId")
  .agg(
    // an easy way will be just use first("from") and concat with collect_list("to"), but this is assuming that the route must be consecutive and no "jumping" of city is involved, which is not the case in real life as people can choose other transportation
    first("from").as("firstFrom"),
    collect_list("to").as("routes"))
  .withColumn("routes",concat(array($"firstFrom"), $"routes").as("routes"))
  .drop("firstFrom")
  .as[PassengerRoute]
 */
