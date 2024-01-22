import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

val spark = SparkSession.builder().appName( name = "Test").master(master = "local").getOrCreate()

val sampleData = Seq(
  (148, Seq("co", "ir", "au", "nl", "uk", "cn", "a")),
  (463, Seq("tk", "pk", "ca", "uk", "iq")),
  // Add more sample data...
)

println(sampleData)
val sampleDF = spark.createDataFrame(sampleData).toDF("passengerId", "routes")
println(sampleDF)


def getLongestNonUKChain(route:Seq[String]): Int = {
  println("start a new row")
  // User Defined Function to count consecutive cities without visiting UK
  val (_, maxLength, currentLength) = route.foldLeft((false,0,0)) {
    case((inChain, maxLength, currentLength), city) =>
      if (city != "uk") {
        println("incrementing")
        (true, maxLength, currentLength + 1)
      }else{
        println("Encountered UK")
        (false, Math.max(maxLength, currentLength),0)
      }
  }
  print(maxLength)
  maxLength
}

val countRun = udf(getLongestNonUKChain _)

// Test the UDF with the sample data
val testResult = sampleDF.withColumn("LongestRun",countRun(col("routes")))
testResult.show(5)

spark.stop()
