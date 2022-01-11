import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object airline_staging_dimension extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("proyecto_idt2021")
    .getOrCreate()


  //dataset
  val airlineCsvPath = "src/datasets/raw_layer/airline"
  val airlineNameDF = spark.read
    .option("sep", ",")
    .option("header", true)
    .option("inferSchema", true)
    .csv(airlineCsvPath)

  val airlineDescriptionCsvPath = "src/datasets/raw_layer/flights"
  val airlineDescDF = spark.read
    .option("sep", ",")
    .option("header", true)
    .option("inferSchema", true)
    .csv(airlineDescriptionCsvPath)


  // Carrier - flights
  val airlineCarrierDF = airlineDescDF.select(col("carrier").alias("code"))


  //Airline Staging Dimension

  import spark.implicits._

  val airlineStagingDF = airlineNameDF.join(airlineCarrierDF, Seq("code"), "left")
    .distinct()
    .withColumn("airline_key", monotonically_increasing_id + 1)
    .withColumnRenamed("Description", "airline_name")
    .select(
      $"airline_key",
      $"code" as "carrier",
      lower($"airline_name") as "airline_name").na.fill("undefined")

  println("\n Airline staging dimension")
  airlineStagingDF.printSchema()
  airlineStagingDF.show(false)

  val qStaging = airlineStagingDF.count()
  print("Airline data- staging", qStaging)


  //Write staging
  val airlineStaging_location = "src/datasets/staging_layer/airlines"
  //sobreescribiendo en parquet
  airlineStagingDF
    .write
    .option("compression", "snappy")
    .format("parquet")
    .mode("overwrite")
    .parquet(airlineStaging_location)


  //test
  val testParquetStaging = spark.read.parquet(airlineStaging_location)
  testParquetStaging.show(false)
  testParquetStaging.printSchema()

  val qParquetStaging = testParquetStaging.select("airline_key").distinct().count()
  println("\n Staging data quantities ", qParquetStaging)


}
