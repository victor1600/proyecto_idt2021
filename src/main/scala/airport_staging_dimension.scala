import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object airport_staging_dimension extends App{

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("proyecto_idt2021")
    .getOrCreate()


  //Datasets
  val airportsDescriptionCsvPath="src/datasets/raw_layer/flights"
  val airportsCsvPath="src/datasets/raw_layer/airport_name_dataset.csv"
  val waCsvPath="src/datasets/raw_layer/wac_dataset.csv"



  val airportInfoDF = spark.read
    .option("sep", ",")
    .option("header", true)
    .option("inferSchema", true)
    .csv(airportsDescriptionCsvPath)

  val wacDF = spark.read
    .option("sep", ";")
    .option("header", true)
    .option("inferSchema", true)
    .csv(waCsvPath)

  val airportNameDF = spark.read
    .option("sep", ",")
    .option("header", true)
    .option("inferSchema", true)
    .csv(airportsCsvPath)




  //Airport data
  val airportDataDF = airportNameDF
    .withColumn("airport_key", monotonically_increasing_id+ 1)
    .withColumn("name_city_code", split(col("Description"), ", "))
    .withColumn("city", element_at(col("name_city_code"), 1))
    .withColumn("airport_citycode", element_at(col("name_city_code"), 2)).drop("name_city_code")
    .withColumn("city_code", split(col("airport_citycode"), ": ")).drop("airport_citycode")
    .withColumn("airport_name", element_at(col("city_code"), 2)).drop("city_code")
    .drop("Description")

  //Test
  airportDataDF.printSchema()
  airportDataDF.show(10,false)
  val qAirport = airportDataDF.select("code").count()
  print("Airport quantities", qAirport)





  //Wac data
  val wacDataDF = wacDF.select(col("world_area_code").alias("wac"),
    col("country"))

  //Test
  wacDataDF.printSchema()
  wacDataDF.show(10,false)
  val qw = wacDataDF.select("wac").count()
  print("WAC quantities", qw)





  //Flight data
  val flightAirportsData = airportInfoDF.select(col("OriginAirportID").alias("code"),
   // col("OriginCityName").alias("city"),
    col("OriginState").alias("state_code"),
    col("OriginStateName").alias("state_name"),
    col("OriginWac").alias("wac")).distinct()
    .drop(col("_c109"))


  //Test
  flightAirportsData.printSchema()
  flightAirportsData.show(10,false)
  val qFlightsA = flightAirportsData.select("code").count()
  print("Flight quantities", qFlightsA)




  //Flight with WAC
  val flightAirportData = flightAirportsData.join(wacDataDF, Seq("wac"), "left")

  //Test
  flightAirportData.printSchema()
  flightAirportData.show(10, false)
  val qFlightA = flightAirportsData.select("code").count()
  print("Flight with WAC quantities", qFlightA)




  //Airport Staging Dimension
  import spark.implicits._
  val airportStaging = airportDataDF.join(flightAirportData, Seq("code"), "left")
  .select(
    $"airport_key",
    lower($"code") as "code",
    lower($"airport_name") as "airport_name",
    lower($"city") as "city",
    lower($"state_code") as "state_code",
    lower($"state_name") as "state_name",
    $"wac".cast("String"),
    lower($"country") as "country").na.fill("UNDEFINED")

  //Test
  airportStaging.printSchema()
  airportStaging.show(20, false)
  val qStaging = airportStaging.select("airport_key").count()
  print("Airport data- staging", qStaging)



  //Write staging
  val airportStaging_location="src/datasets/staging_layer/airports"
  //sobreescribiendo en parquet
  airportStaging
    .write
    .option("compression", "snappy")
    .format("parquet")
    .mode("overwrite")
    .parquet(airportStaging_location)


  //test
  val testParquetStaging = spark.read.parquet(airportStaging_location)
  testParquetStaging.show(false)
  testParquetStaging.printSchema()

  val qParquetStaging = testParquetStaging.select("code").distinct().count()
  println("\n Staging data quantities ", qParquetStaging )

}
