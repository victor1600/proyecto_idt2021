import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object airport_staging_dimension extends App{

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("proyecto_idt2021")
    .getOrCreate()


  //Datasets
  val airportsDescriptionCsvPath = "src/datasets/raw_layer/flights"
  val airportsCsvPath = "src/datasets/raw_layer/airports"
  val waCsvPath = "src/datasets/raw_layer/wac"


  val flightsDF = spark.read
    .option("sep", ",")
    .option("header", true)
    .option("inferSchema", true)
    .csv(airportsDescriptionCsvPath)

  val wacDF = spark.read
    .option("sep", ";")
    .option("header", true)
    .option("inferSchema", true)
    .csv(waCsvPath)

  val airportDF = spark.read
    .option("sep", ",")
    .option("header", true)
    .option("inferSchema", true)
    .csv(airportsCsvPath)
  AirportStaging()

  def AirportStaging(): Unit = {
    val flightAirportsDataDF = flightsDF.
      select(
        col("OriginAirportID").alias("code"),
        col("OriginState").alias("state_code"),
        col("OriginStateName").alias("state_name"),
        col("OriginWac").alias("wac")).distinct()


    val wacDataDF = wacDF.select(col("world_area_code").alias("wac"),
      col("country"))

    //Airport data - dataset
    import spark.implicits._
    val airportDataDF = airportDF
      .withColumn("name_city_code", split(col("Description"), ", "))
      .withColumn("city", element_at(col("name_city_code"), 1))
      .withColumn("airport_citycode", element_at(col("name_city_code"), 2)).drop("name_city_code")
      .withColumn("city_code", split(col("airport_citycode"), ": ")).drop("airport_citycode")
      .withColumn("airport_name", element_at(col("city_code"), 2)).drop("city_code")
      .join(flightAirportsDataDF, Seq("code"), "left")
      .join(wacDataDF, Seq("wac"), "left")
      .select(
        lower($"code") as "code",
        lower($"airport_name") as "airport_name",
        lower($"city") as "city",
        lower($"state_code") as "state_code",
        lower($"state_name") as "state_name",
        $"wac",
        lower($"country") as "country").na.fill("undefined")
    airportDataDF.show(false)

    //Write staging
    val airportStaging_location="src/datasets/staging_layer/airport_staging"

    //sobreescribiendo en parquet
    airportDataDF
      .write
      .option("compression", "snappy")
      .format("parquet")
      .mode("overwrite")
      .parquet(airportStaging_location)
  }
}
