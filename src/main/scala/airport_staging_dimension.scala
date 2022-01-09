import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object airport_staging_dimension extends App{

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("proyecto_idt2021")
    .getOrCreate()


  //Datasets
  val airportsDescriptionCsvPath="src/datasets/flights"
  val airportsCsvPath="src/datasets/airport_name_dataset.csv"
  val waCsvPath="src/datasets/wac_dataset.csv"


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
  val airportData = airportInfoDF.select(col("OriginAirportID").alias("code"),
    col("OriginCityName").alias("city"),
    col("OriginState").alias("state_code"),
    col("OriginStateName").alias("state_name"),
    col("OriginWac").alias("wac"))
    .drop(col("_c109")).distinct()
    .orderBy("city")
  airportData.printSchema()
  airportData.show(10,false)


//Wac data
  val wacDataDF = wacDF.select(col("world_area_code").alias("wac"),
    col("country"))
  wacDataDF.printSchema()
  wacDataDF.show(10,false)


  //Airport Dimension
  import spark.implicits._
  val airportDim = airportData.join(airportNameDF, Seq("Code"), "inner")
    .join(wacDataDF, Seq("wac"), "inner")
    .withColumn("airport_key", monotonically_increasing_id+ 1)
    .withColumn("name_city_code", split(col("Description"), ", "))
    .withColumn("airport_citycode", element_at(col("name_city_code"), 2)).drop("name_city_code")
    .withColumn("city_code", split(col("airport_citycode"), ": ")).drop("airport_citycode")
    .withColumn("airport_name", element_at(col("city_code"), 2)).drop("city_code")
    .withColumn("start_date",lit(java.time.LocalDate.now))
    .withColumn("end_date", to_date(lit("9999-12-31")))
    .withColumn("current_flag",  lit(true))
    .drop("Description")
    .select(
      lower($"code") as "code",
      lower($"airport_key") as "airport_key",
      lower($"airport_name") as "airport_name",
      lower($"city") as "city",
      lower($"state_code") as "state_code",
      lower($"state_name") as "state_name",
      $"wac",
      lower($"country") as "country",
      $"start_date",
      $"end_date",
      $"current_flag"
    )
  airportDim.printSchema()
  airportDim.show(10,false)


  val airportOutputPath="src/datasets/airport.parquet"
  //sobreescribiendo en parquet
  airportDim
    .write
    .option("compression", "snappy")
    .mode("overwrite")
    .parquet(airportOutputPath)

}
