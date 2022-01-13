import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object flight_fact extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("proyecto_idt2021")
    .getOrCreate()


  //Datasets

  //Airline dimension
  val airlinePath = "src/datasets/presentation_layer/airline_dimension"
  val airlineDF = spark.read
    .parquet(airlinePath)

  //Airport dimension
  val airportPath = "src/datasets/presentation_layer/airport_dimension"
  val airportDF = spark.read
    .parquet(airportPath)

  //Plane dimension
  val planePath = "src/datasets/presentation_layer/plane_dimension"
  val planeDF = spark.read
    .parquet(planePath)
  planeDF.show(5, false)

  //Dataset flights
  val flightsPath = "src/datasets/raw_layer/flights"
  val flightsDF = spark.read
    .option("sep", ",")
    .option("header", true)
    .option("inferSchema", true)
    .csv(flightsPath)


  //Dataset time
  val timePath = "src/datasets/raw_layer/time"
  val timeDF = spark.read
    .option("sep", ";")
    .option("header", true)
    .option("inferSchema", true)
    .csv(timePath)


  //Dataset date
  val datePath = "src/datasets/raw_layer/date"
  val dateDF = spark.read
    .option("sep", ";")
    .option("header", true)
    .option("inferSchema", true)
    .csv(datePath)


  import spark.implicits._
  //Origin airport

  //airport
  val airportOriginDF = airportDF.select(
    $"airport_key" as "origin_airport_key",
    $"code" as "OriginAirportID",
    $"airport_name")

  val airportDestDF = airportDF.select(
    $"airport_key" as "dest_airport_key",
    $"code" as "DestAirportID")
    .where(col("current_flag") === true)

  val airportDiv1DF = airportDF.select(
    $"airport_key" as "div1_airport_key",
    $"code" as "Div1AirportID")
    .where(col("current_flag") === true)

  val airportDiv2DF = airportDF.select(
    $"airport_key" as "div2_airport_key",
    $"code" as "Div2AirportID")
    .where(col("current_flag") === true)


  //Plane
  val planeDiv1DF = planeDF.select(
    $"plane_key" as "div1_plane_key",
    $"n_number" as "Div1TailNum")
    .where(col("current_flag") === true)

  val planeDiv2DF = planeDF.select(
    $"plane_key" as "div2_plane_key",
    $"n_number" as "Div2TailNum")
    .where(col("current_flag") === true)



  //Airline
  val airlineCurrentDF = airlineDF.select(
    $"airline_key",
    $"carrier")
    .where(col("current_flag") === true )


  //plane
  val planeCurrentDF = planeDF.select(
    $"plane_key",
    $"n_number" as "TailNum")
    .where(col("current_flag") === true )


// Converter hour type int as hour and minutes - DEPTIME
  val flightHourDF =
    flightsDF.select(col("DepTime").cast("string"))
      .withColumn("hourUnion", split(col("DepTime"), ""))
      .withColumn("hour1", element_at(col("hourUnion"), 1 ))
      .withColumn("hour2", element_at(col("hourUnion"), 2 ))
      .withColumn("min1", element_at(col("hourUnion"), 3 ))
      .withColumn("min2", element_at(col("hourUnion"), 4 ))
      .withColumn("hour",
        when(concat($"hour1", $"hour2") === "24", "0").otherwise(concat($"hour1", $"hour2")))
      .withColumn("minute",
        when(concat($"min1", $"min2") === "00", "0").otherwise(concat($"min1", $"min2")))
      .withColumn("second", lit(0))
      flightHourDF.printSchema()
      flightHourDF.show(5,false)

      val timeDimensionDF = timeDF
     .join(flightHourDF, timeDF("hour") === flightHourDF("hour") &&
       timeDF("minute") === flightHourDF("minute") &&
       timeDF("second") === flightHourDF("second")
       , "inner")
        .select(
        flightHourDF.col("hour"),
        flightHourDF.col("minute"),
        flightHourDF.col("second"),
        flightHourDF.col("DepTime"),
        timeDF.col("time_key").alias( "actual_dep_time_key")
      ).distinct()



  // Converter hour type int as hour and minutes - CRSDEPTIME
  val flightHourCrsDF =
    flightsDF.select(col("CRSDepTime").cast("string"))
      .withColumn("hourUnion", split(col("CRSDepTime"), ""))
      .withColumn("hour1", element_at(col("hourUnion"), 1 ))
      .withColumn("hour2", element_at(col("hourUnion"), 2 ))
      .withColumn("min1", element_at(col("hourUnion"), 3 ))
      .withColumn("min2", element_at(col("hourUnion"), 4 ))
      .withColumn("hour",
        when(concat($"hour1", $"hour2") === "24", "0").otherwise(concat($"hour1", $"hour2")))
      .withColumn("minute",
        when(concat($"min1", $"min2") === "00", "0").otherwise(concat($"min1", $"min2")))
      .withColumn("second", lit(0))
  flightHourCrsDF.printSchema()
  flightHourCrsDF.show(5,false)


  //Time
  val timeDimensionCrsDF = timeDF
    .join(flightHourCrsDF, timeDF("hour") === flightHourCrsDF("hour") &&
      timeDF("minute") === flightHourCrsDF("minute") &&
      timeDF("second") === flightHourCrsDF("second")
      , "inner")
    .select(
      flightHourCrsDF.col("hour").alias("hourcrs"),
      timeDF.col("time_key").alias( "crs_dep_time_key"),
      flightHourCrsDF.col("CRSDepTime")
    )
  timeDimensionCrsDF.show(5,false)



  //Compare dates
       val dateDimensionDF = dateDF
         .withColumn("dateF", col("full_date_description"))
         .select("dateF", "date_key")
         dateDimensionDF.printSchema()
         dateDimensionDF.show(5, false)

  //Fact Table
       val flightDimDF = flightsDF
         .withColumn("dateF", date_format(col("FlightDate"), "MMMM dd, yyyy"))
         .join(airlineCurrentDF, Seq("carrier"), "left")
         .join(airportDiv1DF, Seq("Div1AirportID"), "left")
         .join(airportDiv1DF, Seq("Div1AirportID"), "left")
         .join(airportDiv2DF, Seq("Div2AirportID"), "left")
         .join(airportOriginDF, Seq("OriginAirportID"), "left")
         .join(airportDestDF, Seq("DestAirportID"), "left")
         .join(timeDimensionDF, Seq("DepTime"), "left")
         .join(dateDimensionDF, Seq("dateF"), "left")
         .join(planeCurrentDF, Seq("TailNum"), "left")
         .join(planeDiv1DF, Seq("Div1TailNum"), "left")
         .join(planeDiv2DF, Seq("Div2TailNum"), "left")

         .withColumn("total_delay", col("WeatherDelay")+ col("NASDelay")
           + col("SecurityDelay") + col("LateAircraftDelay"))
         .select(
           $"airline_key",
           $"origin_airport_key",
           $"plane_key",
           $"dest_airport_key",
           $"actual_dep_time_key",
           $"date_key" as "flight_date_key",
           $"cancelled",
           $"diverted",
           $"ArrTime" as "arr_time",
           $"DepTime" as "dep_time",
           $"WheelsOff" as "wheels_off",
           $"WheelsOn" as "wheels_on",
           $"WeatherDelay" as "weather_delay",
           $"NASDelay" as "nas_delay",
           $"SecurityDelay" as "security_delay",
           $"LateAircraftDelay" as "late_aircraft_delay",
           $"total_delay",
           $"TaxiIn" as "taxi_in",
           $"TaxiOut" as "taxi_out",
           airportDiv1DF.col("div1_airport_key"),
           $"div1_plane_key",
           $"Div1WheelsOff" as "div1_wheels_off",
           $"Div1WheelsOn" as "div1_wheels_on",
           $"div2_airport_key",
           $"div2_plane_key",
           $"Div2WheelsOff" as "div2_wheels_off",
           $"Div2WheelsOn" as "div2_wheels_on"
  )
  flightDimDF.show(5, false)


  //Write factTable
  val fact_flight_loc = "src/datasets/presentation_layer/flight_fact_table"
  flightDimDF
    .write
    .option("compression", "snappy")
    .mode("overwrite")
    .parquet(fact_flight_loc)

  val parquetFlightFact = spark.read.parquet(fact_flight_loc)
  val qFlightFact = parquetFlightFact.count()
  print("\n Rows - presentation_layer : ", qFlightFact)

}