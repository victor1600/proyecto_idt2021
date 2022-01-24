import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, udf}


object flight_fact extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("proyecto_idt2021")
    .getOrCreate()

  //Function
  flightFact()

  def flightFact(): Unit = {
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


    //Dataset flights
    val flightsPath = "src/datasets/raw_layer/flights"
    val flightsDF = spark.read
      .option("sep", ";")
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
      .where(col("current_flag") === true)


    //plane
    val planeCurrentDF = planeDF.select(
      $"plane_key",
      $"n_number" as "TailNum")
      .where(col("current_flag") === true)

    val prueba = flightsDF.select(col("DepTime"))
    prueba.show(10,false)


    // Converter hour type int as hour and minutes - DEPTIME
    val flightHourDF =
      flightsDF.select(col("DepTime").cast("integer"))
        .withColumn("hourUnion", split(col("DepTime"), ""))
        .withColumn("hour1", element_at(col("hourUnion"), 1))
        .withColumn("hour2", element_at(col("hourUnion"), 2))
        .withColumn("min1", element_at(col("hourUnion"), 3))
        .withColumn("min2", element_at(col("hourUnion"), 4))
        .withColumn("old_hours", when(concat($"hour1", $"hour2") === "24", "0").otherwise(concat($"hour1", $"hour2")))
        .withColumn("minute", when(concat($"min1", $"min2") === "00", "0").when(isnull(concat($"min1", $"min2")), 0).otherwise(concat($"min1", $"min2")).cast("integer"))
        .withColumn("hour_1", when(col("min2") === "", 0).when(concat($"hour1", $"hour2") === "24", 0).otherwise($"hour1"))
        .withColumn("hour_2", when(col("min2") === "", $"hour1").when(concat($"hour1", $"hour2") === "00", 0).otherwise($"hour2"))
        .withColumn("second", lit(0).cast("integer"))
        .withColumn("hour", concat($"hour_1", $"hour_2").cast("integer"))


    val timeDimensionDF = flightHourDF
      .join(
        timeDF, flightHourDF("hour") === timeDF("hour") &&
          flightHourDF("minute") === timeDF("minute") &&
          flightHourDF("second") === timeDF("second"), "inner")
      .select(
        flightHourDF.col("DepTime"),
        timeDF.col("time_key").alias("actual_dep_time_key"),
        timeDF.col("time_24h")
      ).distinct()




    // Converter hour type int as hour and minutes - CRSDEPTIME
    val flightHourCrsDF =
    flightsDF.select(col("CRSDepTime").cast("string"))
      .withColumn("hourUnion", split(col("CRSDepTime"), ""))
      .withColumn("hour1", element_at(col("hourUnion"), 1))
      .withColumn("hour2", element_at(col("hourUnion"), 2))
      .withColumn("min1", element_at(col("hourUnion"), 3))
      .withColumn("min2", element_at(col("hourUnion"), 4))
      .withColumn("old_hours", when(concat($"hour1", $"hour2") === "24", "0").otherwise(concat($"hour1", $"hour2")))
      .withColumn("old_minute", when(concat($"min1", $"min2") === "00", "0").when(isnull(concat($"min1", $"min2")), 0).otherwise(concat($"min1", $"min2")).cast("integer"))
      .withColumn("hour_1", when(col("min2") === "", 0).when(concat($"hour1", $"hour2") === "24", 0).otherwise($"hour1"))
      .withColumn("hour_2", when(col("min2") === "", $"hour1").when(concat($"hour1", $"hour2") === "00", 0).otherwise($"hour2"))
      .withColumn("minute", when(col("min2") === "", concat($"hour2", $"min1")).otherwise($"old_minute"))
      .withColumn("second", lit(0).cast("integer"))
      .withColumn("hour", concat($"hour_1", $"hour_2").cast("integer"))


    val timeDimensionCrsDF = flightHourCrsDF.
      join(
        timeDF, flightHourCrsDF("hour") === timeDF("hour") &&
          flightHourCrsDF("minute") === timeDF("minute") &&
          flightHourCrsDF("second") === timeDF("second"), "inner")
      .select(
        flightHourCrsDF.col("CRSDepTime"),
        timeDF.col("time_key").alias("crs_dep_time_key")).distinct()



    // Converter hour type  - wheels on y wheels off
    val cFlightDF = flightsDF

      //wheels on
      .withColumn("hourUnion", split(col("WheelsOn"), ""))
      .withColumn("hour1", element_at(col("hourUnion"), 1))
      .withColumn("hour2", element_at(col("hourUnion"), 2))
      .withColumn("min1", element_at(col("hourUnion"), 3))
      .withColumn("min2", element_at(col("hourUnion"), 4))
      .withColumn("old_hours", when(concat($"hour1", $"hour2") === "24", "0").otherwise(concat($"hour1", $"hour2")))
      .withColumn("old_minute", when(concat($"min1", $"min2") === "00", "0").when(isnull(concat($"min1", $"min2")), 0).otherwise(concat($"min1", $"min2")).cast("integer"))
      .withColumn("hour_1", when(col("min2") === "", 0).when(col("min1") === "", 0).when(col("hour2") === "", 0).when(concat($"hour1", $"hour2") === "24", 0).otherwise($"hour1"))
      .withColumn("hour_2", when(col("min2") === "", $"hour1").when(col("min1") === "", $"hour1").when(col("hour2") === "", $"hour1").when(concat($"hour1", $"hour2") === "00", 0).otherwise($"hour2"))
      .withColumn("minute", when(col("min2") === "", concat($"hour2", $"min1")).when(col("min1") === "", concat($"hour2", lit(0))).when(col("hour2") === "",  lit("00")).otherwise($"old_minute"))
      .withColumn("minute_new", when(col("minute") === 60, lit("00")).otherwise($"minute"))
      .withColumn("second", lit(0).cast("integer"))
      .withColumn("hour", concat($"hour_1", $"hour_2").cast("integer"))
      .withColumn("hour_new", when(col("minute") === 60, $"hour" + 1).otherwise($"hour"))

      .withColumn("wheels_on", concat($"hour_new",lit(":"), $"minute_new",lit(":"), $"second"))
      .drop("WheelsOn","hourUnion","hour1","hour2","min1","min2","hour_new","minute_new","old_hours","old_minute","hour_1","hour_2","second","hour", "minute")


      //wheels off
      .withColumn("hourUnion", split(col("WheelsOff"), ""))
      .withColumn("hour1", element_at(col("hourUnion"), 1))
      .withColumn("hour2", element_at(col("hourUnion"), 2))
      .withColumn("min1", element_at(col("hourUnion"), 3))
      .withColumn("min2", element_at(col("hourUnion"), 4))
      .withColumn("old_hours", when(concat($"hour1", $"hour2") === "24", "0").otherwise(concat($"hour1", $"hour2")))
      .withColumn("old_minute", when(concat($"min1", $"min2") === "00", "0").when(isnull(concat($"min1", $"min2")), 0).otherwise(concat($"min1", $"min2")).cast("integer"))
      .withColumn("hour_1", when(col("min2") === "", 0).when(col("min1") === "", 0).when(col("hour2") === "", 0).when(concat($"hour1", $"hour2") === "24", 0).otherwise($"hour1"))
      .withColumn("hour_2", when(col("min2") === "", $"hour1").when(col("min1") === "", $"hour1").when(col("hour2") === "", $"hour1").when(concat($"hour1", $"hour2") === "00", 0).otherwise($"hour2"))
      .withColumn("minute", when(col("min2") === "", concat($"hour2", $"min1")).when(col("min1") === "", concat($"hour2", lit(0))).when(col("hour2") === "",  lit("00")).otherwise($"old_minute"))
      .withColumn("minute_new", when(col("minute") === 60, lit("00")).otherwise($"minute"))
      .withColumn("second", lit(0).cast("integer"))
      .withColumn("hour", concat($"hour_1", $"hour_2").cast("integer"))
      .withColumn("hour_new", when(col("minute") === 60, $"hour" + 1).otherwise($"hour"))

      .withColumn("wheels_off", concat($"hour_new",lit(":"), $"minute_new",lit(":"), $"second"))
      .drop("WheelsOff","hourUnion","hour1","hour2","min1","min2","hour_new","minute_new","old_hours","old_minute","hour_1","hour_2","second","hour", "minute")


      //Div1WheelsOff
      .withColumn("hourUnion", split(col("Div1WheelsOff"), ""))
      .withColumn("hour1", element_at(col("hourUnion"), 1))
      .withColumn("hour2", element_at(col("hourUnion"), 2))
      .withColumn("min1", element_at(col("hourUnion"), 3))
      .withColumn("min2", element_at(col("hourUnion"), 4))
      .withColumn("old_hours", when(concat($"hour1", $"hour2") === "24", "0").otherwise(concat($"hour1", $"hour2")))
      .withColumn("old_minute", when(concat($"min1", $"min2") === "00", "0").when(isnull(concat($"min1", $"min2")), 0).otherwise(concat($"min1", $"min2")).cast("integer"))
      .withColumn("hour_1", when(col("min2") === "", 0).when(col("min1") === "", 0).when(col("hour2") === "", 0).when(concat($"hour1", $"hour2") === "24", 0).otherwise($"hour1"))
      .withColumn("hour_2", when(col("min2") === "", $"hour1").when(col("min1") === "", $"hour1").when(col("hour2") === "", $"hour1").when(concat($"hour1", $"hour2") === "00", 0).otherwise($"hour2"))
      .withColumn("minute", when(col("min2") === "", concat($"hour2", $"min1")).when(col("min1") === "", concat($"hour2", lit(0))).when(col("hour2") === "",  lit("00")).otherwise($"old_minute"))
      .withColumn("minute_new", when(col("minute") === 60, lit("00")).otherwise($"minute"))
      .withColumn("second", lit(0).cast("integer"))
      .withColumn("hour", concat($"hour_1", $"hour_2").cast("integer"))
      .withColumn("hour_new", when(col("minute") === 60, $"hour" + 1).otherwise($"hour"))

      .withColumn("div1_wheels_off", concat($"hour_new",lit(":"), $"minute_new",lit(":"), $"second"))
      .drop("Div1WheelsOff","hourUnion","hour1","hour2","min1","min2","hour_new","minute_new","old_hours","old_minute","hour_1","hour_2","second","hour", "minute")


      //Div1WheelsOn
      .withColumn("hourUnion", split(col("Div1WheelsOn"), ""))
      .withColumn("hour1", element_at(col("hourUnion"), 1))
      .withColumn("hour2", element_at(col("hourUnion"), 2))
      .withColumn("min1", element_at(col("hourUnion"), 3))
      .withColumn("min2", element_at(col("hourUnion"), 4))
      .withColumn("old_hours", when(concat($"hour1", $"hour2") === "24", "0").otherwise(concat($"hour1", $"hour2")))
      .withColumn("old_minute", when(concat($"min1", $"min2") === "00", "0").when(isnull(concat($"min1", $"min2")), 0).otherwise(concat($"min1", $"min2")).cast("integer"))
      .withColumn("hour_1", when(col("min2") === "", 0).when(col("min1") === "", 0).when(col("hour2") === "", 0).when(concat($"hour1", $"hour2") === "24", 0).otherwise($"hour1"))
      .withColumn("hour_2", when(col("min2") === "", $"hour1").when(col("min1") === "", $"hour1").when(col("hour2") === "", $"hour1").when(concat($"hour1", $"hour2") === "00", 0).otherwise($"hour2"))
      .withColumn("minute", when(col("min2") === "", concat($"hour2", $"min1")).when(col("min1") === "", concat($"hour2", lit(0))).when(col("hour2") === "",  lit("00")).otherwise($"old_minute"))
      .withColumn("minute_new", when(col("minute") === 60, lit("00")).otherwise($"minute"))
      .withColumn("second", lit(0).cast("integer"))
      .withColumn("hour", concat($"hour_1", $"hour_2").cast("integer"))
      .withColumn("hour_new", when(col("minute") === 60, $"hour" + 1).otherwise($"hour"))

      .withColumn("div1_wheels_on", concat($"hour_new",lit(":"), $"minute_new",lit(":"), $"second"))
      .drop("Div1WheelsOn","hourUnion","hour1","hour2","min1","min2","hour_new","minute_new","old_hours","old_minute","hour_1","hour_2","second","hour", "minute")


      //Div2WheelsOn
      .withColumn("hourUnion", split(col("Div2WheelsOn"), ""))
      .withColumn("hour1", element_at(col("hourUnion"), 1))
      .withColumn("hour2", element_at(col("hourUnion"), 2))
      .withColumn("min1", element_at(col("hourUnion"), 3))
      .withColumn("min2", element_at(col("hourUnion"), 4))
      .withColumn("old_hours", when(concat($"hour1", $"hour2") === "24", "0").otherwise(concat($"hour1", $"hour2")))
      .withColumn("old_minute", when(concat($"min1", $"min2") === "00", "0").when(isnull(concat($"min1", $"min2")), 0).otherwise(concat($"min1", $"min2")).cast("integer"))
      .withColumn("hour_1", when(col("min2") === "", 0).when(col("min1") === "", 0).when(col("hour2") === "", 0).when(concat($"hour1", $"hour2") === "24", 0).otherwise($"hour1"))
      .withColumn("hour_2", when(col("min2") === "", $"hour1").when(col("min1") === "", $"hour1").when(col("hour2") === "", $"hour1").when(concat($"hour1", $"hour2") === "00", 0).otherwise($"hour2"))
      .withColumn("minute", when(col("min2") === "", concat($"hour2", $"min1")).when(col("min1") === "", concat($"hour2", lit(0))).when(col("hour2") === "",  lit("00")).otherwise($"old_minute"))
      .withColumn("minute_new", when(col("minute") === 60, lit("00")).otherwise($"minute"))
      .withColumn("second", lit(0).cast("integer"))
      .withColumn("hour", concat($"hour_1", $"hour_2").cast("integer"))
      .withColumn("hour_new", when(col("minute") === 60, $"hour" + 1).otherwise($"hour"))

      .withColumn("div2_wheels_on", concat($"hour_new",lit(":"), $"minute_new",lit(":"), $"second"))
      .drop("Div2WheelsOn","hourUnion","hour1","hour2","min1","min2","hour_new","minute_new","old_hours","old_minute","hour_1","hour_2","second","hour", "minute")




      //Div2WheelsOff
      .withColumn("hourUnion", split(col("Div2WheelsOff"), ""))
      .withColumn("hour1", element_at(col("hourUnion"), 1))
      .withColumn("hour2", element_at(col("hourUnion"), 2))
      .withColumn("min1", element_at(col("hourUnion"), 3))
      .withColumn("min2", element_at(col("hourUnion"), 4))
      .withColumn("old_hours", when(concat($"hour1", $"hour2") === "24", "0").otherwise(concat($"hour1", $"hour2")))
      .withColumn("old_minute", when(concat($"min1", $"min2") === "00", "0").when(isnull(concat($"min1", $"min2")), 0).otherwise(concat($"min1", $"min2")).cast("integer"))
      .withColumn("hour_1", when(col("min2") === "", 0).when(col("min1") === "", 0).when(col("hour2") === "", 0).when(concat($"hour1", $"hour2") === "24", 0).otherwise($"hour1"))
      .withColumn("hour_2", when(col("min2") === "", $"hour1").when(col("min1") === "", $"hour1").when(col("hour2") === "", $"hour1").when(concat($"hour1", $"hour2") === "00", 0).otherwise($"hour2"))
      .withColumn("minute", when(col("min2") === "", concat($"hour2", $"min1")).when(col("min1") === "", concat($"hour2", lit(0))).when(col("hour2") === "",  lit("00")).otherwise($"old_minute"))
      .withColumn("minute_new", when(col("minute") === 60, lit("00")).otherwise($"minute"))
      .withColumn("second", lit(0).cast("integer"))
      .withColumn("hour", concat($"hour_1", $"hour_2").cast("integer"))
      .withColumn("hour_new", when(col("minute") === 60, $"hour" + 1).otherwise($"hour"))

      .withColumn("div2_wheels_off", concat($"hour_new",lit(":"), $"minute_new",lit(":"), $"second"))
      .drop("Div2WheelsOff","hourUnion","hour1","hour2","min1","min2","hour_new","minute_new","old_hours","old_minute","hour_1","hour_2","second","hour", "minute")


    cFlightDF.show(10, false)




    //New column FlightDate
    val dateDimensionDF = dateDF
      .withColumn("FlightDate", col("date"))
    dateDimensionDF.printSchema()
    dateDimensionDF.show(10, false)

    val newFlightsDF  = cFlightDF
     .join(dateDimensionDF, Seq("FlightDate"), "inner")
      .drop("day_of_week","calendar_month","calenda_quarter","calendar_year","holiday_indicator","weekday_indicator", "full_date_description")
      newFlightsDF.show(10, false)





    val flighFactDF = newFlightsDF
      .join(airlineCurrentDF, Seq("carrier"), "left")
      .join(airportOriginDF, Seq("OriginAirportID"), "left")
      .join(airportDestDF, Seq("DestAirportID"), "left")
      .join(airportDiv1DF, Seq("Div1AirportID"), "left")
      .join(airportDiv2DF, Seq("Div2AirportID"), "left")
      .join(timeDimensionDF, Seq("DepTime"), "left")
      .join(timeDimensionCrsDF, Seq("CRSDepTime"), "left")
      .join(planeCurrentDF, Seq("TailNum"), "left")
      .join(planeDiv1DF, Seq("Div1TailNum"), "left")
      .join(planeDiv2DF, Seq("Div2TailNum"), "left")
      .withColumn("total_delay", col("WeatherDelay") + col("NASDelay") + col("SecurityDelay")
        + col("LateAircraftDelay"))
      .select(
        $"airline_key",
        $"plane_key",
        $"date_key" as "flight_date_key",
        $"origin_airport_key",
        $"dest_airport_key",
        $"actual_dep_time_key",
        $"crs_dep_time_key",
        $"cancelled",
        $"diverted",
        $"AirTime" as "air_time",
        col("wheels_off"),
        col("wheels_on"),
        col("WeatherDelay").alias("weather_delay"),
        col("NASDelay").alias("nas_delay"),
        col("SecurityDelay").alias("security_delay"),
        col("LateAircraftDelay").alias("late_aircraft_delay"),
        $"total_delay",
        $"TaxiIn" as "taxi_in",
        $"TaxiOut" as "taxi_out",
        airportDiv1DF.col("div1_airport_key"),
        $"div1_plane_key",
        col("div1_wheels_off"),
        col("div1_wheels_on"),
        $"div2_airport_key",
        $"div2_plane_key",
        col("div2_wheels_off"),
        col("div2_wheels_on"),

      )
    flighFactDF.show(10, false)

  }
}