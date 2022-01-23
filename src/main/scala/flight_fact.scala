import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, udf}


object flight_fact extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("proyecto_idt2021")
    .getOrCreate()

  //Función
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
      .where(col("current_flag") === true)


    //plane
    val planeCurrentDF = planeDF.select(
      $"plane_key",
      $"n_number" as "TailNum")
      .where(col("current_flag") === true)


    // Converter hour type int as hour and minutes - DEPTIME
    val flightHourDF =
      flightsDF.select(col("DepTime").cast("integer"))
        .withColumn("hourUnion", split(col("DepTime"), ""))
        .withColumn("hour1", element_at(col("hourUnion"), 1))
        .withColumn("hour2", element_at(col("hourUnion"), 2))
        .withColumn("min1", element_at(col("hourUnion"), 3))
        .withColumn("min2", element_at(col("hourUnion"), 4))
        .withColumn("hours", when(concat($"hour1", $"hour2") === "24", "0").otherwise(concat($"hour1", $"hour2")))
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
        timeDF.col("time_key").alias("actual_dep_time_key")).distinct()


    // Converter hour type int as hour and minutes - CRSDEPTIME
    val flightHourCrsDF =
      flightsDF.select(col("CRSDepTime").cast("string"))
        .withColumn("hourUnion", split(col("CRSDepTime"), ""))
        .withColumn("hour1", element_at(col("hourUnion"), 1))
        .withColumn("hour2", element_at(col("hourUnion"), 2))
        .withColumn("min1", element_at(col("hourUnion"), 3))
        .withColumn("min2", element_at(col("hourUnion"), 4))
        .withColumn("hours", when(concat($"hour1", $"hour2") === "24", "0").otherwise(concat($"hour1", $"hour2")))
        .withColumn("minute", when(concat($"min1", $"min2") === "00", "0").when(isnull(concat($"min1", $"min2")), 0).otherwise(concat($"min1", $"min2")).cast("integer"))
        .withColumn("hour_1", when(col("min2") === "", 0).when(concat($"hour1", $"hour2") === "24", 0).otherwise($"hour1"))
        .withColumn("hour_2", when(col("min2") === "", $"hour1").when(concat($"hour1", $"hour2") === "00", 0).otherwise($"hour2"))
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




    //New column FlightDate
    val dateDimensionDF = dateDF
      .withColumn("FlightDate", col("date"))


    // Converter hour type int as hour and minutes - wheels on y wheels off
    val cFlightDF = flightsDF.select(
      col("FlightDate"),
      col("carrier"),
      col("OriginAirportID"),
      col("DestAirportID"),
      col("Div1AirportID"),
      col("Div2AirportID"),
      col("DepTime"),
      col("CRSDepTime"),
      col("TailNum"),
      col("Div1TailNum"),
      col("Div2TailNum"),
      col("WeatherDelay"),
      col("NASDelay"),
      col("SecurityDelay"),
      col("LateAircraftDelay"),
      col("TaxiIn"),
      col("TaxiOut"),
      col("WheelsOn").cast("string").alias("WheelsOn"),
      col("WheelsOff").cast("string").alias("WheelsOff"),
      col("cancelled"),
      col("diverted"),
      col("AirTime"),
      col("Div1WheelsOff"),
      col("Div1WheelsOn"),
      col("Div2WheelsOff"),
      col("Div2WheelsOn"))
      .withColumn("total_delay", col("WeatherDelay") + col("NASDelay") + col("SecurityDelay")
        + col("LateAircraftDelay"))

      .withColumn("hu1", split(col("WheelsOn"), ""))
      .withColumn("h1", element_at(col("hu1"), 1))
      .withColumn("h2", element_at(col("hu1"), 2))
      .withColumn("m1", element_at(col("hu1"), 3))
      .withColumn("m2", element_at(col("hu1"), 4))
      .withColumn("hour1", when(concat($"h1", $"h2") === "24", "0").otherwise(concat($"h1", $"h2")))
      .withColumn("minute1", when(concat($"m1", $"m2") === "00", "0").when(isnull(concat($"m1", $"m2")), 0).otherwise(concat($"m1", $"m2")).cast("integer"))
      .withColumn("hour_1", when(col("m2") === "", 0).when(concat($"h1", $"h2") === "24", 0).otherwise($"hour1"))
      .withColumn("hour_2", when(col("m2") === "", $"hour1").when(concat($"h1", $"h2") === "00", 0).otherwise($"h2"))
      .withColumn("second1", lit(0).cast("integer"))
      .withColumn("hours1", concat($"hour_1", $"hour_2").cast("integer"))
      .withColumn("wheels_on", concat($"hour1",lit(":"), $"minute1",lit(":"), $"second1"))
      .drop("WheelsOn","hu1","h1","h2","m1","m2","hour1","minute1","hour_1","hour_2","second1","hours1")



      .withColumn("hu2", split(col("WheelsOff"), ""))
      .withColumn("h3", element_at(col("hu2"), 1))
      .withColumn("h4", element_at(col("hu2"), 2))
      .withColumn("m3", element_at(col("hu2"), 3))
      .withColumn("m4", element_at(col("hu2"), 4))
      .withColumn("hour2", when(concat($"h3", $"h4") === "24", "0").otherwise(concat($"h3", $"h4")))
      .withColumn("minute2", when(concat($"m3", $"m4") === "00", "0").when(isnull(concat($"m3", $"m4")), 0).otherwise(concat($"m3", $"m4")).cast("integer"))
      .withColumn("hour_3", when(col("m4") === "", 0).when(concat($"h3", $"h4") === "24", 0).otherwise($"hour2"))
      .withColumn("hour_4", when(col("m4") === "", $"hour2").when(concat($"h3", $"h4") === "00", 0).otherwise($"h4"))
      .withColumn("second2", lit(0).cast("integer"))
      .withColumn("hours2", concat($"hour_3", $"hour_4").cast("integer"))
      .withColumn("wheels_off", concat($"hours2",lit(":"), $"minute2",lit(":"), $"second2"))
      .drop("WheelsOff","hu2","h3","h4","m3","m4","hour2","minute2","hour_3","hour_4","second2","hours2")


      .withColumn("hu3", split(col("Div1WheelsOff"), ""))
      .withColumn("h5", element_at(col("hu3"), 1))
      .withColumn("h6", element_at(col("hu3"), 2))
      .withColumn("m5", element_at(col("hu3"), 3))
      .withColumn("m6", element_at(col("hu3"), 4))
      .withColumn("hour3", when(concat($"h5", $"h6") === "24", "0").otherwise(concat($"h5", $"h6")))
      .withColumn("minute3", when(concat($"m5", $"m6") === "00", "0").when(isnull(concat($"m5", $"m6")), 0).otherwise(concat($"m5", $"m6")).cast("integer"))
      .withColumn("hour_5", when(col("m6") === "", 0).when(concat($"h5", $"h6") === "24", 0).otherwise($"hour3"))
      .withColumn("hour_6", when(col("m6") === "", $"hour3").when(concat($"h5", $"h6") === "00", 0).otherwise($"h6"))
      .withColumn("second3", lit(0).cast("integer"))
      .withColumn("hours3", concat($"hour_5", $"hour_6").cast("integer"))
      .withColumn("div1_wheels_off", concat($"hours3",lit(":"), $"minute3",lit(":"), $"second3"))
      .drop("Div1WheelsOff","hu3","h5","h6","m5","m6","hour3","minute3","hour_5","hour_6","second3","hours3")


      .withColumn("hu4", split(col("Div1WheelsOn"), ""))
      .withColumn("h7", element_at(col("hu4"), 1))
      .withColumn("h8", element_at(col("hu4"), 2))
      .withColumn("m7", element_at(col("hu4"), 3))
      .withColumn("m8", element_at(col("hu4"), 4))
      .withColumn("hour4", when(concat($"h7", $"h8") === "24", "0").otherwise(concat($"h7", $"h8")))
      .withColumn("minute4", when(concat($"m7", $"m8") === "00", "0").when(isnull(concat($"m7", $"m8")), 0).otherwise(concat($"m7", $"m8")).cast("integer"))
      .withColumn("hour_7", when(col("m8") === "", 0).when(concat($"h7", $"h8") === "24", 0).otherwise($"hour4"))
      .withColumn("hour_8", when(col("m8") === "", $"hour4").when(concat($"h7", $"h8") === "00", 0).otherwise($"h8"))
      .withColumn("second4", lit(0).cast("integer"))
      .withColumn("hours4", concat($"hour_7", $"hour_8").cast("integer"))
      .withColumn("div1_wheels_on", concat($"hours4",lit(":"), $"minute4",lit(":"), $"second4"))
      .drop("Div1WheelsOn","hu4","h7","h8","m7","m8","hour4","minute4","hour_7","hour_8","second4","hours4")



      .withColumn("hu5", split(col("Div2WheelsOn"), ""))
      .withColumn("h9", element_at(col("hu5"), 1))
      .withColumn("h10", element_at(col("hu5"), 2))
      .withColumn("m9", element_at(col("hu5"), 3))
      .withColumn("m10", element_at(col("hu5"), 4))
      .withColumn("hour5", when(concat($"h9", $"h10") === "24", "0").otherwise(concat($"h9", $"h10")))
      .withColumn("minute5", when(concat($"m9", $"m10") === "00", "0").when(isnull(concat($"m9", $"m10")), 0).otherwise(concat($"m9", $"m10")).cast("integer"))
      .withColumn("hour_9", when(col("m10") === "", 0).when(concat($"h9", $"h10") === "24", 0).otherwise($"hour5"))
      .withColumn("hour_10", when(col("m10") === "", $"hour5").when(concat($"h9", $"h10") === "00", 0).otherwise($"h10"))
      .withColumn("second5", lit(0).cast("integer"))
      .withColumn("hours5", concat($"hour_9", $"hour_10").cast("integer"))
      .withColumn("div2_wheels_on", concat($"hours5",lit(":"), $"minute5",lit(":"), $"second5"))
      .drop("Div2WheelsOn","hu5","h9","h10","m9","m10","hour5","minute5","hour_9","hour_10","second5","hours5")



      .withColumn("hu6", split(col("Div2WheelsOff"), ""))
      .withColumn("h11", element_at(col("hu6"), 1))
      .withColumn("h12", element_at(col("hu6"), 2))
      .withColumn("m11", element_at(col("hu6"), 3))
      .withColumn("m12", element_at(col("hu6"), 4))
      .withColumn("hour6", when(concat($"h11", $"h12") === "24", "0").otherwise(concat($"h11", $"h12")))
      .withColumn("minute6", when(concat($"m11", $"m12") === "00", "0").when(isnull(concat($"m11", $"m12")), 0).otherwise(concat($"m11", $"m12")).cast("integer"))
      .withColumn("hour_11", when(col("m12") === "", 0).when(concat($"h11", $"h12") === "24", 0).otherwise($"hour6"))
      .withColumn("hour_12", when(col("m12") === "", $"hour6").when(concat($"h11", $"h12") === "00", 0).otherwise($"h12"))
      .withColumn("second6", lit(0).cast("integer"))
      .withColumn("hours6", concat($"hour_11", $"hour_12").cast("integer"))
      .withColumn("div2_wheels_off", concat($"hours6",lit(":"), $"minute6",lit(":"), $"second6"))
      .drop("Div2WheelsOff","hu6","h11","h12","m11","m12","hour6","minute6","hour_11","hour_12","second6","hours6")




    val flighFactDF = cFlightDF
      .join(dateDimensionDF, Seq("FlightDate"), "left")
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
        col("wheels_off").cast("date"),
        col("wheels_on").cast("date"),
        col("WeatherDelay").alias("weather_delay").cast("int"),
        col("NASDelay").alias("nas_delay").cast("int"),
        col("SecurityDelay").alias("security_delay").cast("int"),
        col("LateAircraftDelay").alias("late_aircraft_delay").cast("int"),
        $"total_delay",
        $"TaxiIn" as "taxi_in",
        $"TaxiOut" as "taxi_out",
        airportDiv1DF.col("div1_airport_key"),
        $"div1_plane_key",
        col("div1_wheels_off").cast("date"),
        col("div1_wheels_on").cast("date"),
        $"div2_airport_key",
        $"div2_plane_key",
        col("div2_wheels_off").cast("date"),
        col("div2_wheels_on").cast("date"),

      )

    flighFactDF.printSchema()
    flighFactDF.show(10)

    //Write factTable
    val fact_flight_loc = "src/datasets/presentation_layer/flight_fact_table"
    flighFactDF
      .write
      .option("compression", "snappy")
      .mode("overwrite")
      .parquet(fact_flight_loc)

    val parquetFlightFact = spark.read.parquet(fact_flight_loc)

    val qFlightFact = parquetFlightFact.count()
    print("\n Rows - presentation_layer : ", qFlightFact)
  }

}