import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrameNaFunctions


object airport_dimension extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("proyecto_idt2021")
    .getOrCreate()

  //Actual version - Airport staging dimension
  val staging_airports_location = "src/datasets/staging_layer/airports"
  val stagingAirports = spark.read.parquet(staging_airports_location)

  //Def
  //InitialLoad()
  IncrementalLoad()


  //Inicial load
  def InitialLoad(): Unit = {
    val distinctAirportDF = stagingAirports
      .withColumn("start_date", lit(java.time.LocalDate.now))
      .withColumn("end_date", to_date(lit("9999-12-31")))
      .withColumn("current_flag", lit(true))

    //test
    distinctAirportDF.show(20, false)
    distinctAirportDF.printSchema()
    val qAirport = distinctAirportDF.select("airport_key").count()
    print("Airport quantities - inicialLoad", qAirport)

    //path - save
    val dim_airport_loc = "src/datasets/presentation_layer/dim_airport"
    //write
    distinctAirportDF
      .write
      .option("compression", "snappy")
      .mode("overwrite")
      .parquet(dim_airport_loc)


    //test
    val distinctAirportsDF = spark.read.parquet(dim_airport_loc)

    val qData = distinctAirportsDF.select("airport_key", "airport_name").count()
    println("Data - presentation_layer", qData)


  }


  //Incremental load
  def IncrementalLoad(): Unit = {

    //path - save
    val dim_airport_loc = "src/datasets/presentation_layer/dim_airport"
    val dim_airport_temp_loc = "src/datasets/presentation_layer/temp/dim_airport"


    //Reading Datasets
    val currentDimAirportDF = spark.read.parquet(dim_airport_loc)
    val qAirportSave = currentDimAirportDF.select("airport_key").count()
    println("\n Airport quantities - save \n", qAirportSave)


    // Distinct row - staging
    val distinctAirportsDF = stagingAirports
      .withColumn("start_date", lit(java.time.LocalDate.now))
      .withColumn("end_date", to_date(lit("9999-12-31")))
      .withColumn("current_flag", lit(true))

    //test
    distinctAirportsDF.show(10, false)
    distinctAirportsDF.printSchema()
    val qAirport = distinctAirportsDF.select("airport_key").count()
    println("\n Airport quantities - staging \n", qAirport)


    //Search new rows
    val newAirportDF = distinctAirportsDF.join(currentDimAirportDF, distinctAirportsDF("code") === currentDimAirportDF("code"), "leftanti")

    //test
    val qAirportNew = newAirportDF.select("airport_key").count()
    print("\n Airport new \n", qAirportNew)
    newAirportDF.show(false)
    newAirportDF.printSchema()


    //Union with new rows
    val newDimAirport = currentDimAirportDF.union(newAirportDF)

    //test union
    newDimAirport.show(10, false)
    newDimAirport.printSchema()
    val qAirportUnion = newDimAirport.select("airport_key").count()
    println("Airport with new rows: ", qAirportUnion)


    //write - presentation_layer
    newDimAirport
      .write
      //  .option("compression", "snappy")
      .format("parquet")
      .mode("overwrite")
      .parquet(dim_airport_temp_loc)


    spark.read.parquet(dim_airport_temp_loc)
      .write
      // .option("compression", "snappy")
      .format("parquet")
      .mode("overwrite")
      .parquet(dim_airport_loc)
    println("Write new row - presentation_layer")

    val newCurrentDimAirportDF = spark.read.parquet(dim_airport_loc)
    val writeRows = newCurrentDimAirportDF.select("airport_key").count()
    println("\n Current \n", writeRows)


    //condition, where airport new = 0
    if (qAirportNew == 0) {
      println("\n No new rows were found")

      //call def
      ChangeRows()


    } else {

      //call def
      ChangeRows()
    }

    //def SCD2
    def ChangeRows(): Unit = {

      //Autoincremet - airport_key
      val next_pk_to_insert = distinctAirportsDF.agg(max("airport_key")).
        collectAsList().get(0).get(0).asInstanceOf[Long] + 1


      println("Verify other changes")
      //verify changes - airports rows edit
      val change = distinctAirportsDF.join(newCurrentDimAirportDF,
        distinctAirportsDF("airport_name") === newCurrentDimAirportDF("airport_name") &&
        distinctAirportsDF("city") === newCurrentDimAirportDF("city") &&
        distinctAirportsDF("state_code") === newCurrentDimAirportDF("state_code") &&
        distinctAirportsDF("state_name") === newCurrentDimAirportDF("state_name") &&
        distinctAirportsDF("wac") === newCurrentDimAirportDF("wac") &&
        distinctAirportsDF("country") === newCurrentDimAirportDF("country"),
        "leftanti")

      change.show(false)
      val qAirportChange = change.select("airport_key").count()
      println("\n Airport with change \n", qAirportChange)

      if (qAirportChange == 0) {
        println("\n Not found new changes in the airports \n", qAirportChange)


      } else {

        val newRowAirportDF = change
          // .withColumn("airport_key", monotonically_increasing_id() + next_pk_to_insert)
          .select("code", "airport_name", "city", "state_code", "state_name", "wac", "country")
          .withColumn("airport_key", monotonically_increasing_id() + next_pk_to_insert)
          .withColumn("start_date", lit(java.time.LocalDate.now))
          .withColumn("end_date", to_date(lit("9999-12-31")))
          .withColumn("current_flag", lit(true))
          .select("airport_key", "code", "airport_name", "city", "state_code",
            "state_name", "wac", "country", "start_date", "end_date", "current_flag")

        //test
        newRowAirportDF.printSchema()
        newRowAirportDF.show(false)
        println("\n New rows with changes ")

        //Different rows without change
        val differentRowAirportDF = newCurrentDimAirportDF.join(change,
          newCurrentDimAirportDF("code") === change("code"),
          "leftanti")
        differentRowAirportDF.show(false)
        differentRowAirportDF.printSchema()
        println("\n Different rows - NewRowAirport ")


        /*Rows with match code, drop columns end_date and current_flag,
         new columns end date and current_flag and UNION with new rows, different to new rows and changes */
        val changesRowAirportDF = newCurrentDimAirportDF.join(newRowAirportDF,
          newCurrentDimAirportDF("code") === newRowAirportDF("code"),
          "leftsemi")
          .drop("end_date")
          .drop("current_flag")
          .withColumn("current_flag", lit(false))
          .withColumn("end_date", lit(java.time.LocalDate.now))
          .select(col("airport_key"),
            col("code"),
            col("airport_name"),
            col("city"),
            col("state_code"),
            col("state_name"),
            col("wac"),
            col("country"),
            col("start_date"),
            col("end_date"),
            col("current_flag"))
          .union(differentRowAirportDF)
          .union(newRowAirportDF)
          .orderBy(asc("code"))


        //test
        changesRowAirportDF.show(false)
        changesRowAirportDF.printSchema()
        val airportRows = changesRowAirportDF.select("airport_key").count()

        //write - changes
        changesRowAirportDF
          .write
          //  .option("compression", "snappy")
          .format("parquet")
          .mode("overwrite")
          .parquet(dim_airport_temp_loc)


        spark.read.parquet(dim_airport_temp_loc)
          .write
          // .option("compression", "snappy")
          .format("parquet")
          .mode("overwrite")
          .parquet(dim_airport_loc)


        println("\n Airport final dataframe with changes: ", airportRows)

        val writeChanges1 = spark.read.parquet(dim_airport_temp_loc)
        val writeRows = writeChanges1.select("airport_key").count()
        println("\n Airport final dimension - presentation_layer \n", writeRows)
      }
    }

  }
}
