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
    val distinctAirport = stagingAirports
      .withColumn("start_date", lit(java.time.LocalDate.now))
      .withColumn("end_date", to_date(lit("9999-12-31")))
      .withColumn("current_flag", lit(true))

    //test
    distinctAirport.show(20, false)
    distinctAirport.printSchema()
    val qAirport = distinctAirport.select("airport_key").count()
    print("Airport quantities - inicialLoad", qAirport)

    //path - save
    val dim_airport_loc = "src/datasets/presentation_layer/dim_airport"
    //write
    distinctAirport
      .write
      .option("compression", "snappy")
      .mode("overwrite")
      .parquet(dim_airport_loc)


    //test
    val distinctAirports = spark.read.parquet(dim_airport_loc)

    val qData = distinctAirports.select("airport_key", "airport_name").count()
    println("Data - presentation_layer", qData)


  }


  //Incremental load
  def IncrementalLoad(): Unit = {

    //path - save
    val dim_airport_loc = "src/datasets/presentation_layer/dim_airport"
    val dim_airport_temp_loc = "src/datasets/presentation_layer/temp/dim_airport"


    //Reading Datasets
    val currentDimAirport = spark.read.parquet(dim_airport_loc)
    val qAirportSave = currentDimAirport.select("airport_key").count()
    println("\n Airport quantities - save \n", qAirportSave)


    // Distinct row - staging
    val distinctAirports = stagingAirports
      .withColumn("start_date", lit(java.time.LocalDate.now))
      .withColumn("end_date", to_date(lit("9999-12-31")))
      .withColumn("current_flag", lit(true))

    //test
    distinctAirports.show(10, false)
    distinctAirports.printSchema()
    val qAirport = distinctAirports.select("airport_key").count()
    println("\n Airport quantities - staging \n", qAirport)


    //Search new rows
    val newAirport = distinctAirports.join(currentDimAirport, distinctAirports("airport_key") === currentDimAirport("airport_key"), "leftanti")

    //test
    val qAirportNew = newAirport.select("airport_key").count()
    print("\n Airport new \n", qAirportNew)
    newAirport.show(false)
    newAirport.printSchema()


    //Union with new rows
    val newDimAirport = currentDimAirport.union(newAirport)

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

    val newCurrentDimAirport = spark.read.parquet(dim_airport_loc)
    val writeRows = newCurrentDimAirport.select("airport_key").count()
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
      val next_pk_to_insert = distinctAirports.agg(max("airport_key")).
        collectAsList().get(0).get(0).asInstanceOf[Long] + 1


      println("Verify other changes")
      //verify changes - airports rows edit
      val change = distinctAirports.join(newCurrentDimAirport,
        distinctAirports("code") === newCurrentDimAirport("code") &&
          distinctAirports("airport_name") === newCurrentDimAirport("airport_name") &&
          distinctAirports("city") === newCurrentDimAirport("city") &&
          distinctAirports("state_code") === newCurrentDimAirport("state_code") &&
          distinctAirports("state_name") === newCurrentDimAirport("state_name") &&
          distinctAirports("wac") === newCurrentDimAirport("wac") &&
          distinctAirports("country") === newCurrentDimAirport("country"),
        "leftanti")

      change.show(false)
      val qAirportChange = change.select("airport_key").count()
      println("\n Airport with change \n", qAirportChange)

      if (qAirportChange == 0) {
        println("\n Not found new changes in the airports \n", qAirportChange)


      } else {

        val newRowAirport = change
          // .withColumn("airport_key", monotonically_increasing_id() + next_pk_to_insert)
          .select("code", "airport_name", "city", "state_code", "state_name", "wac", "country")
          .withColumn("airport_key", monotonically_increasing_id() + next_pk_to_insert)
          .withColumn("start_date", lit(java.time.LocalDate.now))
          .withColumn("end_date", to_date(lit("9999-12-31")))
          .withColumn("current_flag", lit(true))
          .select("airport_key", "code", "airport_name", "city", "state_code",
            "state_name", "wac", "country", "start_date", "end_date", "current_flag")

        //test
        newRowAirport.printSchema()
        newRowAirport.show(false)
        println("\n New rows with changes ")

        //Different rows without change
        val differentRowAirport = newCurrentDimAirport.join(change,
          newCurrentDimAirport("code") === change("code"),
          "leftanti")
        differentRowAirport.show(false)
        differentRowAirport.printSchema()
        println("\n Different rows - NewRowAirport ")


        /*Rows with match code, drop columns end_date and current_flag,
         new columns end date and current_flag and UNION with new rows, different to new rows and changes */
        val changesRowAirport = newCurrentDimAirport.join(newRowAirport,
          newCurrentDimAirport("code") === newRowAirport("code"),
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
          .union(differentRowAirport)
          .union(newRowAirport)
          .orderBy(desc("start_date"))


        //test
        changesRowAirport.show(false)
        changesRowAirport.printSchema()
        val airportRows = changesRowAirport.select("airport_key").count()


        //write - changes
        changesRowAirport
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
