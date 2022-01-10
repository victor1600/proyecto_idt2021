import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object airline_dimension extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("proyecto_idt2021")
    .getOrCreate()

  //Actual version - Airline staging dimension
  val staging_airline_location = "src/datasets/staging_layer/airlines"
  val stagingAirline = spark.read.parquet(staging_airline_location)

  //Def inicial and incremental
  //InitialLoad()
  IncrementalLoad()


  //Initial load
  def InitialLoad(): Unit = {
    val distinctAirline = stagingAirline
      .withColumn("start_date", lit(java.time.LocalDate.now))
      .withColumn("end_date", to_date(lit("9999-12-31")))
      .withColumn("current_flag", lit(true))

    //test
    distinctAirline.show(20, false)
    distinctAirline.printSchema()
    val qAirline = distinctAirline.select("airline_key").count()
    print("Airline quantities - inicialLoad", qAirline)

    //path - save
    val dim_airline_loc = "src/datasets/presentation_layer/dim_airline"
    //write
    distinctAirline
      .write
      .option("compression", "snappy")
      .mode("overwrite")
      .parquet(dim_airline_loc)


    //test
    val distinctAirlines = spark.read.parquet(dim_airline_loc)

    val qData = distinctAirlines.select("airline_key", "airline_name").count()
    println("Data - presentation_layer", qData)


  }


  //Incremental load
  def IncrementalLoad(): Unit = {

    //path - save
    val dim_airline_loc = "src/datasets/presentation_layer/dim_airline"
    val dim_airline_temp_loc = "src/datasets/presentation_layer/temp/dim_airline"


    //Reading Datasets
    val currentDimAirline = spark.read.parquet(dim_airline_loc)
    val qAirlineSave = currentDimAirline.select("airline_key").count()
    println("\n Airline quantities - save \n", qAirlineSave)


    // Distinct row - staging
    val distinctAirlines = stagingAirline
      .withColumn("start_date", lit(java.time.LocalDate.now))
      .withColumn("end_date", to_date(lit("9999-12-31")))
      .withColumn("current_flag", lit(true))

    //test
    distinctAirlines.show(10, false)
    distinctAirlines.printSchema()
    val qAirline = distinctAirlines.select("airline_key").count()
    println("\n Airline quantities - staging \n", qAirline)


    //Search new rows
    val newAirline = distinctAirlines.join(currentDimAirline, distinctAirlines("airline_key") === currentDimAirline("airline_key"), "leftanti")

    //test
    val qAirlineNew = newAirline.select("airline_key").count()
    print("\n Airline new \n", qAirlineNew)
    newAirline.show(false)
    newAirline.printSchema()


    //Union with new rows
    val newDimAirline = currentDimAirline.union(newAirline)

    //test union
    newDimAirline.show(10, false)
    newDimAirline.printSchema()
    val qAirlineUnion = newDimAirline.select("airline_key").count()
    println("Airline with new rows: ", qAirlineUnion)


    //write - presentation_layer
    newDimAirline
      .write
      //  .option("compression", "snappy")
      .format("parquet")
      .mode("overwrite")
      .parquet(dim_airline_temp_loc)


    spark.read.parquet(dim_airline_temp_loc)
      .write
      // .option("compression", "snappy")
      .format("parquet")
      .mode("overwrite")
      .parquet(dim_airline_loc)
    println("Write new row - presentation_layer")

    val newCurrentDimAirline = spark.read.parquet(dim_airline_loc)
    val writeRows = newCurrentDimAirline.select("airline_key").count()
    println("\n Current \n", writeRows)


    //condition, where airline new = 0
    if (qAirlineNew == 0) {
      println("\n No new rows were found")
      ChangeRows()


    } else {
      ChangeRows()
    }

    //def SCD2
    def ChangeRows(): Unit = {

      //Autoincremet - airline_key
      val next_pk_to_insert = distinctAirlines.agg(max("airline_key")).
        collectAsList().get(0).get(0).asInstanceOf[Long] + 1


      println("Verify other changes")
      //verify changes - airlines rows edit
      val change = distinctAirlines.join(newCurrentDimAirline,
        distinctAirlines("carrier") === newCurrentDimAirline("carrier") &&
        distinctAirlines("airline_name") === newCurrentDimAirline("airline_name"),
        "leftanti")

      change.show(false)
      val qAirlineChange = change.select("airline_key").count()
      println("\n Airline with change \n", qAirlineChange)

      if (qAirlineChange == 0) {
        println("\n Not found new changes in the airlines \n", qAirlineChange)


      } else {

        val newRowAirline = change
          .select("carrier", "airline_name")
          .withColumn("airline_key", monotonically_increasing_id() + next_pk_to_insert)
          .withColumn("start_date", lit(java.time.LocalDate.now))
          .withColumn("end_date", to_date(lit("9999-12-31")))
          .withColumn("current_flag", lit(true))
          .select("airline_key", "carrier", "airline_name", "start_date", "end_date", "current_flag")

        //test
        newRowAirline.printSchema()
        newRowAirline.show(false)
        println("\n New rows with changes ")

        //Different rows without change
        val differentRowAirline = newCurrentDimAirline.join(change,
          newCurrentDimAirline("carrier") === change("carrier"),
          "leftanti")
        differentRowAirline.show(false)
        differentRowAirline.printSchema()
        println("\n Different rows - NewRowAirline ")


        /* Dimension */
        val changesRowAirline = newCurrentDimAirline.join(newRowAirline,
          newCurrentDimAirline("carrier") === newRowAirline("carrier"),
          "leftsemi")
          .drop("end_date")
          .drop("current_flag")
          .withColumn("current_flag", lit(false))
          .withColumn("end_date", lit(java.time.LocalDate.now))
          .select(col("airline_key"),
            col("carrier"),
            col("airline_name"),
            col("start_date"),
            col("end_date"),
            col("current_flag"))
          .union(differentRowAirline)
          .union(newRowAirline)
          .orderBy(desc("start_date"))


        //test
        changesRowAirline.show(false)
        changesRowAirline.printSchema()
        val airlineRows = changesRowAirline.select("airline_key").count()


        //write - changes
        changesRowAirline
          .write
          //  .option("compression", "snappy")
          .format("parquet")
          .mode("overwrite")
          .parquet(dim_airline_temp_loc)


        spark.read.parquet(dim_airline_temp_loc)
          .write
          // .option("compression", "snappy")
          .format("parquet")
          .mode("overwrite")
          .parquet(dim_airline_loc)


        println("\n Airline final dataframe with changes: ", airlineRows)

        val writeChanges1 = spark.read.parquet(dim_airline_temp_loc)
        val writeRows = writeChanges1.select("airline_key").count()
        println("\n Airline final dimension - presentation_layer \n", writeRows)
      }
    }

  }
}

