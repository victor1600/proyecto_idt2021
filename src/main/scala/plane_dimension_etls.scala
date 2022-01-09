import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions._





object plane_dimension_etls extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()
  import spark.implicits._


  def read_dataframe(dataset_path: String):DataFrame={

      spark.read
      .option("sep", ",")
      .option("header", true)
      .option("inferSchema", true)
      .csv(dataset_path)

  }

  def prepare_stg_planes(engines_origin_path:String="src/datasets/engines.csv", planes_origin_path:String = "src/datasets/planes_new.csv") = {
    val stg_path = "src/datasets/staging"

    val engines_origin =  read_dataframe(engines_origin_path)

    val planes_origin =  read_dataframe(planes_origin_path)

    val planes_engines = planes_origin.join(engines_origin, Seq("Eng MFR Code"), "left").distinct()
      .select(col("N-Number").alias("n_number"),
        col("Serial Number").alias("serial_number"),
        col("MFR MDL Code").alias("aircraft_mfr_model"),
        col("Year MFR").alias("year_mfr"),
        col("City").alias("registrant_city"),
        col("State").alias("registrant_state"),
        col("Street 1").alias("registrant_street"),
        col("Eng MFR Code").alias("eng_mfr_model_code"),
        col("eng mfr name").alias("eng_mfr_name"),
        col("engine model name").alias("eng_model_name"),
        col("horsepower").alias("eng_horse_power")).na.fill("UNDEFINED")

        planes_engines.write.mode(SaveMode.Overwrite).parquet(stg_path)

        planes_engines



  }

  def full_load(df:DataFrame, df_write_path:String="src/datasets/presentation")={

//    df.withColumn("start_date",lit(java.time.LocalDate.now))
//      .withColumn("end_date", to_date(lit("9999-12-31")))
//      .withColumn("current_flag",  lit(true))
      df.withColumn("plane_key", monotonically_increasing_id +1)
//      .show(5)
      .write.mode(SaveMode.Overwrite).parquet(df_write_path)

  }

  def incremental_load(presentation_df_path: String="src/datasets/presentation", staging_dataset: String="src/datasets/staging")={
    val temp_presentation_df_path = presentation_df_path
    val current_df = spark.read.parquet(presentation_df_path)
    val staging_df =  spark.read.parquet(staging_dataset)

    val new_values = staging_df.join(current_df,
      staging_df("n_number")===current_df("n_number"), "leftanti")
        //new_values.show(5)
    // if new values, append to existing df
    //val new_values_count = new_values.count()
    if(new_values.count() != 0){
      // If there are new values
      val next_pk_to_insert = current_df.agg(max("plane_key")).
        collectAsList().get(0).get(0).asInstanceOf[Long] +1

      val new_values_with_pk = new_values.withColumn("plane_key", monotonically_increasing_id +next_pk_to_insert)
      val newDimValues = current_df.union(new_values_with_pk)

      newDimValues.show(5)
      newDimValues.write.mode(SaveMode.Overwrite).parquet(temp_presentation_df_path)
      spark.read.parquet(temp_presentation_df_path).write.mode(SaveMode.Overwrite).parquet(presentation_df_path)
      println("New rows written")

    } else{
      println("No new data to write!")
    }




    // check if df is not empty



    // Insert new records


//    println(next_pk_to_insert)



  }
  //
  val engines_origin_path="src/datasets/engines.csv"
  val planes_full_origin_path="src/datasets/planes.csv"
//
//  val stg_df = prepare_stg_planes(engines_origin_path,planes_full_origin_path)
//  full_load(stg_df)
  val planes_incremental_origin_path="src/datasets/planes_new.csv"
  val stg_df_incremental = prepare_stg_planes(engines_origin_path,planes_incremental_origin_path)
  val presentation_df_path: String="src/datasets/presentation"
  val staging_dataset: String="src/datasets/staging"
  incremental_load(presentation_df_path, staging_dataset)

}
