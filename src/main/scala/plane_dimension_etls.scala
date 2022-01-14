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

  def prepare_stg_planes(engines_origin_path:String="src/datasets/engines.csv", planes_origin_path:String = "src/datasets/planes_new.csv", stg_path: String) = {

    val engines_origin =  read_dataframe(engines_origin_path)

    val planes_origin =  read_dataframe(planes_origin_path)

    val planes_engines = planes_origin.join(engines_origin, Seq("Eng MFR Code"), "left").distinct()
      .select(col("N-Number").alias("n_number"),
        col("Serial Number").alias("serial_number"),
        lower(col("Name")).alias("aircraft_name"),
        col("MFR MDL Code").alias("aircraft_mfr_model"),
        col("Year MFR").alias("year_mfr"),
        lower(col("City")).alias("registrant_city"),
        lower(col("State")).alias("registrant_state"),
        lower(col("Street 1")).alias("registrant_street"),
        col("Eng MFR Code").alias("eng_mfr_model_code"),
        lower(col("eng mfr name")).alias("eng_mfr_name"),
        lower(col("engine model name")).alias("eng_model_name"),
        col("horsepower").alias("eng_horse_power")).na.fill("undefined")

        planes_engines.write.mode(SaveMode.Overwrite).parquet(stg_path)

        planes_engines

  }

  def full_load(df:DataFrame, df_write_path:String)={

    df.withColumn("start_date",lit(java.time.LocalDate.now))
      .withColumn("end_date", to_date(lit("9999-12-31")))
      .withColumn("current_flag",  lit(true))
      .withColumn("plane_key", monotonically_increasing_id +1)
      //.show(5)
      .write.mode(SaveMode.Overwrite).parquet(df_write_path)

  }

  def check_if_scd2(presentation_df_path: String, staging_df: DataFrame)={

    val current_df = spark.read.parquet(presentation_df_path)

    val existing_values = staging_df.join(current_df,
      staging_df("n_number")===current_df("n_number"), "leftsemi")

    if(existing_values.count()!=0){

      val current_df_count = current_df.select("plane_key").count()
      println("Number of rows before applying SCD2: ", current_df_count)

      val next_pk_to_insert = current_df.agg(max("plane_key")).
        collectAsList().get(0).get(0).asInstanceOf[Long] +1

      val new_rows_scd2 = existing_values.withColumn("start_date",lit(java.time.LocalDate.now))
        .withColumn("end_date", to_date(lit("9999-12-31")))
        .withColumn("current_flag",  lit(true))
        .withColumn("plane_key", monotonically_increasing_id +next_pk_to_insert)

       new_rows_scd2.show(20)

      val rows_to_update = current_df.join(staging_df,
        staging_df("n_number")===current_df("n_number"), "leftsemi")
        .withColumn("current_flag", lit(false))
        .withColumn("end_date", lit(java.time.LocalDate.now))

      rows_to_update.show(20)

      val unchanged_rows = current_df.join(staging_df,
        staging_df("n_number")===current_df("n_number"), "leftanti")

      val complete_df = unchanged_rows.union(rows_to_update).union(new_rows_scd2)
      val complete_df_count = complete_df.select("plane_key").count()
      println("Number of rows after applying SCD2: ", complete_df_count)
      complete_df.show(5)

      val temp_presentation_df_path = "src/datasets/presentation_layer/temp"
      complete_df.write.mode(SaveMode.Overwrite).parquet(temp_presentation_df_path)
      spark.read.parquet(temp_presentation_df_path).write.mode(SaveMode.Overwrite).parquet(presentation_df_path)
    }
  }

  def incremental_load(presentation_df_path: String, staging_df: DataFrame)={

    val temp_presentation_df_path = "src/datasets/presentation_layer/temp"
    check_if_scd2(presentation_df_path, staging_df)
    val current_df = spark.read.parquet(presentation_df_path)

    val new_values = staging_df.join(current_df,
      staging_df("n_number")===current_df("n_number"), "leftanti")

    // if new values, append to existing df'
    //existing_values.show(5)
    if(new_values.count() != 0){
      // If there are new values
      val next_pk_to_insert = current_df.agg(max("plane_key")).
        collectAsList().get(0).get(0).asInstanceOf[Long] +1

      val new_values_with_pk = new_values
        .withColumn("start_date",lit(java.time.LocalDate.now))
        .withColumn("end_date", to_date(lit("9999-12-31")))
        .withColumn("current_flag",  lit(true))
        .withColumn("plane_key", monotonically_increasing_id +next_pk_to_insert)

      val newDimValues = current_df.union(new_values_with_pk)
      newDimValues.orderBy(desc("plane_key")).show(10)

      newDimValues.write.mode(SaveMode.Overwrite).parquet(temp_presentation_df_path)
      spark.read.parquet(temp_presentation_df_path).write.mode(SaveMode.Overwrite).parquet(presentation_df_path)
      println("New rows written")

    } else{
      println("No new data to write!")
    }



  }

  val engines_origin_path="src/datasets/engines.csv"
  val planes_full_origin_path="src/datasets/planes.csv"
  val stg_path = "src/datasets/staging_layer/plane_dimension"
  val presentation_df_path: String="src/datasets/presentation_layer/plane_dimension"

  val stg_df = prepare_stg_planes(engines_origin_path,planes_full_origin_path,stg_path)



  val planes_incremental_origin_path="src/datasets/planes_new.csv"
  val stg_df_incremental = prepare_stg_planes(engines_origin_path,planes_incremental_origin_path, stg_path)

  val staging_dataset: String="src/datasets/staging_layer/plane_dimension"
  incremental_load(presentation_df_path, stg_df_incremental)

}
