import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions._



object plane_dimension_etls extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()


  def read_dataframe(dataset_path: String):DataFrame={

      spark.read
      .option("sep", ",")
      .option("header", true)
      .option("inferSchema", true)
      .csv(dataset_path)

  }

  def full_load() = {
    val engines_origin_path = "src/datasets/engines.csv"
    val planes_origin_path = "src/datasets/planes2.csv"

    val engines_origin =  read_dataframe(engines_origin_path)

    val planes_origin =  read_dataframe(planes_origin_path)



    val planes_engines = planes_origin.join(engines_origin, Seq("Eng MFR Code"), "left").distinct()
      .withColumn("plane_key", monotonically_increasing_id+ 1)
      .select(col("plane_key"), col("N-Number").alias("n_number"),
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
        .withColumn("start_date",lit(java.time.LocalDate.now))
        .withColumn("end_date", to_date(lit("9999-12-31")))
        .withColumn("current_flag",  lit(true))




    planes_engines.show(5)


  }

  full_load()

}
