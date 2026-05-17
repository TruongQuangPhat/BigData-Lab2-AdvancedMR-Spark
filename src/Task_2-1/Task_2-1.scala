package lab3.task21

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkTask21 {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: SparkTask21 <input_csv> <output_dir>")
      System.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val spark = SparkSession.builder()
      .appName("Task 2-1: Advanced Spark Structured API")
      // .master("local[*]") // uncomment if running locally without spark-submit
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val rawDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)

    // Parse fields used for date arithmetic and numeric comparison.
    val df = rawDf
      .withColumn("ParsedDate", to_date(col("Date"), "MM-dd-yy"))
      .withColumn("AmountNum", col("Amount").cast(DoubleType))

    // Job 1: Find temporally-valid promotions.
    // active period = last appearance - first appearance >= 2 days.
    val validPromotionsDf = df
      .filter(col("promotion-ids").isNotNull && trim(col("promotion-ids")) =!= "")
      .withColumn("promotion_id", explode(split(col("promotion-ids"), ",")))
      .withColumn("promotion_id", trim(col("promotion_id")))
      .filter(col("promotion_id").isNotNull && col("promotion_id") =!= "")
      .groupBy("promotion_id")
      .agg(
        min("ParsedDate").alias("first_appearance"),
        max("ParsedDate").alias("last_appearance")
      )
      .withColumn("active_period", datediff(col("last_appearance"), col("first_appearance")))
      .filter(col("active_period") >= 2)
      .select("promotion_id")

    // Job 2: Find orders having at least 3 distinct valid promotions.
    val orderPromosDf = df
      .filter(col("promotion-ids").isNotNull && trim(col("promotion-ids")) =!= "")
      .select("Order ID", "promotion-ids")
      .withColumn("promotion_id", explode(split(col("promotion-ids"), ",")))
      .withColumn("promotion_id", trim(col("promotion_id")))
      .join(validPromotionsDf, Seq("promotion_id"), "inner")
      .groupBy("Order ID")
      .agg(countDistinct("promotion_id").alias("valid_promo_count"))
      .filter(col("valid_promo_count") >= 3)
      .select("Order ID")

    // Job 3: Compute average merchant-shipped amount by state.
    val stateAvgDf = df
      .filter(col("Fulfilment") === "Merchant" && col("Courier Status") === "Shipped")
      .filter(col("ship-state").isNotNull && trim(col("ship-state")) =!= "")
      .withColumn("ship-state-upper", upper(trim(col("ship-state"))))
      .groupBy("ship-state-upper")
      .agg(avg("AmountNum").alias("state_avg_amount"))

    // Job 4: Build the cancelled-Standard base orders.
    val targetOrdersDf = df
      .filter(col("Status") === "Cancelled" && col("ship-service-level") === "Standard")
      .filter(col("ship-city").isNotNull && trim(col("ship-city")) =!= "")
      .withColumn("ship-state-upper", upper(trim(col("ship-state"))))
      .withColumn("ship-city-upper", upper(trim(col("ship-city"))))

    // Job 5: Keep orders with amount < state average and >= 3 valid promotions.
    val qualifiedOrdersDf = targetOrdersDf
      .join(stateAvgDf, Seq("ship-state-upper"), "inner")
      .filter(col("AmountNum") < col("state_avg_amount"))
      .join(orderPromosDf, Seq("Order ID"), "inner")

    // Job 6: Calculate percentage per city.
    // percentage = qualified cancelled-Standard orders / all cancelled-Standard orders.
    val denominatorDf = targetOrdersDf
      .groupBy("ship-city-upper")
      .agg(count("Order ID").alias("total_cancelled_standard"))

    val numeratorDf = qualifiedOrdersDf
      .groupBy("ship-city-upper")
      .agg(count("Order ID").alias("qualified_count"))

    val resultDf = denominatorDf
      .join(numeratorDf, Seq("ship-city-upper"), "left")
      .withColumn("qualified_count_clean", coalesce(col("qualified_count"), lit(0)))
      .withColumn("percentage", round((col("qualified_count_clean") / col("total_cancelled_standard")) * 100, 2))
      .select(
        col("ship-city-upper").alias("City"),
        col("percentage").alias("Percentage_Qualified_Cancelled_Standard")
      )
      .orderBy("City")

    println("\n=== Task 2-1: extended execution plan ===")
    resultDf.explain(true)
    println("=== End of plan ===\n")

    resultDf
      .coalesce(1)
      .write
      .mode("overwrite")
      .parquet(outputPath)

    spark.stop()
  }
}
