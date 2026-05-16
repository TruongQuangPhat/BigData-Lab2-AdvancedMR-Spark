import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Task 2.2 — Dynamic Percentile Thresholds & Population StdDev
 *
 * Yêu cầu chính:
 *   - Với mỗi SKU trong mỗi tháng, tính stddev_pop(Amount) của các đơn hàng
 *     có số promotion >= ngưỡng percentile động.
 *   - Hai mức percentile: P80 và P90.
 *   - Số promotion = số promotion identifiers gắn với order, bao gồm Amazon-issued promotions.
 *   - Nếu nhóm sau lọc có < 2 đơn hợp lệ thì stddev = 0.
 *   - Cài 2 cách tính percentile:
 *       1. Approximate: percentile_approx / approx_percentile
 *       2. Exact: tự cài bằng DataFrame/Window, dùng nearest-rank percentile
 *   - Benchmark mỗi cách ít nhất 5 lần, báo cáo mean và stddev thời gian chạy.
 *   - So sánh threshold approx vs exact và các nhóm có qualifying orders khác nhau.
 *   - Kiểm tra nhóm SKU-month có > 1000 orders.
 *   - Xuất kết quả cuối ra single Parquet file: Task_2-2.parquet
 */
object Task22 {

  def timeMs(block: => Unit): Long = {
    val t0 = System.currentTimeMillis()
    block
    System.currentTimeMillis() - t0
  }

  def meanStddev(samples: Seq[Long]): (Double, Double) = {
    val n = samples.length.toDouble
    val mean = samples.sum / n
    val variance = samples.map(x => math.pow(x - mean, 2)).sum / n
    (mean, math.sqrt(variance))
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Task_2_2_Dynamic_Percentile_StdDev")
      .config("spark.sql.shuffle.partitions", "200")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val DATA_PATH = if (args.length >= 1) args(0) else "/lab2/input/Amazon_Sale_Report.csv"
    val OUTPUT_PATH = if (args.length >= 2) args(1) else "/lab2/output/Task_2-2.parquet"

    println("TASK 2.2 — Dynamic Percentile Population StdDev")
    println(s"Input : $DATA_PATH")
    println(s"Output: $OUTPUT_PATH")

    // STEP 1 — Load và chuẩn hóa dữ liệu
    println("\n[STEP 1] Loading and preparing data...")

    val raw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(DATA_PATH)

    val renamed = raw
      .withColumnRenamed("Order ID", "Order_ID")
      .withColumnRenamed("promotion-ids", "promotion_ids")

    val prepared = renamed
      .withColumn("OrderDate", to_date(col("Date"), "MM-dd-yy"))
      .withColumn("Month", month(col("OrderDate")))
      .filter(
        col("Order_ID").isNotNull &&
        col("SKU").isNotNull &&
        col("Amount").isNotNull &&
        col("OrderDate").isNotNull &&
        col("Month").isNotNull
      )
      .withColumn(
        "promo_count",
        when(col("promotion_ids").isNull || trim(col("promotion_ids")) === "", lit(0))
          .otherwise(size(split(trim(col("promotion_ids")), ",")))
      )

    val orders = prepared
      .select(
        col("Order_ID"),
        col("SKU"),
        col("Month"),
        col("Amount").cast("double").as("Amount"),
        col("promo_count").cast("int").as("promo_count")
      )
      .cache()

    val totalOrders = orders.count()
    println(s"Usable orders after filtering: $totalOrders")

    // STEP 2 — Tính percentile threshold bằng 2 cách
    /*
     * Approach 1 — Approximate percentile.
     */
    def computeApprox(): DataFrame = {
      orders
        .groupBy("SKU", "Month")
        .agg(
          percentile_approx(col("promo_count"), lit(0.80), lit(10000)).as("p80"),
          percentile_approx(col("promo_count"), lit(0.90), lit(10000)).as("p90")
        )
    }

    /*
     * Approach 2 — Exact percentile tự cài bằng nearest-rank.
     *
     * Với mỗi nhóm SKU-Month:
     *   n = tổng số order
     *   rank_p80 = ceil(0.80 * n)
     *   rank_p90 = ceil(0.90 * n)
     *
     * Threshold là promo_count nhỏ nhất sao cho cumulative frequency >= rank.
     */
    def computeExact(): DataFrame = {
      val counts = orders
        .groupBy("SKU", "Month", "promo_count")
        .agg(count(lit(1)).as("freq"))

      val cdfWindow = Window
        .partitionBy("SKU", "Month")
        .orderBy("promo_count")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

      val groupWindow = Window.partitionBy("SKU", "Month")

      val cdf = counts
        .withColumn("cum_freq", sum(col("freq")).over(cdfWindow))
        .withColumn("n", sum(col("freq")).over(groupWindow))
        .withColumn("rank_p80", ceil(col("n") * lit(0.80)))
        .withColumn("rank_p90", ceil(col("n") * lit(0.90)))

      val p80 = cdf
        .filter(col("cum_freq") >= col("rank_p80"))
        .groupBy("SKU", "Month")
        .agg(min(col("promo_count")).as("p80"))

      val p90 = cdf
        .filter(col("cum_freq") >= col("rank_p90"))
        .groupBy("SKU", "Month")
        .agg(min(col("promo_count")).as("p90"))

      p80.join(p90, Seq("SKU", "Month"), "inner")
    }

    // STEP 3 — Benchmark 5 lần
    println("\n[STEP 3] Benchmarking percentile approaches...")

    val N_RUNS = 5

    var thresholdsApprox: DataFrame = null
    var thresholdsExact: DataFrame = null

    val timesApprox = (1 to N_RUNS).map { i =>
      if (thresholdsApprox != null) thresholdsApprox.unpersist(blocking = true)

      val t = timeMs {
        val df = computeApprox().cache()
        df.count()
        thresholdsApprox = df
      }

      println(s"[approx] Run $i: ${t} ms")
      t
    }

    val timesExact = (1 to N_RUNS).map { i =>
      if (thresholdsExact != null) thresholdsExact.unpersist(blocking = true)

      val t = timeMs {
        val df = computeExact().cache()
        df.count()
        thresholdsExact = df
      }

      println(s"[exact]  Run $i: ${t} ms")
      t
    }

    val (meanApprox, sdApprox) = meanStddev(timesApprox)
    val (meanExact, sdExact) = meanStddev(timesExact)

    println("\nBenchmark summary:")
    println("Method                  Mean(ms)      Stddev(ms)")
    println(f"Approx percentile       $meanApprox%10.2f      $sdApprox%10.2f")
    println(f"Exact nearest-rank      $meanExact%10.2f      $sdExact%10.2f")

    // STEP 4 — Build result cho từng percentile
    println("\n[STEP 4] Computing population stddev after percentile filtering...")

    def buildResult(
        thresholdsDF: DataFrame,
        thresholdCol: String,
        levelLabel: String,
        methodLabel: String
    ): DataFrame = {

      val joined = orders.join(
        thresholdsDF.select("SKU", "Month", thresholdCol),
        Seq("SKU", "Month"),
        "inner"
      )

      val filtered = joined.filter(col("promo_count") >= col(thresholdCol))

      filtered
        .groupBy("SKU", "Month")
        .agg(
          count("Order_ID").as("qualifying_orders"),
          stddev_pop(col("Amount")).as("_raw_stddev")
        )
        .withColumn(
          "stddev_amount",
          when(col("qualifying_orders") < 2, lit(0.0))
            .otherwise(col("_raw_stddev"))
        )
        .drop("_raw_stddev")
        .withColumn("percentile_level", lit(levelLabel))
        .withColumn("method", lit(methodLabel))
    }

    val resultP80Approx = buildResult(thresholdsApprox, "p80", "P80", "approx")
    val resultP90Approx = buildResult(thresholdsApprox, "p90", "P90", "approx")
    val resultP80Exact = buildResult(thresholdsExact, "p80", "P80", "exact")
    val resultP90Exact = buildResult(thresholdsExact, "p90", "P90", "exact")

    // STEP 5 — Tạo final DataFrame dạng wide để dễ kiểm tra/chấm
    println("\n[STEP 5] Merging final output...")

    val totalOrdersDF = orders
      .groupBy("SKU", "Month")
      .agg(count("Order_ID").as("total_orders"))

    val threshApprox = thresholdsApprox
      .withColumnRenamed("p80", "threshold_p80_approx")
      .withColumnRenamed("p90", "threshold_p90_approx")

    val threshExact = thresholdsExact
      .withColumnRenamed("p80", "threshold_p80_exact")
      .withColumnRenamed("p90", "threshold_p90_exact")

    val resP80Approx = resultP80Approx
      .select(
        col("SKU"),
        col("Month"),
        col("qualifying_orders").as("orders_p80_approx"),
        col("stddev_amount").as("stddev_p80_approx")
      )

    val resP90Approx = resultP90Approx
      .select(
        col("SKU"),
        col("Month"),
        col("qualifying_orders").as("orders_p90_approx"),
        col("stddev_amount").as("stddev_p90_approx")
      )

    val resP80Exact = resultP80Exact
      .select(
        col("SKU"),
        col("Month"),
        col("qualifying_orders").as("orders_p80_exact"),
        col("stddev_amount").as("stddev_p80_exact")
      )

    val resP90Exact = resultP90Exact
      .select(
        col("SKU"),
        col("Month"),
        col("qualifying_orders").as("orders_p90_exact"),
        col("stddev_amount").as("stddev_p90_exact")
      )

    val joinCols = Seq("SKU", "Month")

    val finalDF = totalOrdersDF
      .join(threshApprox, joinCols, "inner")
      .join(threshExact, joinCols, "inner")
      .join(resP80Approx, joinCols, "left")
      .join(resP90Approx, joinCols, "left")
      .join(resP80Exact, joinCols, "left")
      .join(resP90Exact, joinCols, "left")
      .na.fill(
        0.0,
        Seq(
          "stddev_p80_approx",
          "stddev_p90_approx",
          "stddev_p80_exact",
          "stddev_p90_exact"
        )
      )
      .na.fill(
        0,
        Seq(
          "orders_p80_approx",
          "orders_p90_approx",
          "orders_p80_exact",
          "orders_p90_exact"
        )
      )
      .select(
        col("SKU"),
        col("Month"),
        col("total_orders"),

        col("threshold_p80_approx"),
        col("orders_p80_approx"),
        col("stddev_p80_approx"),

        col("threshold_p90_approx"),
        col("orders_p90_approx"),
        col("stddev_p90_approx"),

        col("threshold_p80_exact"),
        col("orders_p80_exact"),
        col("stddev_p80_exact"),

        col("threshold_p90_exact"),
        col("orders_p90_exact"),
        col("stddev_p90_exact")
      )

    finalDF.cache()
    println(s"Final output groups: ${finalDF.count()}")

    // STEP 6 — Accuracy comparison approx vs exact
    println("\n[STEP 6] Comparing approx vs exact thresholds...")

    val thresholdComparison = thresholdsApprox
      .withColumnRenamed("p80", "p80_approx")
      .withColumnRenamed("p90", "p90_approx")
      .join(
        thresholdsExact
          .withColumnRenamed("p80", "p80_exact")
          .withColumnRenamed("p90", "p90_exact"),
        Seq("SKU", "Month"),
        "inner"
      )
      .withColumn("p80_diff", col("p80_approx") - col("p80_exact"))
      .withColumn("p90_diff", col("p90_approx") - col("p90_exact"))

    val totalGroups = thresholdComparison.count()

    val mismatchP80 = thresholdComparison.filter(col("p80_approx") =!= col("p80_exact"))
    val mismatchP90 = thresholdComparison.filter(col("p90_approx") =!= col("p90_exact"))

    val nMismatchP80 = mismatchP80.count()
    val nMismatchP90 = mismatchP90.count()

    println(s"Total SKU-month groups      : $totalGroups")
    println(f"P80 threshold mismatches    : $nMismatchP80 (${nMismatchP80 * 100.0 / totalGroups}%.2f%%)")
    println(f"P90 threshold mismatches    : $nMismatchP90 (${nMismatchP90 * 100.0 / totalGroups}%.2f%%)")

    println("\nTop P80 threshold differences:")
    if (nMismatchP80 > 0) {
      mismatchP80
        .withColumn("abs_p80_diff", abs(col("p80_diff")))
        .orderBy(col("abs_p80_diff").desc)
        .drop("abs_p80_diff")
        .show(20, false)
    } else {
      println("No P80 threshold differences found.")
    }

    println("\nTop P90 threshold differences:")
    if (nMismatchP90 > 0) {
      mismatchP90
        .withColumn("abs_p90_diff", abs(col("p90_diff")))
        .orderBy(col("abs_p90_diff").desc)
        .drop("abs_p90_diff")
        .show(20, false)
    } else {
      println("No P90 threshold differences found.")
    }

    // STEP 7 — So sánh qualifying orders và stddev giữa approx vs exact
    println("\n[STEP 7] Comparing qualifying order sets approx vs exact...")

    val p80OrderDiff = finalDF
      .filter(
        col("orders_p80_approx") =!= col("orders_p80_exact") ||
        round(col("stddev_p80_approx"), 6) =!= round(col("stddev_p80_exact"), 6)
      )

    val p90OrderDiff = finalDF
      .filter(
        col("orders_p90_approx") =!= col("orders_p90_exact") ||
        round(col("stddev_p90_approx"), 6) =!= round(col("stddev_p90_exact"), 6)
      )

    val nP80OrderDiff = p80OrderDiff.count()
    val nP90OrderDiff = p90OrderDiff.count()

    println(s"P80 groups with different qualifying order counts/stddev: $nP80OrderDiff")
    println(s"P90 groups with different qualifying order counts/stddev: $nP90OrderDiff")

    println("\nSample P80 order/stddev differences:")
    if (nP80OrderDiff > 0) {
      p80OrderDiff.show(20, false)
    } else {
      println("No P80 order/stddev differences found.")
    }

    println("\nSample P90 order/stddev differences:")
    if (nP90OrderDiff > 0) {
      p90OrderDiff.show(20, false)
    } else {
      println("No P90 order/stddev differences found.")
    }

    // STEP 8 — Kiểm tra nhóm lớn > 1000 orders
    println("\n[STEP 8] Checking large SKU-month groups...")

    val largeGroups = totalOrdersDF
      .filter(col("total_orders") > 1000)
      .orderBy(desc("total_orders"))

    val nLargeGroups = largeGroups.count()
    println(s"Number of SKU-month groups with more than 1000 orders: $nLargeGroups")

    if (nLargeGroups > 0) {
      println("\nLarge groups:")
      largeGroups.show(50, false)

    }

    // STEP 9 — Xuất single Parquet file
    println("\n[STEP 9] Writing output as a single Parquet file...")

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val outputFile = new Path(OUTPUT_PATH)
    val stagingDir = new Path(OUTPUT_PATH + "_staging")

    if (fs.exists(stagingDir)) {
      fs.delete(stagingDir, true)
    }

    if (fs.exists(outputFile)) {
      fs.delete(outputFile, true)
    }

    finalDF
      .repartition(1)
      .write
      .mode("overwrite")
      .parquet(stagingDir.toString)

    val partFiles = fs
      .listStatus(stagingDir)
      .filter(status => status.getPath.getName.startsWith("part-") && status.getPath.getName.endsWith(".parquet"))

    if (partFiles.isEmpty) {
      throw new RuntimeException("No part-*.parquet file found in staging output.")
    }

    val partFile = partFiles(0).getPath

    val renamedOk = fs.rename(partFile, outputFile)
    if (!renamedOk) {
      throw new RuntimeException(s"Failed to rename $partFile to $outputFile")
    }

    fs.delete(stagingDir, true)

    println(s"Single Parquet file written to: $OUTPUT_PATH")

    // STEP 10 — Test đọc lại output bằng Spark
    println("\n[STEP 10] Verifying output can be read by Spark...")

    val verifyDF = spark.read.parquet(OUTPUT_PATH)
    println(s"Verified output rows: ${verifyDF.count()}")

    println("Task 2.2 completed successfully.")

    orders.unpersist()
    finalDF.unpersist()
    thresholdsApprox.unpersist()
    thresholdsExact.unpersist()

    spark.stop()
  }
}