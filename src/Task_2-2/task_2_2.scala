import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Task 2.2.2 — Population Standard Deviation with Dynamic Percentile Thresholds
 *
 * Yêu cầu:
 *   - Tính ngưỡng P80 / P90 của promo_count theo nhóm (SKU, Month) bằng 2 cách
 *   - Cách 1 (Approximate): percentile_approx / approx_percentile
 *   - Cách 2 (Exact):       Window percent_rank()
 *   - Benchmark 5 lần, tính mean & stddev thời gian chạy
 *   - Lọc đơn hàng >= ngưỡng → tính stddev_pop(Amount) mỗi nhóm
 *   - Nếu nhóm có < 2 đơn hợp lệ → stddev = 0
 *   - Gộp P80 và P90 thành 1 DataFrame → xuất Task_2-2.parquet (repartition(1))
 */
object Task22 {

  // Helper: đo thời gian thực thi (ms) của một khối code
  def timeMs(block: => Unit): Long = {
    val t0 = System.currentTimeMillis()
    block
    System.currentTimeMillis() - t0
  }

  // Helper: tính mean và population stddev của một dãy Long
  def meanStddev(samples: Seq[Long]): (Double, Double) = {
    val n    = samples.length.toDouble
    val mean = samples.sum / n
    val variance = samples.map(x => math.pow(x - mean, 2)).sum / n
    (mean, math.sqrt(variance))
  }

  // Main
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Task_2_2_StdDev_DynamicPercentile")
      // Xoá comment .master khi chạy local; bỏ dòng này khi submit lên YARN
      // .master("local[*]")
      .config("spark.sql.shuffle.partitions", "200")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // Đường dẫn — chỉnh thành HDFS path nếu chạy trên cluster
    val DATA_PATH = "/lab2/input/Amazon_Sale_Report.csv" 
    // Spark ghi parquet ra thư mục chứa part-*.parquet.
    // Ta dùng thư mục staging tạm, sau đó copy file part duy nhất
    // ra đúng tên file Task_2-2.parquet để đọc được ở local bình thường.

    val OUTPUT_PATH = "/lab2/output/Task_2-2.parquet"

    println("TASK 2.2.2  -  Dynamic-Percentile Population Std Dev")

    // STEP 1 — Chuẩn bị dữ liệu SKU-Tháng
    println("\n[STEP 1] Loading and preparing data...")

    val raw = spark.read
      .option("header",      "true")
      .option("inferSchema", "true")
      .csv(DATA_PATH)

    // Đổi tên cột có ký tự đặc biệt để dùng col() thuận tiện
    val renamed = raw
      .withColumnRenamed("Order ID",      "Order_ID")
      .withColumnRenamed("promotion-ids", "promotion_ids")

    // Bỏ hàng thiếu Amount hoặc SKU (không thể tính toán)
    val base = renamed.filter(col("Amount").isNotNull && col("SKU").isNotNull)

    // Trích Tháng từ chuỗi định dạng "MM-dd-yy"
    val withMonth = base.withColumn(
      "Month",
      month(to_date(col("Date"), "MM-dd-yy"))
    )

    // Đếm số lượng ID khuyến mãi mỗi đơn hàng:
    //   - NULL hoặc chuỗi rỗng "" → 0
    //   - Ngược lại: split theo dấu phẩy rồi đếm phần tử
    //   LƯU Ý: Phải check cả trường hợp chuỗi rỗng vì split("", ",")
    //   trả về Array("") có size = 1 (không phải 0), gây đếm sai.
    val withPromo = withMonth.withColumn(
      "promo_count",
      when(col("promotion_ids").isNull || trim(col("promotion_ids")) === "", lit(0))
        .otherwise(size(split(trim(col("promotion_ids")), ",")))
    )

    // DataFrame cốt lõi (cache để tái sử dụng nhiều lần)
    val orders = withPromo
      .select(
        col("Order_ID"),
        col("SKU"),
        col("Month"),
        col("Amount").cast("double").as("Amount"),
        col("promo_count").cast("int").as("promo_count")
      )
      .cache()

    val totalOrders = orders.count()
    println(s"   Usable orders after filtering: $totalOrders")

    // STEP 2 — Tính ngưỡng phân vị (2 cách)

    // Cách 1 (Approximate): percentile_approx
    //   - Dùng thuật toán Greenwald-Khanna sketch (accuracy = 10000)
    //   - Tham số accuracy càng cao thì càng chính xác, tốn bộ nhớ hơn
    def computeApprox(): DataFrame =
      orders
        .groupBy("SKU", "Month")
        .agg(
          percentile_approx(col("promo_count"), lit(0.80), lit(10000)).as("p80"),
          percentile_approx(col("promo_count"), lit(0.90), lit(10000)).as("p90")
        )

    // Cách 2 (Exact): Window percent_rank()
    //   - partitionBy(SKU, Month) → Spark shuffle tất cả hàng cùng nhóm
    //     về cùng 1 executor (đảm bảo tính đúng thứ hạng)
    //   - orderBy(promo_count) → sắp xếp tăng dần trong mỗi partition
    //   - percent_rank() = (rank - 1) / (N - 1)  (0.0 → 1.0)
    //   - Ngưỡng P80 = min(promo_count) với percent_rank >= 0.80
    val windowSpec = Window
      .partitionBy("SKU", "Month")
      .orderBy("promo_count")

    def computeExact(): DataFrame = {
      val ranked = orders.withColumn("prank", percent_rank().over(windowSpec))
      ranked
        .groupBy("SKU", "Month")
        .agg(
          // Dùng coalesce với max() để tránh null khi N=1 (vì khi N=1, prank luôn là 0.0)
          coalesce(min(when(col("prank") >= 0.80, col("promo_count"))), max(col("promo_count"))).as("p80"),
          coalesce(min(when(col("prank") >= 0.90, col("promo_count"))), max(col("promo_count"))).as("p90")
        )
    }

    // STEP 3 — Benchmark: chạy mỗi cách 5 lần, đo mean & stddev (ms)
    println("\n[STEP 3] Benchmarking (5 runs each)...")

    val N_RUNS = 5

    // Lưu kết quả của lần chạy CUỐI để dùng ở bước 4
    var thresholdsApprox: DataFrame = null
    var thresholdsExact:  DataFrame = null

    // Cách 1: Approximate (percentile_approx)
    // Mỗi lần: unpersist cache cũ → tính lại → cache mới → count để trigger
    // unpersist() giữa các lần đảm bảo Spark tính lại từ đầu (đo thời gian thực)
    val timesApprox = (1 to N_RUNS).map { i =>
      if (thresholdsApprox != null) thresholdsApprox.unpersist()
      val t = timeMs {
        val df = computeApprox().cache()
        df.count()   // action bắt buộc — trigger toàn bộ DAG
        thresholdsApprox = df
      }
      println(s"   [approx] Run $i: ${t}ms")
      t
    }

    // Cách 2: Exact (percent_rank Window)
    val timesExact = (1 to N_RUNS).map { i =>
      if (thresholdsExact != null) thresholdsExact.unpersist()
      val t = timeMs {
        val df = computeExact().cache()
        df.count()   // action bắt buộc
        thresholdsExact = df
      }
      println(s"   [exact]  Run $i: ${t}ms")
      t
    }

    val (meanApprox, sdApprox) = meanStddev(timesApprox)
    val (meanExact,  sdExact)  = meanStddev(timesExact)

    println("\n  Method                  Mean(ms)   Std(ms)")
    println(f"  Approach 1 (approx)     ${meanApprox}%8.1f   ${sdApprox}%8.1f")
    println(f"  Approach 2 (exact)      ${meanExact}%8.1f   ${sdExact}%8.1f")

    // STEP 4 — Lọc đơn hàng và tính stddev_pop(Amount)
    // Sử dụng thresholdsExact (kết quả chính xác) làm ngưỡng chính thức.
    // Nếu muốn dùng approx, đổi thresholdsExact → thresholdsApprox.
    println("\n[STEP 4] Computing population stddev per SKU-Month group...")

    /**
     * Với một ngưỡng cụ thể (P80 hoặc P90):
     *   1. Join orders với bảng ngưỡng
     *   2. Lọc: promo_count >= ngưỡng
     *   3. groupBy(SKU, Month) → count + stddev_pop(Amount)
     *   4. Nếu count < 2 → stddev = 0.0
     *   5. Thêm cột nhãn percentile_level (P80 / P90)
     */
    def buildResult(
        thresholdsDF:    DataFrame,
        thresholdCol:    String,          // "p80" hoặc "p90"
        levelLabel:      String           // "P80" hoặc "P90"
    ): DataFrame = {

      // Join để gắn ngưỡng vào từng đơn hàng
      val joined = orders.join(
        thresholdsDF.select("SKU", "Month", thresholdCol),
        Seq("SKU", "Month"),
        "inner"
      )

      // Lọc: chỉ giữ đơn có promo_count >= ngưỡng phân vị
      val filtered = joined.filter(col("promo_count") >= col(thresholdCol))

      // Tính count và stddev_pop cho mỗi nhóm
      filtered
        .groupBy("SKU", "Month")
        .agg(
          count("Order_ID").as("order_count"),
          stddev_pop(col("Amount")).as("_raw_stddev")
        )
        // Logic ngoại lệ: nhóm < 2 đơn hợp lệ → gán stddev = 0
        .withColumn(
          "stddev_amount",
          when(col("order_count") < 2, lit(0.0))
            .otherwise(col("_raw_stddev"))
        )
        .drop("_raw_stddev")
        .withColumn("percentile_level", lit(levelLabel))
    }

    // Tính cho P80 và P90 (dùng kết quả exact làm ngưỡng chính thức)
    val resultP80 = buildResult(thresholdsExact, "p80", "P80")
    val resultP90 = buildResult(thresholdsExact, "p90", "P90")

    // STEP 5 — Gộp P80 + P90 thành 1 DataFrame, xuất PARQUET trực tiếp lên HDFS
    println("\n[STEP 5] Merging results and writing Parquet to HDFS...")

    val finalDF = resultP80
      .unionByName(resultP90)
      .select(
        col("SKU"),
        col("Month"),
        col("percentile_level"),
        col("order_count"),
        col("stddev_amount")
      )

    // Lưu ra thư mục staging tạm
    val STAGING_PATH = OUTPUT_PATH + "_staging"
    finalDF
      .repartition(1)
      .write
      .mode("overwrite")
      .parquet(STAGING_PATH)

    // Dùng Hadoop FileSystem API để copy file part-*.parquet thành file duy nhất đúng với yêu cầu
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val stagingDir = new Path(STAGING_PATH)
    val outputFile = new Path(OUTPUT_PATH)

    if (fs.exists(outputFile)) {
      fs.delete(outputFile, true)
    }

    val filesStatus = fs.listStatus(stagingDir)
    val partFile = filesStatus.find(_.getPath.getName.startsWith("part-"))
    
    partFile.foreach { f =>
      fs.rename(f.getPath, outputFile)
    }

    // Xoá thư mục staging
    fs.delete(stagingDir, true)

    println(s"   [OK] Data exported to single file: $OUTPUT_PATH")

    // STEP 6 — So sánh nhanh approx vs exact (phục vụ báo cáo)
    println("\n[STEP 6] Accuracy comparison: approx vs exact thresholds...")

    val combined = thresholdsApprox
      .withColumnRenamed("p80", "p80_approx")
      .withColumnRenamed("p90", "p90_approx")
      .join(
        thresholdsExact
          .withColumnRenamed("p80", "p80_exact")
          .withColumnRenamed("p90", "p90_exact"),
        Seq("SKU", "Month"),
        "inner"
      )

    val totalGroups = combined.count()

    val mismatchP80 = combined
      .filter(col("p80_approx") =!= col("p80_exact"))
    val mismatchP90 = combined
      .filter(col("p90_approx") =!= col("p90_exact"))

    val nMismatchP80 = mismatchP80.count()
    val nMismatchP90 = mismatchP90.count()

    println(s"   Total SKU-Month groups         : $totalGroups")
    println(s"   Groups with P80 mismatch       : $nMismatchP80 " +
            f"(${nMismatchP80 * 100.0 / totalGroups}%.1f%%)")
    println(s"   Groups with P90 mismatch       : $nMismatchP90 " +
            f"(${nMismatchP90 * 100.0 / totalGroups}%.1f%%)")

    if (nMismatchP80 > 0) {
      println("\n   Sample P80 mismatches (top 5):")
      mismatchP80
        .select("SKU", "Month", "p80_approx", "p80_exact")
        .show(5, truncate = false)
    }
    if (nMismatchP90 > 0) {
      println("\n   Sample P90 mismatches (top 5):")
      mismatchP90
        .select("SKU", "Month", "p90_approx", "p90_exact")
        .show(5, truncate = false)
    }

    // Nhóm nào có tập đơn hàng hợp lệ khác nhau?
    val p80Approx = buildResult(thresholdsApprox, "p80", "P80")
      .select(col("SKU"), col("Month"), col("order_count").as("count_approx"))
    val p80Exact  = resultP80
      .select(col("SKU"), col("Month"), col("order_count").as("count_exact"))

    val orderDiff = p80Approx
      .join(p80Exact, Seq("SKU", "Month"), "inner")
      .filter(col("count_approx") =!= col("count_exact"))

    val nOrderDiff = orderDiff.count()
    println(s"\n   Groups (P80) with different eligible order counts: $nOrderDiff")
    if (nOrderDiff > 0) orderDiff.show(10, truncate = false)

    // Có nhóm nào > 1,000 đơn không?
    val largeGroups = orders
      .groupBy("SKU", "Month")
      .agg(count("Order_ID").as("cnt"))
      .filter(col("cnt") > 1000)
    val nLarge = largeGroups.count()
    println(s"\n   SKU-Month groups with > 1,000 orders: $nLarge")
    if (nLarge > 0) largeGroups.orderBy(col("cnt").desc).show(10, truncate = false)

    println("\n[OK] Task 2.2.2 completed successfully.")
    println(s"  Output: $OUTPUT_PATH")

    spark.stop()
  }
}
