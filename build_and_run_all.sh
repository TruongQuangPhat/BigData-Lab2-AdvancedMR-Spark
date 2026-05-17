#!/bin/bash

set -e

echo "============================================================"
echo "CHUẨN BỊ MÔI TRƯỜNG BUILD & RUN"
echo "============================================================"

mkdir -p results

if [[ -n "${SPARK_HOME:-}" && -x "$SPARK_HOME/bin/spark-submit" ]]; then
  echo "Sử dụng SPARK_HOME từ biến môi trường: $SPARK_HOME"

elif [[ -x "/opt/spark/bin/spark-submit" ]]; then
  export SPARK_HOME="/opt/spark"
  echo "Sử dụng SPARK_HOME: $SPARK_HOME"

elif [[ -x "$HOME/spark/bin/spark-submit" ]]; then
  export SPARK_HOME="$HOME/spark"
  echo "Sử dụng SPARK_HOME: $SPARK_HOME"

elif command -v spark-submit >/dev/null 2>&1; then
  SPARK_SUBMIT_PATH="$(command -v spark-submit)"
  export SPARK_HOME="$(cd "$(dirname "$SPARK_SUBMIT_PATH")/.." && pwd)"
  echo "Tự động phát hiện SPARK_HOME từ PATH: $SPARK_HOME"

else
  echo "LỖI: Không tìm thấy Spark."
  echo "Vui lòng cài Spark hoặc cấu hình SPARK_HOME thủ công."
  echo "Ví dụ:"
  echo "  export SPARK_HOME=\$HOME/spark"
  echo "  export PATH=\$SPARK_HOME/bin:\$SPARK_HOME/sbin:\$PATH"
  exit 1
fi

export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

if [[ ! -d "$SPARK_HOME/jars" ]]; then
  echo "LỖI: SPARK_HOME đang là '$SPARK_HOME' nhưng không tìm thấy thư mục '$SPARK_HOME/jars'."
  exit 1
fi

HDFS_URI=$(hdfs getconf -confKey fs.defaultFS)

if [[ -z "$HDFS_URI" ]]; then
  echo "LỖI: Không lấy được fs.defaultFS từ cấu hình Hadoop."
  exit 1
fi

echo "SPARK_HOME=$SPARK_HOME"
echo "HDFS_URI=$HDFS_URI"

echo "============================================================"
echo "CHUẨN BỊ DỮ LIỆU INPUT TRÊN HDFS"
echo "============================================================"

hadoop fs -mkdir -p /lab03/input/
hadoop fs -put -f data/Amazon_Sale_Report.csv /lab03/input/

SPARK_INPUT_PATH="${HDFS_URI}/lab03/input/Amazon_Sale_Report.csv"
SPARK_TASK21_OUTPUT_PATH="${HDFS_URI}/lab03/output/Task_2-1.parquet"

echo "Spark input path: $SPARK_INPUT_PATH"
echo "Spark Task 2-1 output path: $SPARK_TASK21_OUTPUT_PATH"

export HADOOP_CLASSPATH=$(hadoop classpath):/usr/share/scala/lib/scala-library.jar
export SPARK_CLASSPATH=$(find "$SPARK_HOME/jars" -name "*.jar" | tr '\n' ':')

echo "============================================================"
echo "BUILD & RUN TASK 1-1: SLIDING WINDOW"
echo "============================================================"

cd src/Task_1-1

echo "--- Đang biên dịch và đóng gói Task_1-1 ---"

mkdir -p classes
rm -rf classes/*
rm -f SlidingWindowJob.jar

scalac -classpath "$HADOOP_CLASSPATH" -d classes Task_1-1.scala
jar -cvf SlidingWindowJob.jar -C classes .

echo "--- Đang xóa output HDFS cũ của Task_1-1 nếu tồn tại ---"

hadoop fs -rm -r -f /lab03/output/task1-1 > /dev/null 2>&1 || true

echo "--- Đang chạy Hadoop MapReduce job cho Task_1-1 ---"

hadoop jar SlidingWindowJob.jar lab03.SlidingWindowJob \
  /lab03/input/Amazon_Sale_Report.csv \
  /lab03/output/task1-1

echo "--- Đang lấy kết quả Task_1-1 từ HDFS về thư mục results/ ---"

mkdir -p ../../results
rm -f ../../results/Task_1-1.csv

hadoop fs -getmerge /lab03/output/task1-1 ../../results/Task_1-1.csv
sed -i '2,${/^State,TargetDate/d}' ../../results/Task_1-1.csv

echo "Đã lưu kết quả Task_1-1 tại: results/Task_1-1.csv"

cd ../..

echo "============================================================"
echo "BUILD & RUN TASK 1-2: MEDIAN VARIETY"
echo "============================================================"

cd src/Task_1-2

echo "--- Đang biên dịch và đóng gói Task_1-2 ---"

mkdir -p classes
rm -rf classes/*
rm -f MedianVarietyJob.jar

scalac -classpath "$HADOOP_CLASSPATH" -d classes Task_1-2.scala
jar -cvf MedianVarietyJob.jar -C classes .

echo "--- Đang xóa output HDFS cũ của Task_1-2 nếu tồn tại ---"

hadoop fs -rm -r -f /lab03/output/task1-2-temp > /dev/null 2>&1 || true
hadoop fs -rm -r -f /lab03/output/task1-2 > /dev/null 2>&1 || true

echo "--- Đang chạy Hadoop MapReduce job cho Task_1-2 ---"

hadoop jar MedianVarietyJob.jar lab03.MedianVarietyJob \
  /lab03/input/Amazon_Sale_Report.csv \
  /lab03/output/task1-2-temp \
  /lab03/output/task1-2

echo "--- Đang lấy kết quả Task_1-2 từ HDFS về thư mục results/ ---"

mkdir -p ../../results
rm -f ../../results/Task_1-2.csv

hadoop fs -getmerge /lab03/output/task1-2 ../../results/Task_1-2.csv
sed -i '2,${/^Month,State/d}' ../../results/Task_1-2.csv

echo "Đã lưu kết quả Task_1-2 tại: results/Task_1-2.csv"

cd ../..

echo "============================================================"
echo "BUILD & RUN TASK 2-1: SPARK STRUCTURED API"
echo "============================================================"

cd src/Task_2-1

echo "--- Đang biên dịch và đóng gói Task_2-1 ---"

mkdir -p classes
rm -rf classes/*
rm -f SparkTask21.jar

scalac -classpath "$HADOOP_CLASSPATH:$SPARK_CLASSPATH" -d classes Task_2-1.scala
jar -cvf SparkTask21.jar -C classes lab3

echo "--- Đang xóa output HDFS cũ của Task_2-1 nếu tồn tại ---"

hadoop fs -rm -r -f /lab03/output/Task_2-1.parquet > /dev/null 2>&1 || true
hadoop fs -rm -r -f /lab03/output/Task_2-1.parquet_staging > /dev/null 2>&1 || true

echo "--- Đang chạy Spark job cho Task_2-1 ---"

spark-submit \
  --class lab3.task21.SparkTask21 \
  --master local[*] \
  SparkTask21.jar \
  "$SPARK_INPUT_PATH" \
  "$SPARK_TASK21_OUTPUT_PATH"

echo "--- Đang lấy kết quả Task_2-1 từ HDFS về thư mục results/ ---"

mkdir -p ../../results
rm -rf ../../results/Task_2-1.parquet

hadoop fs -get /lab03/output/Task_2-1.parquet ../../results/Task_2-1.parquet

echo "Đã lưu kết quả Task_2-1 tại: results/Task_2-1.parquet"

cd ../..

echo "============================================================"
echo "HOÀN TẤT BUILD VÀ CHẠY TOÀN BỘ PROJECT"
echo "============================================================"
echo "Kết quả được lưu tại:"
echo "- results/Task_1-1.csv"
echo "- results/Task_1-2.csv"
echo "- results/Task_2-1.parquet"
echo "============================================================"
