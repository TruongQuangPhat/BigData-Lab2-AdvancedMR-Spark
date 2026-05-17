#!/bin/bash

set -e

echo "--- PREPARING HDFS ---"

mkdir -p results

: "${SPARK_HOME:=/opt/spark}"

hadoop fs -mkdir -p /lab03/input/
hadoop fs -put -f data/Amazon_Sale_Report.csv /lab03/input/

export HADOOP_CLASSPATH=$(hadoop classpath):/usr/share/scala/lib/scala-library.jar
export SPARK_CLASSPATH=$(find "$SPARK_HOME/jars" -name "*.jar" | tr '\n' ':')

echo "--- BUILDING & RUNNING TASK 1-1 ---"

cd src/Task_1-1

mkdir -p classes
rm -rf classes/*
rm -f SlidingWindowJob.jar

scalac -classpath "$HADOOP_CLASSPATH" -d classes Task_1-1.scala
jar -cvf SlidingWindowJob.jar -C classes .

hadoop fs -rm -r -f /lab03/output/task1-1 > /dev/null 2>&1 || true

hadoop jar SlidingWindowJob.jar lab03.SlidingWindowJob \
  /lab03/input/Amazon_Sale_Report.csv \
  /lab03/output/task1-1

mkdir -p ../../results
rm -f ../../results/Task_1-1.csv

hadoop fs -getmerge /lab03/output/task1-1 ../../results/Task_1-1.csv
sed -i '2,${/^State,TargetDate/d}' ../../results/Task_1-1.csv

cd ../..

echo "--- BUILDING & RUNNING TASK 1-2 ---"

cd src/Task_1-2

mkdir -p classes
rm -rf classes/*
rm -f MedianVarietyJob.jar

scalac -classpath "$HADOOP_CLASSPATH" -d classes Task_1-2.scala
jar -cvf MedianVarietyJob.jar -C classes .

hadoop fs -rm -r -f /lab03/output/task1-2-temp > /dev/null 2>&1 || true
hadoop fs -rm -r -f /lab03/output/task1-2 > /dev/null 2>&1 || true

hadoop jar MedianVarietyJob.jar lab03.MedianVarietyJob \
  /lab03/input/Amazon_Sale_Report.csv \
  /lab03/output/task1-2-temp \
  /lab03/output/task1-2

mkdir -p ../../results
rm -f ../../results/Task_1-2.csv

hadoop fs -getmerge /lab03/output/task1-2 ../../results/Task_1-2.csv
sed -i '2,${/^Month,State/d}' ../../results/Task_1-2.csv

cd ../..

echo "--- BUILDING & RUNNING TASK 2-1 ---"

cd src/Task_2-1

mkdir -p classes
rm -rf classes/*
rm -f SparkTask21.jar

scalac -classpath "$HADOOP_CLASSPATH:$SPARK_CLASSPATH" -d classes Task_2-1.scala
jar -cvf SparkTask21.jar -C classes lab3

hadoop fs -rm -r -f /lab03/output/task2-1 > /dev/null 2>&1 || true

spark-submit \
  --class lab3.task21.SparkTask21 \
  --master local[*] \
  SparkTask21.jar \
  /lab03/input/Amazon_Sale_Report.csv \
  /lab03/output/task2-1

mkdir -p ../../results
rm -rf ../../results/Task_2-1.parquet
hadoop fs -get /lab03/output/task2-1 ../../results/Task_2-1.parquet

cd ../..

echo "HOÀN TẤT BUILD VÀ CHẠY TOÀN BỘ PROJECT!"
echo "Kết quả được lưu tại:"
echo "- results/Task_1-1.csv"
echo "- results/Task_1-2.csv"
echo "- results/Task_2-1.parquet"
