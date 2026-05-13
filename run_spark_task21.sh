#!/bin/bash

# 0. Nạp biến môi trường (Hadoop, Spark)
export SPARK_HOME="/opt/spark"
export HADOOP_HOME="/usr/local/hadoop"
export PATH=$PATH:$SPARK_HOME/bin:$HADOOP_HOME/bin

# 1. Cấu hình đường dẫn
INPUT_CSV="/app/data.csv"
OUTPUT_DIR="/app/output_task21"
JAR_NAME="SparkTask21.jar"
MAIN_CLASS="lab2.task21.SparkTask21"

# Tự động tìm file Scala (ưu tiên trong src, nếu không thấy thì tìm ở root)
SCALA_FILE="src/Task_2-1/SparkTask21.scala"
if [ ! -f "$SCALA_FILE" ]; then
    SCALA_FILE="/app/SparkTask21.scala"
fi

echo "=== [1/3] Khởi tạo môi trường ==="
echo "log4j.rootCategory=ERROR, console" > log4j.properties
echo "log4j.appender.console=org.apache.log4j.ConsoleAppender" >> log4j.properties
echo "log4j.appender.console.layout=org.apache.log4j.PatternLayout" >> log4j.properties
echo "log4j.appender.console.layout.ConversionPattern=%m%n" >> log4j.properties

echo "=== [2/3] Biên dịch mã nguồn Scala ($SCALA_FILE) ==="
rm -rf lab2 classes $JAR_NAME
mkdir -p classes

# Lấy classpath (nếu lệnh hadoop fail thì dùng đường dẫn mặc định)
H_CLASSPATH=$(hadoop classpath 2>/dev/null || echo "/usr/local/hadoop/etc/hadoop:/usr/local/hadoop/share/hadoop/common/lib/*:/usr/local/hadoop/share/hadoop/common/*")

scalac -classpath "$H_CLASSPATH:$SPARK_HOME/jars/*" -d classes "$SCALA_FILE"
if [ $? -ne 0 ]; then echo "Lỗi biên dịch!"; exit 1; fi
jar -cvf $JAR_NAME -C classes lab2

echo "=== [3/3] Thực thi Spark Job (Xuất Parquet) ==="
rm -rf $OUTPUT_DIR
spark-submit \
    --class $MAIN_CLASS \
    --master "local[*]" \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
    $JAR_NAME $INPUT_CSV $OUTPUT_DIR

echo "=== HOÀN THÀNH ==="
echo "Kết quả Parquet đã được lưu tại: $OUTPUT_DIR"