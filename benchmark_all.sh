#!/bin/bash
set -euo pipefail


RUNS=5

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$PROJECT_ROOT/logs"

INPUT_PATH="/lab03/input/Amazon_Sale_Report.csv"

mkdir -p "$LOG_DIR"

echo "============================================================"
echo "CHUẨN BỊ MÔI TRƯỜNG BENCHMARK"
echo "============================================================"

if [[ -n "${SPARK_HOME:-}" && -x "$SPARK_HOME/bin/spark-submit" ]]; then
  echo "Sử dụng SPARK_HOME từ môi trường: $SPARK_HOME"

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

SPARK_INPUT_PATH="${HDFS_URI}/lab03/input/Amazon_Sale_Report.csv"
SPARK_TASK21_OUTPUT_PATH="${HDFS_URI}/lab03/output/Task_2-1.parquet"

echo "SPARK_HOME=$SPARK_HOME"
echo "HDFS_URI=$HDFS_URI"
echo "Spark input path: $SPARK_INPUT_PATH"
echo "Spark Task 2-1 output path: $SPARK_TASK21_OUTPUT_PATH"

export HADOOP_CLASSPATH=$(hadoop classpath):/usr/share/scala/lib/scala-library.jar
export SPARK_CLASSPATH=$(find "$SPARK_HOME/jars" -name "*.jar" | tr '\n' ':')


echo "============================================================"
echo "CHUẨN BỊ DỮ LIỆU INPUT TRÊN HDFS"
echo "============================================================"

hadoop fs -mkdir -p /lab03/input/
hadoop fs -put -f "$PROJECT_ROOT/data/Amazon_Sale_Report.csv" /lab03/input/

write_json_log() {
  local task_name="$1"
  local main_class="$2"
  local framework="$3"
  local input_path="$4"
  local output_path="$5"
  local tmp_file="$6"
  local log_file="$7"

  python3 - <<PY
import json
import statistics
from datetime import datetime

task_name = "$task_name"
main_class = "$main_class"
framework = "$framework"
input_path = "$input_path"
output_path = "$output_path"
tmp_file = "$tmp_file"
log_file = "$log_file"

runs = []

with open(tmp_file, "r", encoding="utf-8") as f:
    for idx, line in enumerate(f, start=1):
        runtime = float(line.strip())
        runs.append({
            "run": idx,
            "runtime_seconds": runtime
        })

values = [item["runtime_seconds"] for item in runs]

result = {
    "task": task_name,
    "framework": framework,
    "benchmark_type": "job_runtime",
    "description": "Benchmark only measures the execution time of the submitted job. Compilation, JAR packaging, HDFS input preparation, result copying, and local post-processing are excluded.",
    "input_path": input_path,
    "output_path": output_path,
    "main_class": main_class,
    "number_of_runs": len(runs),
    "runs": runs,
    "statistics": {
        "mean_seconds": round(statistics.mean(values), 6),
        "stddev_seconds": round(statistics.stdev(values), 6) if len(values) >= 2 else 0.0,
        "min_seconds": round(min(values), 6),
        "max_seconds": round(max(values), 6)
    },
    "created_at": datetime.now().isoformat(timespec="seconds")
}

with open(log_file, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2, ensure_ascii=False)

print(json.dumps(result, indent=2, ensure_ascii=False))
PY
}


benchmark_task_1_1() {
  local TASK_NAME="Task_1-1"
  local SRC_DIR="$PROJECT_ROOT/src/Task_1-1"
  local JAR_NAME="SlidingWindowJob.jar"
  local MAIN_CLASS="lab03.SlidingWindowJob"
  local OUTPUT_PATH="/lab03/output/task1-1"
  local TMP_FILE="$LOG_DIR/task1_1_times.tmp"
  local LOG_FILE="$LOG_DIR/Task_1-1.json"

  echo "============================================================"
  echo "BENCHMARK $TASK_NAME"
  echo "============================================================"

  echo "--- Đang biên dịch và đóng gói $TASK_NAME ---"

  cd "$SRC_DIR"

  mkdir -p classes
  rm -rf classes/*
  rm -f "$JAR_NAME"

  scalac -classpath "$HADOOP_CLASSPATH" -d classes Task_1-1.scala
  jar -cvf "$JAR_NAME" -C classes . > /dev/null

  cd "$PROJECT_ROOT"

  rm -f "$TMP_FILE"

  for i in $(seq 1 "$RUNS")
  do
    echo "--- $TASK_NAME | Lần chạy $i/$RUNS ---"

    hadoop fs -rm -r -f "$OUTPUT_PATH" > /dev/null 2>&1 || true

    START_NS=$(date +%s%N)

    hadoop jar "$SRC_DIR/$JAR_NAME" "$MAIN_CLASS" \
      "$INPUT_PATH" \
      "$OUTPUT_PATH"

    END_NS=$(date +%s%N)

    RUNTIME=$(python3 - <<PY
start_ns = int("$START_NS")
end_ns = int("$END_NS")
print(round((end_ns - start_ns) / 1_000_000_000, 6))
PY
)

    echo "$RUNTIME" >> "$TMP_FILE"
    echo "$TASK_NAME | Lần chạy $i: $RUNTIME giây"
  done

  write_json_log \
    "$TASK_NAME" \
    "$MAIN_CLASS" \
    "Hadoop MapReduce" \
    "$INPUT_PATH" \
    "$OUTPUT_PATH" \
    "$TMP_FILE" \
    "$LOG_FILE"

  rm -f "$TMP_FILE"

  echo "Đã lưu log benchmark $TASK_NAME tại: $LOG_FILE"
}

benchmark_task_1_2() {
  local TASK_NAME="Task_1-2"
  local SRC_DIR="$PROJECT_ROOT/src/Task_1-2"
  local JAR_NAME="MedianVarietyJob.jar"
  local MAIN_CLASS="lab03.MedianVarietyJob"
  local TEMP_PATH="/lab03/output/task1-2-temp"
  local OUTPUT_PATH="/lab03/output/task1-2"
  local TMP_FILE="$LOG_DIR/task1_2_times.tmp"
  local LOG_FILE="$LOG_DIR/Task_1-2.json"

  echo "============================================================"
  echo "BENCHMARK $TASK_NAME"
  echo "============================================================"

  echo "--- Đang biên dịch và đóng gói $TASK_NAME ---"

  cd "$SRC_DIR"

  mkdir -p classes
  rm -rf classes/*
  rm -f "$JAR_NAME"

  scalac -classpath "$HADOOP_CLASSPATH" -d classes Task_1-2.scala
  jar -cvf "$JAR_NAME" -C classes . > /dev/null

  cd "$PROJECT_ROOT"

  rm -f "$TMP_FILE"

  for i in $(seq 1 "$RUNS")
  do
    echo "--- $TASK_NAME | Lần chạy $i/$RUNS ---"

    hadoop fs -rm -r -f "$TEMP_PATH" > /dev/null 2>&1 || true
    hadoop fs -rm -r -f "$OUTPUT_PATH" > /dev/null 2>&1 || true

    START_NS=$(date +%s%N)

    hadoop jar "$SRC_DIR/$JAR_NAME" "$MAIN_CLASS" \
      "$INPUT_PATH" \
      "$TEMP_PATH" \
      "$OUTPUT_PATH"

    END_NS=$(date +%s%N)

    RUNTIME=$(python3 - <<PY
start_ns = int("$START_NS")
end_ns = int("$END_NS")
print(round((end_ns - start_ns) / 1_000_000_000, 6))
PY
)

    echo "$RUNTIME" >> "$TMP_FILE"
    echo "$TASK_NAME | Lần chạy $i: $RUNTIME giây"
  done

  write_json_log \
    "$TASK_NAME" \
    "$MAIN_CLASS" \
    "Hadoop MapReduce" \
    "$INPUT_PATH" \
    "$OUTPUT_PATH" \
    "$TMP_FILE" \
    "$LOG_FILE"

  rm -f "$TMP_FILE"

  echo "Đã lưu log benchmark $TASK_NAME tại: $LOG_FILE"
}

benchmark_task_2_1() {
  local TASK_NAME="Task_2-1"
  local SRC_DIR="$PROJECT_ROOT/src/Task_2-1"
  local JAR_NAME="SparkTask21.jar"
  local MAIN_CLASS="lab3.task21.SparkTask21"
  local OUTPUT_PATH="/lab03/output/Task_2-1.parquet"
  local TMP_FILE="$LOG_DIR/task2_1_times.tmp"
  local LOG_FILE="$LOG_DIR/Task_2-1.json"

  echo "============================================================"
  echo "BENCHMARK $TASK_NAME"
  echo "============================================================"

  echo "--- Đang biên dịch và đóng gói $TASK_NAME ---"

  cd "$SRC_DIR"

  mkdir -p classes
  rm -rf classes/*
  rm -f "$JAR_NAME"

  scalac -classpath "$HADOOP_CLASSPATH:$SPARK_CLASSPATH" -d classes Task_2-1.scala
  jar -cvf "$JAR_NAME" -C classes lab3 > /dev/null

  cd "$PROJECT_ROOT"

  rm -f "$TMP_FILE"

  for i in $(seq 1 "$RUNS")
  do
    echo "--- $TASK_NAME | Lần chạy $i/$RUNS ---"

    hadoop fs -rm -r -f "$OUTPUT_PATH" > /dev/null 2>&1 || true
    hadoop fs -rm -r -f "${OUTPUT_PATH}_staging" > /dev/null 2>&1 || true

    START_NS=$(date +%s%N)

    spark-submit \
      --class "$MAIN_CLASS" \
      --master local[*] \
      "$SRC_DIR/$JAR_NAME" \
      "$SPARK_INPUT_PATH" \
      "$SPARK_TASK21_OUTPUT_PATH"

    END_NS=$(date +%s%N)

    RUNTIME=$(python3 - <<PY
start_ns = int("$START_NS")
end_ns = int("$END_NS")
print(round((end_ns - start_ns) / 1_000_000_000, 6))
PY
)

    echo "$RUNTIME" >> "$TMP_FILE"
    echo "$TASK_NAME | Lần chạy $i: $RUNTIME giây"
  done

  write_json_log \
    "$TASK_NAME" \
    "$MAIN_CLASS" \
    "Apache Spark" \
    "$SPARK_INPUT_PATH" \
    "$SPARK_TASK21_OUTPUT_PATH" \
    "$TMP_FILE" \
    "$LOG_FILE"

  rm -f "$TMP_FILE"

  echo "Đã lưu log benchmark $TASK_NAME tại: $LOG_FILE"
}

echo "============================================================"
echo "LAB 03 - BENCHMARK TOÀN BỘ TASK HIỆN CÓ"
echo "Số lần chạy mỗi task: $RUNS"
echo "Thư mục lưu log: $LOG_DIR"
echo "============================================================"

benchmark_task_1_1
benchmark_task_1_2
benchmark_task_2_1

echo "============================================================"
echo "HOÀN TẤT BENCHMARK"
echo "Các file log được tạo:"
echo "- logs/Task_1-1.json"
echo "- logs/Task_1-2.json"
echo "- logs/Task_2-1.json"
echo "============================================================"
