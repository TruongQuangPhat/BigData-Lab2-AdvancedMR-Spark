#!/bin/bash
set -euo pipefail

# ============================================================
# LAB 03 - BENCHMARK ALL
# Current scope:
#   - Task 1-1: Hadoop MapReduce - Sliding Window
#   - Task 1-2: Hadoop MapReduce - Median Variety
#   - Task 2-1: Spark - Cancelled Standard Order Qualification Percentage
# ============================================================

RUNS=5

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$PROJECT_ROOT/logs"

INPUT_PATH="/lab03/input/Amazon_Sale_Report.csv"

mkdir -p "$LOG_DIR"

: "${SPARK_HOME:=/opt/spark}"

export HADOOP_CLASSPATH=$(hadoop classpath):/usr/share/scala/lib/scala-library.jar
export SPARK_CLASSPATH=$(find "$SPARK_HOME/jars" -name "*.jar" | tr '\n' ':')

# ============================================================
# Prepare HDFS input
# ============================================================

echo "============================================================"
echo "PREPARING HDFS INPUT"
echo "============================================================"

hadoop fs -mkdir -p /lab03/input/
hadoop fs -put -f "$PROJECT_ROOT/data/Amazon_Sale_Report.csv" /lab03/input/

# ============================================================
# Utility: write benchmark result to JSON
# ============================================================

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
    "description": "Benchmark measures only the execution time of the submitted job. Compilation, JAR packaging, HDFS input preparation, result copying, and local post-processing are excluded.",
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

# ============================================================
# Benchmark Task 1-1
# ============================================================

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

  echo "--- Building $TASK_NAME ---"

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
    echo "--- $TASK_NAME | Run $i/$RUNS ---"

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
    echo "$TASK_NAME | Run $i runtime: $RUNTIME seconds"
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
}

# ============================================================
# Benchmark Task 1-2
# ============================================================

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

  echo "--- Building $TASK_NAME ---"

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
    echo "--- $TASK_NAME | Run $i/$RUNS ---"

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
    echo "$TASK_NAME | Run $i runtime: $RUNTIME seconds"
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
}

# ============================================================
# Benchmark Task 2-1
# ============================================================

benchmark_task_2_1() {
  local TASK_NAME="Task_2-1"
  local SRC_DIR="$PROJECT_ROOT/src/Task_2-1"
  local JAR_NAME="SparkTask21.jar"
  local MAIN_CLASS="lab3.task21.SparkTask21"
  local OUTPUT_PATH="/lab03/output/task2-1"
  local TMP_FILE="$LOG_DIR/task2_1_times.tmp"
  local LOG_FILE="$LOG_DIR/Task_2-1.json"

  echo "============================================================"
  echo "BENCHMARK $TASK_NAME"
  echo "============================================================"

  echo "--- Building $TASK_NAME ---"

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
    echo "--- $TASK_NAME | Run $i/$RUNS ---"

    hadoop fs -rm -r -f "$OUTPUT_PATH" > /dev/null 2>&1 || true

    START_NS=$(date +%s%N)

    spark-submit \
      --class "$MAIN_CLASS" \
      --master local[*] \
      "$SRC_DIR/$JAR_NAME" \
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
    echo "$TASK_NAME | Run $i runtime: $RUNTIME seconds"
  done

  write_json_log \
    "$TASK_NAME" \
    "$MAIN_CLASS" \
    "Apache Spark" \
    "$INPUT_PATH" \
    "$OUTPUT_PATH" \
    "$TMP_FILE" \
    "$LOG_FILE"

  rm -f "$TMP_FILE"
}

# ============================================================
# Main
# ============================================================

echo "============================================================"
echo "LAB 03 BENCHMARK ALL"
echo "Current scope: Task 1-1, Task 1-2, and Task 2-1"
echo "Number of runs per task: $RUNS"
echo "Logs directory: $LOG_DIR"
echo "============================================================"

benchmark_task_1_1
benchmark_task_1_2
benchmark_task_2_1

echo "============================================================"
echo "BENCHMARK COMPLETED"
echo "Generated logs:"
echo "- logs/Task_1-1.json"
echo "- logs/Task_1-2.json"
echo "- logs/Task_2-1.json"
echo "============================================================"
