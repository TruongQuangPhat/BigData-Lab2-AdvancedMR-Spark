# LAB 03 - ADVANCED MAPREDUCE PROBLEMS

## Hướng dẫn Build, Chạy và Benchmark

README này hướng dẫn cách chuẩn bị môi trường, chạy hai bài MapReduce hiện tại của Lab 03 và đo thời gian thực thi phục vụ phần benchmark trong báo cáo.

Phạm vi hiện tại:

- `Task_1-1`: Sliding Window
- `Task_1-2`: Median Variety

Các task Spark còn lại có thể được bổ sung vào script và README sau khi nhóm hoàn thiện phần triển khai tương ứng.

## YÊU CẦU HỆ THỐNG

- Hadoop 3.x ở chế độ pseudo-distributed mode
- Scala 2.12.x hoặc 2.13.x
- Java 8 hoặc Java 11
- Môi trường Linux hoặc WSL
- Python 3 để xử lý thống kê benchmark và ghi log JSON

## CẤU TRÚC THƯ MỤC QUAN TRỌNG

```text
BigData-Lab03-AdvancedMR-Spark/
├── data/
│   └── Amazon_Sale_Report.csv
├── results/
│   ├── Task_1-1.csv
│   └── Task_1-2.csv
├── logs/
│   ├── Task_1-1.json
│   └── Task_1-2.json
├── src/
│   ├── Task_1-1/
│   │   └── task1_1.scala
│   └── Task_1-2/
│       └── task1_2.scala
├── build_and_run_all.sh
├── benchmark_all.sh
└── README.md
```

Trong đó:

- `data/` chứa dữ liệu đầu vào local.
- `src/` chứa mã nguồn Scala.
- `results/` chứa các file kết quả cuối cùng dùng để nộp bài.
- `logs/` chứa log benchmark phục vụ báo cáo.
- `build_and_run_all.sh` dùng để build và chạy các task MapReduce.
- `benchmark_all.sh` dùng để đo thời gian thực thi của các task MapReduce.

## CHUẨN BỊ MÔI TRƯỜNG VÀ DỮ LIỆU TRÊN HDFS

Tất cả các lệnh dưới đây được thực thi tại thư mục gốc của dự án.

Trước tiên, di chuyển vào project root:

```bash
cd ~/AI_Project/BigData-Lab03-AdvancedMR-Spark
```

### 1. Khởi động Hadoop

```bash
start-dfs.sh
start-yarn.sh
```

### 2. Kiểm tra các tiến trình Hadoop

```bash
jps
```

Cần bảo đảm các tiến trình chính đã chạy, ví dụ:

```text
NameNode
DataNode
ResourceManager
NodeManager
```

### 3. Tạo thư mục input trên HDFS và tải dữ liệu lên

```bash
hadoop fs -mkdir -p /lab03/input/
hadoop fs -put -f data/Amazon_Sale_Report.csv /lab03/input/
```

Kiểm tra dữ liệu trên HDFS:

```bash
hadoop fs -ls /lab03/input/
```

### 4. Tạo thư mục output local

```bash
mkdir -p results logs
```

## TASK 1-1: SLIDING WINDOW

### Cấu trúc source

- File source: `src/Task_1-1/task1_1.scala`
- Package: `lab03`
- Main class: `lab03.SlidingWindowJob`
- Input HDFS: `/lab03/input/Amazon_Sale_Report.csv`
- Output HDFS: `/lab03/output/task1-1`
- Output local cuối cùng: `results/Task_1-1.csv`

### 1. Di chuyển vào thư mục mã nguồn

```bash
cd src/Task_1-1
```

### 2. Thiết lập classpath Hadoop và Scala

```bash
export HADOOP_CLASSPATH=$(hadoop classpath):/usr/share/scala/lib/scala-library.jar
```

### 3. Biên dịch và đóng gói JAR

```bash
mkdir -p classes
rm -rf classes/*
rm -f SlidingWindowJob.jar

scalac -classpath "$HADOOP_CLASSPATH" -d classes task1_1.scala
jar -cvf SlidingWindowJob.jar -C classes .
```

### 4. Chạy Hadoop MapReduce job

```bash
hadoop jar SlidingWindowJob.jar lab03.SlidingWindowJob \
  /lab03/input/Amazon_Sale_Report.csv \
  /lab03/output/task1-1
```

### 5. Lấy kết quả từ HDFS về thư mục `results/`

Hadoop MapReduce ghi output ra thư mục HDFS, thường gồm các file `part-r-*`. Vì yêu cầu nộp là một file `.csv` duy nhất, cần dùng `getmerge` để gộp kết quả về local.

```bash
mkdir -p ../../results
rm -f ../../results/Task_1-1.csv

hadoop fs -getmerge /lab03/output/task1-1 ../../results/Task_1-1.csv
sed -i '2,${/^State,TargetDate/d}' ../../results/Task_1-1.csv
```

### 6. Quay lại thư mục gốc

```bash
cd ../..
```

## TASK 1-2: MEDIAN VARIETY

### Cấu trúc source

- File source: `src/Task_1-2/task1_2.scala`
- Package: `lab03`
- Main class: `lab03.MedianVarietyJob`
- Input HDFS: `/lab03/input/Amazon_Sale_Report.csv`
- Temp output HDFS: `/lab03/output/task1-2-temp`
- Final output HDFS: `/lab03/output/task1-2`
- Output local cuối cùng: `results/Task_1-2.csv`

Task này sử dụng kỹ thuật job chaining, gồm hai MapReduce jobs chạy nối tiếp nhau.

### 1. Di chuyển vào thư mục mã nguồn

```bash
cd src/Task_1-2
```

### 2. Thiết lập classpath Hadoop và Scala

```bash
export HADOOP_CLASSPATH=$(hadoop classpath):/usr/share/scala/lib/scala-library.jar
```

### 3. Biên dịch và đóng gói JAR

```bash
mkdir -p classes
rm -rf classes/*
rm -f MedianVarietyJob.jar

scalac -classpath "$HADOOP_CLASSPATH" -d classes task1_2.scala
jar -cvf MedianVarietyJob.jar -C classes .
```

### 4. Chạy Hadoop MapReduce job

Task 1-2 cần truyền đủ ba tham số:

1. Input path trên HDFS
2. Temp output path cho Job 1
3. Final output path cho Job 2

```bash
hadoop jar MedianVarietyJob.jar lab03.MedianVarietyJob \
  /lab03/input/Amazon_Sale_Report.csv \
  /lab03/output/task1-2-temp \
  /lab03/output/task1-2
```

### 5. Lấy kết quả từ HDFS về thư mục `results/`

```bash
mkdir -p ../../results
rm -f ../../results/Task_1-2.csv

hadoop fs -getmerge /lab03/output/task1-2 ../../results/Task_1-2.csv
sed -i '2,${/^Month,State/d}' ../../results/Task_1-2.csv
```

### 6. Quay lại thư mục gốc

```bash
cd ../..
```

---

## TASK 2-2: POPULATION STANDARD DEVIATION WITH DYNAMIC PERCENTILES (SPARK)

### Cấu trúc package: `Task22`

1. Di chuyển vào thư mục mã nguồn:
```bash
cd src/Task_2-2
```

2. Cấu hình classpath và biên dịch:
```bash
export SPARK_CLASSPATH=$(find $SPARK_HOME/jars -name "*.jar" | tr '\n' ':')
mkdir -p classes && rm -rf classes/* && rm -f Task_2_2.jar
scalac -classpath "$SPARK_CLASSPATH" -d classes task_2_2.scala
jar -cvf Task_2_2.jar -C classes .
```

3. Submit Job lên Spark và xuất Log:
```bash
spark-submit --class Task22 --master local[*] Task_2_2.jar 2>&1 | tee task_2-2_stats.log
```

4. Lấy kết quả Parquet về máy:
```bash
rm -f Task_2-2.parquet
hadoop fs -get /lab2/output/Task_2-2.parquet ./Task_2-2.parquet
cd ../..
```

---

## KIỂM TRA ĐỊNH DẠNG KẾT QUẢ

Các file kết quả sẽ được lưu tại `src/Task_1-1/Task_1-1.csv`, `src/Task_1-2/Task_1-2.csv` và `Task_2-2.parquet`.
## KIỂM TRA ĐỊNH DẠNG KẾT QUẢ

Các file kết quả cuối cùng được lưu tại:

```text
results/Task_1-1.csv
results/Task_1-2.csv
```

Kiểm tra nhanh:

```bash
tree results
head results/Task_1-1.csv
head results/Task_1-2.csv
```

### Task 1-1 Output Format

```csv
State,TargetDate,MostBoughtSize,Count
ANDAMAN & NICOBAR,04-02-22,M,1
ANDHRA PRADESH,04-02-22,M,41
...
```

### Task 1-2 Output Format

```csv
Month,State,MedianVariety
2022-03,ANDHRA PRADESH,1.0
2022-04,MAHARASHTRA,3.5
...
```

### Task 2-2 Output Format (`Task_2-2.parquet`):
Định dạng file xuất ra là **Parquet** dạng Wide format, bao gồm 15 cột:
`SKU, Month, total_orders, threshold_p80_approx, threshold_p90_approx, orders_p80_approx, orders_p90_approx, stddev_p80_approx, stddev_p90_approx, threshold_p80_exact, threshold_p90_exact, orders_p80_exact, orders_p90_exact, stddev_p80_exact, stddev_p90_exact`

---
## QUY TRÌNH TỰ ĐỘNG HÓA: BUILD & RUN ALL

Dự án cung cấp script `build_and_run_all.sh` để tự động hóa quy trình chạy hai bài MapReduce hiện tại.

Script này thực hiện các bước:

1. Chuẩn bị dữ liệu input trên HDFS.
2. Biên dịch mã nguồn Scala.
3. Đóng gói file JAR.
4. Chạy Hadoop MapReduce job cho `Task_1-1` và `Task_1-2`.
5. Trích xuất kết quả cuối cùng từ HDFS về thư mục `results/`.

Các file kết quả sau khi chạy script:

```text
results/Task_1-1.csv
results/Task_1-2.csv
```

### 1. Cấp quyền thực thi

Chỉ cần thực hiện một lần:

```bash
chmod +x build_and_run_all.sh
```

### 2. Chạy script

```bash
./build_and_run_all.sh
```

### 3. Kiểm tra kết quả

```bash
#!/bin/bash

echo "--- PREPARING HDFS ---"

hadoop fs -mkdir -p /lab2/input/

hadoop fs -test -e /lab2/input/Amazon_Sale_Report.csv || hadoop fs -put data/Amazon_Sale_Report.csv /lab2/input/

export HADOOP_CLASSPATH=$(hadoop classpath):/usr/share/scala/lib/scala-library.jar

echo "--- BUILDING & RUNNING TASK 1-1 ---"

cd src/Task_1-1
mkdir -p classes && rm -rf classes/* && rm -f SlidingWindowJob.jar
scalac -classpath "$HADOOP_CLASSPATH" -d classes SlidingWindowJob.scala
jar -cvf SlidingWindowJob.jar -C classes .
hadoop jar SlidingWindowJob.jar lab2.task11.SlidingWindowJob /lab2/input/Amazon_Sale_Report.csv /lab2/output/task1-1
rm -f Task_1-1.csv
hadoop fs -getmerge /lab2/output/task1-1 Task_1-1.csv
sed -i '2,${/^State,TargetDate/d}' Task_1-1.csv
cd ../..

echo "--- BUILDING & RUNNING TASK 1-2 ---"

cd src/Task_1-2
mkdir -p classes && rm -rf classes/* && rm -f MedianVarietyJob.jar
scalac -classpath "$HADOOP_CLASSPATH" -d classes MedianVarietyJob.scala
jar -cvf MedianVarietyJob.jar -C classes .
hadoop jar MedianVarietyJob.jar lab2.task12.MedianVarietyJob /lab2/input/Amazon_Sale_Report.csv /lab2/output/task1-2-temp /lab2/output/task1-2
rm -f Task_1-2.csv
hadoop fs -getmerge /lab2/output/task1-2 Task_1-2.csv
sed -i '2,${/^Month,State/d}' Task_1-2.csv
echo "--- BUILDING & RUNNING TASK 2-2 (SPARK) ---"

cd src/Task_2-2
mkdir -p classes && rm -rf classes/* && rm -f Task_2_2.jar
export SPARK_CLASSPATH=$(find $SPARK_HOME/jars -name "*.jar" | tr '\n' ':')
scalac -classpath "$SPARK_CLASSPATH" -d classes task_2_2.scala
jar -cvf Task_2_2.jar -C classes .

spark-submit \
  --class Task22 \
  --master local[*] \
  Task_2_2.jar 2>&1 | tee task_2-2_stats.log

rm -f Task_2-2.parquet
hadoop fs -get /lab2/output/Task_2-2.parquet ./Task_2-2.parquet
cd ../..

echo "HOÀN TẤT BUILD VÀ CHẠY TOÀN BỘ PROJECT!"
tree results
head results/Task_1-1.csv
head results/Task_1-2.csv
```

## QUY TRÌNH BENCHMARK

Dự án cung cấp script `benchmark_all.sh` để đo thời gian thực thi cho các bài MapReduce.

Ở phiên bản hiện tại, script benchmark hỗ trợ:

1. `Task_1-1`: Sliding Window
2. `Task_1-2`: Median Variety

Mỗi task được chạy 5 lần. Kết quả benchmark được lưu thành hai file log JSON riêng biệt:

```text
logs/Task_1-1.json
logs/Task_1-2.json
```

Các file log chứa:

- thời gian chạy từng lần;
- thời gian trung bình;
- độ lệch chuẩn;
- thời gian nhỏ nhất;
- thời gian lớn nhất;
- thông tin main class và output path tương ứng.

Benchmark chỉ đo thời gian thực thi Hadoop MapReduce job chính. Các bước biên dịch Scala, đóng gói JAR, chuẩn bị input HDFS, `getmerge` kết quả về local và xử lý hậu kỳ file CSV không được tính vào thời gian benchmark.

### 1. Cấp quyền thực thi

Chỉ cần thực hiện một lần:

```bash
chmod +x benchmark_all.sh
```

### 2. Chạy benchmark

```bash
./benchmark_all.sh
```

### 3. Kiểm tra file log benchmark

```bash
tree logs
cat logs/Task_1-1.json
cat logs/Task_1-2.json
```
