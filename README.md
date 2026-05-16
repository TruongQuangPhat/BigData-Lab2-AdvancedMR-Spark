# LAB 03 - ADVANCED MAPREDUCE PROBLEMS

## Hướng dẫn Build và Chạy

### YÊU CẦU HỆ THỐNG

- Hadoop 3.x (Pseudo-distributed mode)
- Scala 2.12.x hoặc 2.13.x
- Java 8 hoặc 11
- Môi trường Linux / WSL (Windows Subsystem for Linux)

## CHUẨN BỊ MÔI TRƯỜNG VÀ DỮ LIỆU TRÊN HDFS

Tất cả các lệnh dưới đây được thực thi tại thư mục gốc của dự án.

Đảm bảo bạn đang đứng tại thư mục gốc trước khi bắt đầu:

```bash
cd ~/AI_Project/BigData-Lab03-AdvancedMR-Spark
```

### 1. Khởi động Hadoop

```bash
start-dfs.sh
start-yarn.sh
```

### 2. Kiểm tra các tiến trình Hadoop

Đảm bảo có các tiến trình như `NameNode`, `DataNode`, `NodeManager`, `ResourceManager`:

```bash
jps
```

### 3. Tạo thư mục input trên HDFS và tải dữ liệu lên

```bash
hadoop fs -mkdir -p /lab03/input/
hadoop fs -put -f data/Amazon_Sale_Report.csv /lab03/input/
```

### 4. Tạo thư mục kết quả local

Các file CSV cuối cùng sẽ được lưu trong thư mục `results/` tại project root:

```bash
mkdir -p results
```

## TASK 1-1: SLIDING WINDOW

### Cấu trúc source

- File source: `src/Task_1-1/task1_1.scala`
- Package: `lab03`
- Main class: `lab03.SlidingWindowJob`
- Output local cuối cùng: `results/Task_1-1.csv`

### 1. Di chuyển vào thư mục mã nguồn và thiết lập môi trường

```bash
cd src/Task_1-1
export HADOOP_CLASSPATH=$(hadoop classpath):/usr/share/scala/lib/scala-library.jar
```

### 2. Biên dịch và đóng gói thành file JAR

```bash
mkdir -p classes
rm -rf classes/*
rm -f SlidingWindowJob.jar

scalac -classpath "$HADOOP_CLASSPATH" -d classes task1_1.scala
jar -cvf SlidingWindowJob.jar -C classes .
```

### 3. Chạy Job trên Hadoop

```bash
hadoop jar SlidingWindowJob.jar lab03.SlidingWindowJob \
  /lab03/input/Amazon_Sale_Report.csv \
  /lab03/output/task1-1
```

### 4. Lấy kết quả từ HDFS về thư mục `results/`

Hadoop MapReduce ghi output ra thư mục HDFS, thường gồm các file `part-r-*`. Vì yêu cầu nộp là một file `.csv` duy nhất, ta dùng `getmerge` để gộp kết quả về local.

```bash
mkdir -p ../../results
rm -f ../../results/Task_1-1.csv

hadoop fs -getmerge /lab03/output/task1-1 ../../results/Task_1-1.csv
sed -i '2,${/^State,TargetDate/d}' ../../results/Task_1-1.csv
```

### 5. Quay lại thư mục gốc

```bash
cd ../..
```

## TASK 1-2: MEDIAN VARIETY

### Cấu trúc source

- File source: `src/Task_1-2/task1_2.scala`
- Package: `lab03`
- Main class: `lab03.MedianVarietyJob`
- Output local cuối cùng: `results/Task_1-2.csv`

Lưu ý: Task này sử dụng kỹ thuật Job Chaining, gồm 2 MapReduce jobs chạy nối tiếp nhau.

### 1. Di chuyển vào thư mục mã nguồn và thiết lập môi trường

```bash
cd src/Task_1-2
export HADOOP_CLASSPATH=$(hadoop classpath):/usr/share/scala/lib/scala-library.jar
```

### 2. Biên dịch và đóng gói thành file JAR

```bash
mkdir -p classes
rm -rf classes/*
rm -f MedianVarietyJob.jar

scalac -classpath "$HADOOP_CLASSPATH" -d classes task1_2.scala
jar -cvf MedianVarietyJob.jar -C classes .
```

### 3. Chạy Job trên Hadoop

Task 1-2 cần truyền đủ 3 tham số:

1. Input path trên HDFS
2. Temp output path cho Job 1
3. Final output path cho Job 2

```bash
hadoop jar MedianVarietyJob.jar lab03.MedianVarietyJob \
  /lab03/input/Amazon_Sale_Report.csv \
  /lab03/output/task1-2-temp \
  /lab03/output/task1-2
```

### 4. Lấy kết quả từ HDFS về thư mục `results/`

```bash
mkdir -p ../../results
rm -f ../../results/Task_1-2.csv

hadoop fs -getmerge /lab03/output/task1-2 ../../results/Task_1-2.csv
sed -i '2,${/^Month,State/d}' ../../results/Task_1-2.csv
```

### 5. Quay lại thư mục gốc

```bash
cd ../..
```

## KIỂM TRA ĐỊNH DẠNG KẾT QUẢ

Các file kết quả cuối cùng được lưu tại:

```text
results/Task_1-1.csv
results/Task_1-2.csv
```

### Task 1-1 Output Format: `Task_1-1.csv`

```csv
State,TargetDate,MostBoughtSize,Count
ANDAMAN & NICOBAR,04-02-22,M,1
ANDHRA PRADESH,04-02-22,M,41
...
```

### Task 1-2 Output Format: `Task_1-2.csv`

```csv
Month,State,MedianVariety
2022-03,ANDHRA PRADESH,1.0
2022-04,MAHARASHTRA,3.5
...
```

## QUY TRÌNH TỰ ĐỘNG HÓA: BUILD & RUN ALL

Dự án cung cấp script shell `build_and_run_all.sh` để tự động hóa toàn bộ quy trình:

1. Chuẩn bị HDFS input
2. Biên dịch mã nguồn Scala
3. Đóng gói JAR
4. Chạy MapReduce trên Hadoop
5. Trích xuất kết quả CSV cuối cùng về thư mục `results/`

### Bước 1: Cấp quyền thực thi

Chỉ cần chạy một lần:

```bash
chmod +x build_and_run_all.sh
```

### Bước 2: Thực thi script

```bash
./build_and_run_all.sh
```

### Nội dung đề xuất cho `build_and_run_all.sh`

```bash
#!/bin/bash

set -e

echo "--- PREPARING HDFS ---"

mkdir -p results

hadoop fs -mkdir -p /lab03/input/
hadoop fs -put -f data/Amazon_Sale_Report.csv /lab03/input/

export HADOOP_CLASSPATH=$(hadoop classpath):/usr/share/scala/lib/scala-library.jar

echo "--- BUILDING & RUNNING TASK 1-1 ---"

cd src/Task_1-1

mkdir -p classes
rm -rf classes/*
rm -f SlidingWindowJob.jar

scalac -classpath "$HADOOP_CLASSPATH" -d classes task1_1.scala
jar -cvf SlidingWindowJob.jar -C classes .

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

scalac -classpath "$HADOOP_CLASSPATH" -d classes task1_2.scala
jar -cvf MedianVarietyJob.jar -C classes .

hadoop jar MedianVarietyJob.jar lab03.MedianVarietyJob \
  /lab03/input/Amazon_Sale_Report.csv \
  /lab03/output/task1-2-temp \
  /lab03/output/task1-2

mkdir -p ../../results
rm -f ../../results/Task_1-2.csv

hadoop fs -getmerge /lab03/output/task1-2 ../../results/Task_1-2.csv
sed -i '2,${/^Month,State/d}' ../../results/Task_1-2.csv

cd ../..

echo "HOÀN TẤT BUILD VÀ CHẠY TOÀN BỘ PROJECT!"
echo "Kết quả được lưu tại:"
echo "- results/Task_1-1.csv"
echo "- results/Task_1-2.csv"
```
