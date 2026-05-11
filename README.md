# LAB 2 - ADVANCED MAPREDUCE PROBLEMS
## Hướng dẫn Build và Chạy

### YÊU CẦU HỆ THỐNG
- Hadoop 3.x (pseudo-distributed mode)
- Scala 2.12.x
- SBT (Scala Build Tool) hoặc có thể compile thủ công
- Java 8 hoặc 11

---

## TASK 1-1: SLIDING WINDOW

### Cách 1: Build bằng SBT (Khuyến nghị)

```bash
cd Task_1-1

# Tạo thư mục project nếu chưa có
mkdir -p project

# Tạo file plugins.sbt
echo 'addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")' > project/plugins.sbt

# Build JAR file
sbt clean assembly

# File JAR sẽ được tạo tại: target/scala-2.12/SlidingWindowJob-assembly-1.0.jar
```

### Cách 2: Compile thủ công bằng scalac

```bash
cd Task_1-1

# Tìm classpath của Hadoop
export HADOOP_CLASSPATH=$(hadoop classpath)

# Compile
scalac -classpath $HADOOP_CLASSPATH -d . SlidingWindowJob.scala

# Tạo JAR
jar -cvf SlidingWindowJob.jar lab2/

# Hoặc sử dụng Maven để package
```

### Chạy trên Hadoop

```bash
# Upload dữ liệu lên HDFS (nếu chưa có)
hadoop fs -mkdir -p /user/$(whoami)/lab2/input
hadoop fs -put Amazon_Sale_Report.csv /user/$(whoami)/lab2/input/

# Chạy job
hadoop jar Task_1-1/target/scala-2.12/SlidingWindowJob-assembly-1.0.jar \
  lab2.task11.SlidingWindowJob \
  /user/$(whoami)/lab2/input/Amazon_Sale_Report.csv \
  /user/$(whoami)/lab2/output/task1-1

# Xem kết quả
hadoop fs -cat /user/$(whoami)/lab2/output/task1-1/part-* | head -20

# Tải về máy local
hadoop fs -getmerge /user/$(whoami)/lab2/output/task1-1 Task_1-1.csv

# Xóa header nếu có nhiều file part (giữ lại header đầu tiên)
# sed -i '2,${/^State,TargetDate,MostBoughtSize,Count$/d}' Task_1-1.csv
```

---

## TASK 1-2: MEDIAN VARIETY

### Build bằng SBT

```bash
cd Task_1-2

# Tạo thư mục project
mkdir -p project
echo 'addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")' > project/plugins.sbt

# Build
sbt clean assembly

# File JAR: target/scala-2.12/MedianVarietyJob-assembly-1.0.jar
```

### Chạy trên Hadoop (Job Chaining)

```bash
# Chạy job (bao gồm cả Job 1 và Job 2)
hadoop jar Task_1-2/target/scala-2.12/MedianVarietyJob-assembly-1.0.jar \
  lab2.task12.MedianVarietyJob \
  /user/$(whoami)/lab2/input/Amazon_Sale_Report.csv \
  /user/$(whoami)/lab2/temp/task1-2 \
  /user/$(whoami)/lab2/output/task1-2

# Xem kết quả
hadoop fs -cat /user/$(whoami)/lab2/output/task1-2/part-* | head -20

# Tải về
hadoop fs -getmerge /user/$(whoami)/lab2/output/task1-2 Task_1-2.csv
```

---

## GIẢI THÍCH THAM SỐ

### Task 1-1: SlidingWindowJob
```
<input path>   : Đường dẫn file CSV trên HDFS
<output path>  : Đường dẫn output trên HDFS
```

### Task 1-2: MedianVarietyJob
```
<input path>   : Đường dẫn file CSV trên HDFS
<temp path>    : Đường dẫn tạm cho output Job 1
<output path>  : Đường dẫn output cuối cùng (Job 2)
```

---

## XỬ LÝ LỖI THƯỜNG GẶP

### 1. Lỗi "ClassNotFoundException"
- Kiểm tra lại JAR file đã build đúng chưa
- Đảm bảo package name đúng: `lab2.task11` hoặc `lab2.task12`

### 2. Lỗi "Permission denied" trên HDFS
```bash
# Tạo thư mục và set quyền
hadoop fs -mkdir -p /user/$(whoami)/lab2
hadoop fs -chmod -R 755 /user/$(whoami)/lab2
```

### 3. Output directory already exists
```bash
# Xóa output cũ
hadoop fs -rm -r /user/$(whoami)/lab2/output/task1-1
hadoop fs -rm -r /user/$(whoami)/lab2/output/task1-2
hadoop fs -rm -r /user/$(whoami)/lab2/temp/task1-2
```

### 4. File CSV có nhiều part-* files
```bash
# Merge tất cả parts thành 1 file duy nhất
hadoop fs -getmerge /user/$(whoami)/lab2/output/task1-1 Task_1-1.csv

# Nếu có nhiều headers, xóa các header thừa (giữ lại dòng đầu)
awk 'NR==1 || !/^State,TargetDate/' Task_1-1.csv > Task_1-1_clean.csv
mv Task_1-1_clean.csv Task_1-1.csv
```

---

## KIỂM TRA KẾT QUẢ

### Task 1-1 Output Format:
```
State,TargetDate,MostBoughtSize,Count
MAHARASHTRA,05-01-22,M,245
KARNATAKA,05-01-22,L,189
...
```

### Task 1-2 Output Format:
```
Month,State,MedianVariety
2022-04,MAHARASHTRA,3.5
2022-04,KARNATAKA,4.0
...
```

---

## LƯU Ý QUAN TRỌNG

1. **Đảm bảo Hadoop đã chạy**:
```bash
# Kiểm tra Hadoop services
jps
# Phải thấy: NameNode, DataNode, ResourceManager, NodeManager
```

2. **Kiểm tra file input**:
```bash
hadoop fs -ls /user/$(whoami)/lab2/input/
hadoop fs -cat /user/$(whoami)/lab2/input/Amazon_Sale_Report.csv | head -5
```

3. **Monitor job progress**:
- Truy cập Web UI: http://localhost:8088 (YARN ResourceManager)
- Hoặc: http://localhost:9870 (HDFS NameNode)

4. **Debug**:
```bash
# Xem logs
yarn logs -applicationId <application_id>

# Hoặc xem trực tiếp từ container logs
```

---

## BUILD NHANH (TẤT CẢ TASKS)

```bash
#!/bin/bash

# Build Task 1-1
cd Task_1-1
mkdir -p project
echo 'addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")' > project/plugins.sbt
sbt clean assembly
cd ..

# Build Task 1-2
cd Task_1-2
mkdir -p project
echo 'addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")' > project/plugins.sbt
sbt clean assembly
cd ..

echo "Build completed!"
```

Lưu script trên thành `build_all.sh`, chmod +x và chạy:
```bash
chmod +x build_all.sh
./build_all.sh
```
