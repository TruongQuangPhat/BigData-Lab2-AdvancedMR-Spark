# LAB 2 - ADVANCED MAPREDUCE PROBLEMS

## Hướng dẫn Build và Chạy

### YÊU CẦU HỆ THỐNG
- Hadoop 3.x (Pseudo-distributed mode)
- Scala 2.12.x hoặc 2.13.x
- Java 8 hoặc 11
- Môi trường Linux / WSL (Windows Subsystem for Linux)

---

## CHUẨN BỊ MÔI TRƯỜNG VÀ DỮ LIỆU TRÊN HDFS

Tất cả các lệnh dưới đây được thực thi tại thư mục gốc của dự án. 
Đảm bảo bạn đang đứng tại thư mục gốc trước khi bắt đầu.

1. Khởi động Hadoop:
```bash
start-dfs.sh
start-yarn.sh
```

2. Kiểm tra các tiến trình (Đảm bảo có NameNode, DataNode, NodeManager, ResourceManager):
```bash
jps
```

3. Tạo thư mục input trên HDFS và tải dữ liệu từ thư mục `data/` lên:
```bash
hadoop fs -mkdir -p /lab2/input/
hadoop fs -put data/Amazon_Sale_Report.csv /lab2/input/
```

---

## TASK 1-1: SLIDING WINDOW

### Cấu trúc package: `lab2.task11.SlidingWindowJob`

1. Di chuyển vào thư mục mã nguồn và thiết lập môi trường:
```bash
cd src/Task_1-1
export HADOOP_CLASSPATH=$(hadoop classpath):/usr/share/scala/lib/scala-library.jar
```

2. Biên dịch và đóng gói thành file JAR:
```bash
mkdir -p classes
rm -rf classes/*
rm -f SlidingWindowJob.jar
scalac -classpath "$HADOOP_CLASSPATH" -d classes SlidingWindowJob.scala
jar -cvf SlidingWindowJob.jar -C classes .
```

3. Chạy Job trên Hadoop:
```bash
hadoop jar SlidingWindowJob.jar lab2.task11.SlidingWindowJob /lab2/input/Amazon_Sale_Report.csv /lab2/output/task1-1
```

4. Lấy kết quả về máy và dọn dẹp header thừa:
```bash
rm -f Task_1-1.csv
hadoop fs -getmerge /lab2/output/task1-1 Task_1-1.csv
sed -i '2,${/^State,TargetDate/d}' Task_1-1.csv
```

5. Quay lại thư mục gốc:
```bash
cd ../..
```

---

## TASK 1-2: MEDIAN VARIETY

### Cấu trúc package: `lab2.task12.MedianVarietyJob`
*Lưu ý: Task này sử dụng kỹ thuật Job Chaining (Nối 2 Jobs).*

1. Di chuyển vào thư mục mã nguồn và thiết lập môi trường:
```bash
cd src/Task_1-2
export HADOOP_CLASSPATH=$(hadoop classpath):/usr/share/scala/lib/scala-library.jar
```

2. Biên dịch và đóng gói thành file JAR:
```bash
mkdir -p classes
rm -rf classes/*
rm -f MedianVarietyJob.jar
scalac -classpath "$HADOOP_CLASSPATH" -d classes MedianVarietyJob.scala
jar -cvf MedianVarietyJob.jar -C classes .
```

3. Chạy Job trên Hadoop (Cần truyền đủ 3 tham số):
```bash
hadoop jar MedianVarietyJob.jar lab2.task12.MedianVarietyJob /lab2/input/Amazon_Sale_Report.csv /lab2/output/task1-2-temp /lab2/output/task1-2
```

4. Lấy kết quả về máy và dọn dẹp header thừa:
```bash
rm -f Task_1-2.csv
hadoop fs -getmerge /lab2/output/task1-2 Task_1-2.csv
sed -i '2,${/^Month,State/d}' Task_1-2.csv
```

5. Quay lại thư mục gốc:
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

### Task 1-1 Output Format (`Task_1-1.csv`):
```csv
State,TargetDate,MostBoughtSize,Count
ANDAMAN & NICOBAR,04-02-22,M,1
ANDHRA PRADESH,04-02-22,M,41
...
```

### Task 1-2 Output Format (`Task_1-2.csv`):
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

## QUY TRÌNH TỰ ĐỘNG HÓA (BUILD & RUN ALL)

Dự án cung cấp một script shell giúp tự động hóa toàn bộ các bước: Dọn dẹp môi trường cũ -> Biên dịch mã nguồn -> Đóng gói JAR -> Chạy MapReduce trên Hadoop -> Trích xuất kết quả CSV cuối cùng.

### Bước 1: Cấp quyền thực thi
Mặc định các file mới tạo sẽ không có quyền chạy. Bạn cần cấp quyền thực thi cho script bằng lệnh sau (chỉ cần làm 1 lần duy nhất):
```bash
chmod +x build_and_run_all.sh
```

### Bước 2: Thực thi script
Để bắt đầu quá trình xử lý toàn bộ các Task, hãy chạy lệnh:
```bash
./build_and_run_all.sh
```

<details>
<summary><b>Xem chi tiết nội dung file build_and_run_all.sh</b></summary>

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
```

</details>
