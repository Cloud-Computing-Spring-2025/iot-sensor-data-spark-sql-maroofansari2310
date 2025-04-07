# 📌 IoT Sensor Data Analysis with PySpark SQL

## 📂 Description
This project analyzes synthetic IoT sensor data using Apache Spark (PySpark). The data includes temperature and humidity readings from sensors placed across different building floors. The goal is to explore, filter, aggregate, rank, and pivot this data using Spark SQL and DataFrame APIs.

---

## 🧑‍💻 Technologies Used
- **Python**
- **Apache Spark (PySpark)**
- **Spark SQL**
- **CSV** (Input/Output)

---

## 📁 Files in the Repository

| File               | Description                                                           |
|--------------------|-----------------------------------------------------------------------|
| `data_generator.py`| Script to generate `sensor_data.csv` with 1000 records using Faker     |
| `sensor_data.csv`  | Sample IoT sensor dataset (can be generated using the script)          |
| `main.py`          | Complete analysis script that runs all 5 Spark SQL tasks               |
| `task1_output.csv` | Output for Task 1: Basic exploration result                            |
| `task2_output.csv` | Output for Task 2: Filtered and aggregated results by location         |
| `task3_output.csv` | Output for Task 3: Hourly average temperature                          |
| `task4_output.csv` | Output for Task 4: Top 5 sensors ranked by average temperature         |
| `task5_output.csv` | Output for Task 5: Pivoted table with average temperature per location/hour |

---

## ✅ Tasks Breakdown & Outputs

---

### 📌 Task 1: Load & Basic Exploration

**Goal:** Load and inspect the sensor dataset.

**Steps:**
- Load `sensor_data.csv` into Spark
- Create a temporary view `sensor_readings`
- Display:
  - First 5 records
  - Total number of records
  - Distinct locations

**Output File:** `task1_output.csv`
```bash
First 5 rows:
+---------+-------------------+-----------+--------+----------------+-----------+
|sensor_id|          timestamp|temperature|humidity|        location|sensor_type|
+---------+-------------------+-----------+--------+----------------+-----------+
|     1019|2025-04-06 07:46:24|      18.99|   42.82|BuildingB_Floor2|      TypeA|
|     1091|2025-04-06 17:29:40|      19.74|   66.84|BuildingA_Floor1|      TypeC|
|     1055|2025-04-06 05:09:42|      17.41|    46.0|BuildingA_Floor1|      TypeA|
|     1093|2025-04-05 02:08:28|      16.09|   49.24|BuildingB_Floor2|      TypeB|
|     1076|2025-04-06 13:34:50|       21.7|   36.76|BuildingB_Floor1|      TypeB|
+---------+-------------------+-----------+--------+----------------+-----------+
```
---

### 📌 Task 2: Filtering & Aggregations

**Goal:** Identify temperature outliers and compute averages.

**Steps:**
- Filter records where `temperature` is **between 18–30°C** (in-range)
- Count in-range vs out-of-range
- Group by `location` to calculate:
  - Average `temperature`
  - Average `humidity`

**Output File:** `task2_output.csv`

```bash
In-range temperature count: 605
Out-of-range temperature count: 395
Average temperature & humidity by location:
+----------------+------------------+------------------+
|        location|   avg_temperature|      avg_humidity|
+----------------+------------------+------------------+
|BuildingA_Floor2|25.197109374999997|             54.51|
|BuildingB_Floor2|25.113117408906877|54.682105263157894|
|BuildingB_Floor1| 24.85833992094862| 55.36434782608691|
|BuildingA_Floor1|24.195327868852466|  57.1081147540984|
+----------------+------------------+------------------+
```
---

### 📌 Task 3: Time-Based Analysis

**Goal:** Understand hourly temperature trends.

**Steps:**
- Convert `timestamp` string to Spark timestamp type
- Extract the `hour_of_day` from each timestamp
- Group by hour and calculate the **average temperature**

**Output File:** `task3_output.csv`
```bash
Average temperature by hour:
+-----------+------------------+
|hour_of_day|          avg_temp|
+-----------+------------------+
|          0|23.279787234042544|
|          1| 25.45628571428572|
|          2| 24.16234042553191|
|          3|25.793947368421048|
|          4|24.005348837209304|
|          5|24.920238095238098|
+-----------+------------------+

```
---
### 📌 Task 4: Sensor Ranking by Temperature

**Goal:** Identify the top 5 sensors with the highest average temperature.

**Steps:**
- Group by `sensor_id` and calculate `avg_temp`
- Use a **Window Function (`RANK()`)** to rank sensors in descending order
- Show top 5 sensors

**Output File:** `task4_output.csv`

```bash
+---------+------------------+---------+
|sensor_id|          avg_temp|rank_temp|
+---------+------------------+---------+
|     1000|            29.313|        1|
|     1045|          28.72125|        2|
|     1099|28.433333333333334|        3|
|     1078|28.298000000000002|        4|
|     1006| 28.10571428571429|        5|
+---------+------------------+---------+
```
---


### 📌 Task 5: Pivot Table by Location and Hour

**Goal:** Visualize temperature variation by hour and location.

**Steps:**
- Use `location` as rows and `hour_of_day` (0–23) as columns
- Use **average temperature** as values
- Create a **pivot table**

**Output File:** `task5_output.csv`
```bash
+------------------+-------+-------+-----+-------+
| location         |   0   |   1   | ... |  23   |
+------------------+-------+-------+-----+-------+
| BuildingA_Floor1 | 24.84 | 24.72 | ... | 25.88 |
| BuildingA_Floor2 | 26.26 | 29.10 | ... | 23.58 |
| BuildingB_Floor1 | 25.30 | 23.70 | ... | 23.64 |
| BuildingB_Floor2 | 27.81 | 27.14 | ... | 23.45 |
+------------------+-------+-------+-----+-------+
```
---

## 🚀 Running the Project

### Install Dependencies
Install the necessary Python libraries:
```bash
pip install pyspark faker
```
### Data generator and main code running 
```bash
python data_generator.py
python main.py
