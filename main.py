from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, hour, to_timestamp, rank
from pyspark.sql.window import Window

def main():
    # Start Spark Session
    spark = SparkSession.builder.appName("IoT Sensor Data Analysis").getOrCreate()

    # Task 1: Load & Basic Exploration
    print("\n--- Task 1: Load & Basic Exploration ---")
    df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)
    df.createOrReplaceTempView("sensor_readings")
    
    print("First 5 rows:")
    df.show(5)

    print("Total number of records:", df.count())
    
    print("Distinct locations:")
    df.select("location").distinct().show()

    df.write.csv("task1_output.csv", header=True, mode="overwrite")

    # Task 2: Filtering & Aggregations
    print("\n--- Task 2: Filtering & Aggregations ---")
    in_range_df = df.filter((col("temperature") >= 18) & (col("temperature") <= 30))
    out_range_df = df.filter((col("temperature") < 18) | (col("temperature") > 30))

    print("In-range temperature count:", in_range_df.count())
    print("Out-of-range temperature count:", out_range_df.count())

    agg_df = df.groupBy("location") \
        .agg(
            avg("temperature").alias("avg_temperature"),
            avg("humidity").alias("avg_humidity")
        ) \
        .orderBy(col("avg_temperature").desc())

    print("Average temperature & humidity by location:")
    agg_df.show()
    agg_df.write.csv("task2_output.csv", header=True, mode="overwrite")

    # Task 3: Time-Based Analysis
    print("\n--- Task 3: Time-Based Analysis ---")
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
    df_with_hour = df.withColumn("hour_of_day", hour(col("timestamp")))

    hourly_avg_df = df_with_hour.groupBy("hour_of_day") \
        .agg(avg("temperature").alias("avg_temp")) \
        .orderBy("hour_of_day")

    print("Average temperature by hour:")
    hourly_avg_df.show()
    hourly_avg_df.write.csv("task3_output.csv", header=True, mode="overwrite")

    # Task 4: Window Function
    print("\n--- Task 4: Sensor Ranking by Avg Temperature ---")
    sensor_avg_temp = df.groupBy("sensor_id") \
        .agg(avg("temperature").alias("avg_temp"))

    window_spec = Window.orderBy(col("avg_temp").desc())
    ranked_sensors = sensor_avg_temp.withColumn("rank_temp", rank().over(window_spec))

    top5_sensors = ranked_sensors.orderBy("rank_temp").limit(5)
    print("Top 5 sensors by avg temperature:")
    top5_sensors.show()
    top5_sensors.write.csv("task4_output.csv", header=True, mode="overwrite")

    # Task 5: Pivot Table
    print("\n--- Task 5: Pivot Table by Location and Hour ---")
    df_with_hour = df.withColumn("hour_of_day", hour(col("timestamp")))
    pivot_df = df_with_hour.groupBy("location") \
        .pivot("hour_of_day", list(range(24))) \
        .agg(avg("temperature")) \
        .orderBy("location")

    print("Pivot Table:")
    pivot_df.show(truncate=False)
    pivot_df.write.csv("task5_output.csv", header=True, mode="overwrite")

    # End session
    spark.stop()
    print("\nAll tasks completed. Output files saved as taskX_output.csv")

if __name__ == "__main__":
    main()