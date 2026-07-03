from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count, when, round

spark = SparkSession.builder.appName("smart-logistics-spark").getOrCreate()

base = "/app/data"
ship_path = f"{base}/DataCoSupplyChainDataset.csv"
fuel_path = f"{base}/fuel.csv"

df = spark.read.option("header", True).option("inferSchema", True).csv(ship_path)
fuel = spark.read.option("header", True).option("inferSchema", True).csv(fuel_path)

df.createOrReplaceTempView("shipments")
fuel.createOrReplaceTempView("fuel")

print("shipment rows:", df.count())
print("shipment cols:", len(df.columns))
print("fuel rows:", fuel.count())
print("fuel cols:", len(fuel.columns))

print("\ndelivery performance")
spark.sql("""
SELECT
    `Delivery Status`,
    COUNT(*) AS order_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM shipments), 2) AS order_pct,
    ROUND(AVG(`Days for shipping (real)`), 2) AS avg_real_days
FROM shipments
GROUP BY `Delivery Status`
ORDER BY order_count DESC
""").show()

print("\nwarehouse utilization")
spark.sql("""
SELECT
    `Department Name`,
    COUNT(*) AS order_count,
    SUM(`Order Item Quantity`) AS total_quantity,
    ROUND(SUM(Sales), 2) AS total_sales
FROM shipments
GROUP BY `Department Name`
ORDER BY total_quantity DESC
""").show()

print("\ndelayed shipments")
spark.sql("""
SELECT
    `Shipping Mode`,
    COUNT(*) AS delayed_count,
    ROUND(AVG(`Days for shipping (real)`), 2) AS avg_real_days
FROM shipments
WHERE `Late_delivery_risk` = 1
GROUP BY `Shipping Mode`
ORDER BY delayed_count DESC
""").show()

print("\nfuel trends")
spark.sql("""
SELECT
    VEHICLECLASS,
    COUNT(*) AS vehicle_count,
    ROUND(AVG(FUELCONSUMPTION_COMB), 2) AS avg_fuel_comb,
    ROUND(AVG(CO2EMISSIONS), 2) AS avg_co2
FROM fuel
GROUP BY VEHICLECLASS
ORDER BY avg_fuel_comb DESC
""").show(10, False)

print("\nregional logistics")
spark.sql("""
SELECT
    `Order Region`,
    COUNT(*) AS shipment_count,
    ROUND(SUM(Sales), 2) AS total_sales,
    SUM(CASE WHEN `Late_delivery_risk` = 1 THEN 1 ELSE 0 END) AS late_count
FROM shipments
GROUP BY `Order Region`
ORDER BY total_sales DESC
""").show(10, False)

out = "/app/output"

ship_etl = df.select(
    "Order Id",
    "Customer Id",
    "Shipping Mode",
    "Delivery Status",
    "Late_delivery_risk",
    "Days for shipping (real)",
    "Days for shipment (scheduled)",
    "Sales",
    "Order Region",
    "Order Country",
    "Order City"
).dropna()

ship_etl = ship_etl.withColumn(
    "delay_status",
    when(col("Late_delivery_risk") == 1, "delayed").otherwise("not_delayed")
)

warehouse_etl = df.groupBy("Department Name").agg(
    count("*").alias("order_count"),
    sum("Order Item Quantity").alias("total_quantity"),
    round(sum("Sales"), 2).alias("total_sales"),
    round(avg("Sales"), 2).alias("avg_sales")
)

tracking_etl = df.select(
    "Order Id",
    "Order Region",
    "Order Country",
    "Order City",
    "Shipping Mode",
    "Delivery Status",
    "Days for shipping (real)",
    "Days for shipment (scheduled)",
    "Late_delivery_risk"
).dropna()

tracking_etl = tracking_etl.withColumn(
    "tracking_status",
    when(col("Delivery Status") == "Late delivery", "delayed")
    .when(col("Delivery Status") == "Shipping canceled", "cancelled")
    .otherwise("moving_or_completed")
)

ship_etl.write.mode("overwrite").parquet(f"{out}/shipments")
warehouse_etl.write.mode("overwrite").parquet(f"{out}/warehouse")
tracking_etl.write.mode("overwrite").parquet(f"{out}/tracking")

print("\netl completed")
print("application completed")

spark.stop()
