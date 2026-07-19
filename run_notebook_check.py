# Run notebook logic end-to-end (same style as AI_Compute_CaseStudy1.ipynb)
import os
import sys
import builtins
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ---- Cell 0: env ----
_jdk17_candidates = []
_ms = Path(r"C:\Program Files\Microsoft")
if _ms.exists():
    _jdk17_candidates.extend(sorted(_ms.glob("jdk-17*"), reverse=True))
for _jdk in _jdk17_candidates:
    if (_jdk / "bin" / "java.exe").exists():
        os.environ["JAVA_HOME"] = str(_jdk)
        os.environ["PATH"] = str(_jdk / "bin") + os.pathsep + os.environ.get("PATH", "")
        break

_hadoop_candidates = [
    Path.cwd() / "hadoop",
    Path(r"D:\CDAC_PROJECT\AI Comupte CaseStudy\hadoop"),
]
for _h in _hadoop_candidates:
    if (_h / "bin" / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(_h.resolve())
        os.environ["PATH"] = str((_h / "bin").resolve()) + os.pathsep + os.environ.get("PATH", "")
        break

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

print("JAVA_HOME:", os.environ.get("JAVA_HOME"))
print("HADOOP_HOME:", os.environ.get("HADOOP_HOME"))
print("Python:", sys.executable)

# ---- Cell 1: paths ----
_candidates = [
    Path.cwd(),
    Path.cwd().parent,
    Path(r"D:\CDAC_PROJECT\AI Comupte CaseStudy"),
]
base_path = None
for _p in _candidates:
    if (_p / "DataCoSupplyChainDataset.csv").exists() and (_p / "fuel.csv").exists():
        base_path = str(_p.resolve())
        break
if base_path is None:
    raise FileNotFoundError("CSV files not found")

shipments_path = os.path.join(base_path, "DataCoSupplyChainDataset.csv")
fuel_path = os.path.join(base_path, "fuel.csv")
print("base_path:", base_path)
print("shipments_path exists:", os.path.exists(shipments_path))
print("fuel_path exists:", os.path.exists(fuel_path))

# ---- Cell 3 imports ----
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, to_timestamp, date_format
from pyspark.sql.functions import round as rnd
from pyspark.sql.functions import sum as spark_sum
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# ---- Cell 4 spark ----
spark = SparkSession.builder \
    .appName("logistics") \
    .master("local[2]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

sc = spark.sparkContext
print("spark version:", spark.version)
print("app name:", sc.appName)
print("master:", sc.master)

# ---- Cell 5 load ----
df = spark.read.option("header", True).option("inferSchema", True).option("encoding", "ISO-8859-1").csv(shipments_path)
fuel = spark.read.option("header", True).option("inferSchema", True).csv(fuel_path)

print("shipment rows:", df.count())
print("shipment columns:", len(df.columns))
print("fuel rows:", fuel.count())
print("fuel columns:", len(fuel.columns))

# ---- Cell 8 sample ----
print("\n--- sample shipments ---")
df.select(
    "Order Id", "Delivery Status", "Late_delivery_risk",
    "Shipping Mode", "Department Name", "Order Region", "Sales"
).show(5, False)

# ---- Cell 9 timestamps + views ----
df = df.withColumn(
    "order_ts",
    to_timestamp(col("order date (DateOrders)"), "M/d/yyyy H:mm")
).withColumn(
    "ship_ts",
    to_timestamp(col("shipping date (DateOrders)"), "M/d/yyyy H:mm")
)
df.createOrReplaceTempView("shipments")
fuel.createOrReplaceTempView("fuel")
print("views ready")

# ---- Q2 RDD ----
print("\n=== Q2 RDD ===")
rdd_df = df.select(
    "Shipping Mode",
    "Late_delivery_risk",
    "Sales",
    "Department Name",
    "Order Region"
).repartition(20)
rdd_df.cache()
print("rdd source rows:", rdd_df.count())
rdd = rdd_df.rdd
mapped = rdd.map(lambda x: (x["Shipping Mode"], x["Late_delivery_risk"], x["Sales"]))
late = mapped.filter(lambda x: x[1] == 1)
mode_pairs = rdd.map(lambda x: (x["Shipping Mode"], 1))
flat = mode_pairs.flatMap(lambda x: [(x[0], x[1]), (x[0] + "_total", 1)])
sales_mode = mapped.map(lambda x: (x[0], x[2])).reduceByKey(lambda a, b: a + b)

total_rows = rdd.count()
late_rows = late.count()
top_modes = sales_mode.sortBy(lambda x: x[1], ascending=False).take(5)
flat_sample = flat.take(10)

print("total rows:", total_rows)
print("late rows:", late_rows)
print("top modes by sales:", top_modes)
print("flat sample:", flat_sample[:5])

# ---- Q3 ----
print("\n=== Q3 Key-value ===")
kv_mode = rdd.map(lambda x: (x["Shipping Mode"], x["Late_delivery_risk"]))
kv_dept = rdd.map(lambda x: (x["Department Name"], x["Sales"]))
kv_region = rdd.map(lambda x: (x["Order Region"], 1))

grouped_mode = kv_mode.groupByKey().mapValues(list)
mode_late = grouped_mode.mapValues(
    lambda x: (builtins.sum(x), len(x), round(builtins.sum(x) * 100 / len(x), 2))
)
dept_sales = kv_dept.reduceByKey(lambda a, b: a + b)
region_sorted = kv_region.reduceByKey(lambda a, b: a + b).sortByKey()
dept_sales.persist()
region_sorted.persist()

print("groupByKey summary:")
print(mode_late.take(5))
print("top departments by sales:")
print(dept_sales.sortBy(lambda x: x[1], ascending=False).take(5))
print("regions sorted:")
print(region_sorted.take(5))

# ---- Q4 ----
print("\n=== Q4 DataFrame ===")
late_df = df.filter(col("Late_delivery_risk") == 1)
mode_stats = df.groupBy("Shipping Mode").agg(
    count("*").alias("order_count"),
    rnd(avg("Sales"), 2).alias("avg_sales"),
    rnd(spark_sum("Sales"), 2).alias("total_sales"),
    rnd(avg("Days for shipping (real)"), 2).alias("avg_real_days")
)
dept_avg = df.groupBy("Department Name").agg(
    rnd(avg("Sales"), 2).alias("dept_avg_sales")
)
joined_df = df.join(dept_avg, "Department Name", "left")
print("late delivery rows:", late_df.count())
print("shipping mode stats:")
mode_stats.show()
print("joined sample:")
joined_df.select(
    "Department Name", "Sales", "dept_avg_sales", "Shipping Mode", "Late_delivery_risk"
).show(5)

# ---- Q5 SQL ----
print("\n=== Q5 EDA SQL ===")
delivery_perf = spark.sql("""
SELECT
    `Delivery Status`,
    COUNT(*) AS order_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM shipments), 2) AS order_pct,
    ROUND(AVG(`Days for shipping (real)`), 2) AS avg_real_days,
    ROUND(AVG(`Days for shipment (scheduled)`), 2) AS avg_scheduled_days,
    ROUND(SUM(Sales), 2) AS total_sales
FROM shipments
GROUP BY `Delivery Status`
ORDER BY order_count DESC
""")
print("delivery performance:")
delivery_perf.show()

warehouse_util = spark.sql("""
SELECT
    `Department Name`,
    COUNT(*) AS order_count,
    SUM(`Order Item Quantity`) AS total_quantity,
    ROUND(SUM(Sales), 2) AS total_sales,
    ROUND(AVG(Sales), 2) AS avg_sales,
    SUM(CASE WHEN `Late_delivery_risk` = 1 THEN 1 ELSE 0 END) AS late_orders,
    ROUND(SUM(CASE WHEN `Late_delivery_risk` = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS late_pct
FROM shipments
GROUP BY `Department Name`
ORDER BY total_quantity DESC
""")
print("warehouse utilization:")
warehouse_util.show(20, False)

delayed_mode = spark.sql("""
SELECT
    `Shipping Mode`,
    COUNT(*) AS delayed_count,
    ROUND(AVG(`Days for shipping (real)`), 2) AS avg_real_days,
    ROUND(AVG(`Days for shipment (scheduled)`), 2) AS avg_scheduled_days,
    ROUND(SUM(Sales), 2) AS delayed_sales
FROM shipments
WHERE `Late_delivery_risk` = 1
GROUP BY `Shipping Mode`
ORDER BY delayed_count DESC
""")
delayed_region = spark.sql("""
SELECT
    `Order Region`,
    COUNT(*) AS delayed_count,
    ROUND(SUM(Sales), 2) AS delayed_sales,
    ROUND(AVG(`Days for shipping (real)`), 2) AS avg_real_days
FROM shipments
WHERE `Late_delivery_risk` = 1
GROUP BY `Order Region`
ORDER BY delayed_count DESC
""")
print("delayed by mode:")
delayed_mode.show()
print("delayed by region:")
delayed_region.show(10, False)

fuel_class = spark.sql("""
SELECT
    VEHICLECLASS,
    COUNT(*) AS vehicle_count,
    ROUND(AVG(FUELCONSUMPTION_COMB), 2) AS avg_fuel_comb,
    ROUND(AVG(FUELCONSUMPTION_COMB_MPG), 2) AS avg_mpg,
    ROUND(AVG(CO2EMISSIONS), 2) AS avg_co2
FROM fuel
GROUP BY VEHICLECLASS
ORDER BY avg_fuel_comb DESC
""")
print("fuel by class:")
fuel_class.show(20, False)

region_report = spark.sql("""
SELECT
    `Order Region`,
    COUNT(*) AS shipment_count,
    ROUND(SUM(Sales), 2) AS total_sales,
    ROUND(AVG(Sales), 2) AS avg_sales,
    ROUND(AVG(`Days for shipping (real)`), 2) AS avg_real_days,
    ROUND(AVG(`Days for shipment (scheduled)`), 2) AS avg_scheduled_days,
    SUM(CASE WHEN `Late_delivery_risk` = 1 THEN 1 ELSE 0 END) AS late_count,
    ROUND(SUM(CASE WHEN `Late_delivery_risk` = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS late_pct
FROM shipments
GROUP BY `Order Region`
ORDER BY total_sales DESC
""")
print("regional logistics:")
region_report.show(20, False)

monthly = spark.sql("""
SELECT
    date_format(order_ts, 'yyyy-MM') AS month,
    COUNT(*) AS shipment_count,
    SUM(CASE WHEN `Late_delivery_risk` = 1 THEN 1 ELSE 0 END) AS late_count,
    ROUND(SUM(Sales), 2) AS total_sales
FROM shipments
WHERE order_ts IS NOT NULL
GROUP BY date_format(order_ts, 'yyyy-MM')
ORDER BY month
""")
print("monthly:")
monthly.show(20, False)

# ---- Plots (save instead of show) ----
plot_dir = os.path.join(base_path, "run_plots")
os.makedirs(plot_dir, exist_ok=True)

p = delivery_perf.toPandas()
plt.figure(figsize=(8, 4))
plt.bar(p["Delivery Status"], p["order_count"])
plt.title("Orders by delivery status")
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
plt.savefig(os.path.join(plot_dir, "delivery_status.png"))
plt.close()

p = warehouse_util.limit(10).toPandas().sort_values("total_quantity")
plt.figure(figsize=(8, 5))
plt.barh(p["Department Name"], p["total_quantity"])
plt.title("Top departments by quantity")
plt.tight_layout()
plt.savefig(os.path.join(plot_dir, "departments.png"))
plt.close()

p = delayed_mode.toPandas()
plt.figure(figsize=(7, 4))
plt.bar(p["Shipping Mode"], p["delayed_count"])
plt.title("Delayed shipments by mode")
plt.tight_layout()
plt.savefig(os.path.join(plot_dir, "delayed_mode.png"))
plt.close()

p = fuel.select("FUELCONSUMPTION_COMB", "CO2EMISSIONS").dropna().toPandas()
plt.figure(figsize=(7, 4))
plt.scatter(p["FUELCONSUMPTION_COMB"], p["CO2EMISSIONS"], alpha=0.5)
plt.title("Fuel consumption vs CO2")
plt.tight_layout()
plt.savefig(os.path.join(plot_dir, "fuel_co2.png"))
plt.close()

p = region_report.limit(10).toPandas().sort_values("total_sales")
plt.figure(figsize=(8, 5))
plt.barh(p["Order Region"], p["total_sales"])
plt.title("Top regions by sales")
plt.tight_layout()
plt.savefig(os.path.join(plot_dir, "regions.png"))
plt.close()

p = monthly.toPandas()
plt.figure(figsize=(10, 4))
plt.plot(p["month"], p["shipment_count"], marker="o")
plt.title("Monthly shipment count")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig(os.path.join(plot_dir, "monthly.png"))
plt.close()
print("plots saved to", plot_dir)

# ---- Q6 ETL ----
print("\n=== Q6 ETL ===")
out_path = os.path.join(base_path, "etl_output")

ship_etl = df.select(
    "Order Id", "Customer Id", "Shipping Mode", "Delivery Status",
    "Late_delivery_risk", "Days for shipping (real)", "Days for shipment (scheduled)",
    "Sales", "Order Region", "Order Country", "Order City", "order_ts", "ship_ts"
).dropna()

ship_etl = ship_etl.withColumn(
    "delay_flag",
    when(col("Late_delivery_risk") == 1, "delayed").otherwise("not_delayed")
).withColumn(
    "delay_days",
    col("Days for shipping (real)") - col("Days for shipment (scheduled)")
)

warehouse_etl = df.groupBy("Department Name").agg(
    count("*").alias("order_count"),
    spark_sum("Order Item Quantity").alias("total_quantity"),
    rnd(spark_sum("Sales"), 2).alias("total_sales"),
    rnd(avg("Sales"), 2).alias("avg_sales"),
    rnd(avg("Late_delivery_risk"), 2).alias("avg_late_risk")
)

tracking_etl = df.select(
    "Order Id", "Latitude", "Longitude", "Order Region", "Order Country",
    "Order City", "Shipping Mode", "Delivery Status", "Late_delivery_risk",
    "order_ts", "ship_ts"
).dropna()

tracking_etl = tracking_etl.withColumn(
    "tracking_status",
    when(col("Delivery Status") == "Late delivery", "delayed")
    .when(col("Delivery Status") == "Shipping canceled", "cancelled")
    .otherwise("moving_or_completed")
)

ship_etl.write.mode("overwrite").parquet(os.path.join(out_path, "shipments"))
warehouse_etl.write.mode("overwrite").parquet(os.path.join(out_path, "warehouse"))
tracking_etl.write.mode("overwrite").parquet(os.path.join(out_path, "tracking"))

print("shipment etl rows:", ship_etl.count())
print("warehouse etl rows:", warehouse_etl.count())
print("tracking etl rows:", tracking_etl.count())

spark.read.parquet(os.path.join(out_path, "shipments")).select(
    "Order Id", "delay_flag", "delay_days", "Sales"
).show(5)

spark.read.parquet(os.path.join(out_path, "warehouse")).show(5, False)

spark.read.parquet(os.path.join(out_path, "tracking")).select(
    "Order Id", "Latitude", "Longitude", "tracking_status"
).show(5, False)

# ---- Q7 ML ----
print("\n=== Q7 ML ===")
ml_df = df.select(
    "Late_delivery_risk",
    "Days for shipment (scheduled)",
    "Sales",
    "Order Item Quantity",
    "Benefit per order",
    "Order Item Profit Ratio",
    "Shipping Mode",
    "Department Name",
    "Market",
    "Type",
    "Order Region",
    "Customer Segment"
).dropna()

cats = [
    "Shipping Mode", "Department Name", "Market", "Type",
    "Order Region", "Customer Segment"
]
for c in cats:
    idx = c.replace(" ", "_").lower() + "_idx"
    si = StringIndexer(inputCol=c, outputCol=idx, handleInvalid="keep").fit(ml_df)
    ml_df = si.transform(ml_df)

late_cnt = ml_df.filter(col("Late_delivery_risk") == 1).count()
ok_cnt = ml_df.filter(col("Late_delivery_risk") == 0).count()
w0 = late_cnt / ok_cnt
ml_df = ml_df.withColumn(
    "weight",
    when(col("Late_delivery_risk") == 0, w0).otherwise(1.0)
)
print("late:", late_cnt)
print("not late:", ok_cnt)
print("not late weight:", w0)

features = [
    "Days for shipment (scheduled)",
    "Sales",
    "Order Item Quantity",
    "Benefit per order",
    "Order Item Profit Ratio",
    "shipping_mode_idx",
    "department_name_idx",
    "market_idx",
    "type_idx",
    "order_region_idx",
    "customer_segment_idx"
]
va = VectorAssembler(inputCols=features, outputCol="features")
data = va.transform(ml_df).select(
    col("Late_delivery_risk").alias("label"),
    "features",
    "weight"
)
train, test = data.randomSplit([0.8, 0.2], seed=42)
print("train rows:", train.count())
print("test rows:", test.count())

lr = LogisticRegression(maxIter=50, weightCol="weight")
model = lr.fit(train)
pred = model.transform(test)
acc = MulticlassClassificationEvaluator(metricName="accuracy").evaluate(pred)
auc = BinaryClassificationEvaluator(metricName="areaUnderROC").evaluate(pred)
print("accuracy:", acc)
print("auc:", auc)

spark.stop()
print("spark stopped")
print("\nALL STEPS COMPLETED SUCCESSFULLY")
