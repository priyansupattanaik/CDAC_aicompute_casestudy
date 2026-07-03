# Smart Logistics and Supply Chain Analytics System using Apache Spark

## Project Overview

This project implements a Smart Logistics and Supply Chain Analytics System using Apache Spark and PySpark.

The system analyzes shipment records, delivery performance, department-level warehouse utilization, delayed shipments, fuel consumption trends, and regional logistics performance. It also builds a machine learning model to predict delivery delay risk.

This work is based on the case study requirement to design, implement, and deploy a logistics analytics platform using Apache Spark and PySpark.

## Case Study Requirements Covered

| Question | Requirement                                                   | Status    |
| -------- | ------------------------------------------------------------- | --------- |
| Q1       | Spark initialization and logistics dataset loading            | Completed |
| Q2       | RDD transformations and actions                               | Completed |
| Q3       | Key-value RDD operations, shuffle operations, and persistence | Completed |
| Q4       | Spark DataFrame filtering, grouping, joins, and aggregations  | Completed |
| Q5       | EDA and Spark SQL analysis                                    | Completed |
| Q6       | ETL pipeline for shipment, warehouse, and tracking-style data | Completed |
| Q7       | ML model for delivery delay prediction                        | Completed |

## Dataset Used

### 1. DataCo Supply Chain Dataset

File used:

```text
DataCoSupplyChainDataset.csv
```

[Kaggle](https://www.kaggle.com/datasets/evilspirit05/datasupplychain)

Purpose:

- Shipment analysis
- Delivery performance analysis
- Delayed shipment identification
- Department-level warehouse utilization
- Regional logistics reporting
- Delay prediction model

Important columns used:

```text
Order Id
Customer Id
Shipping Mode
Delivery Status
Late_delivery_risk
Days for shipping (real)
Days for shipment (scheduled)
Sales
Order Region
Order Country
Order City
Department Name
Order Item Quantity
Benefit per order
Order Item Profit Ratio
Market
Type
Customer Segment
```

### 2. Fuel Consumption Dataset

File used:

```text
fuel.csv
```

[IBM Developer Skills Network](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-ML0101EN-SkillsNetwork/labs/Module%202/data/FuelConsumptionCo2.csv)

Purpose:

- Fuel consumption trend analysis
- Vehicle class fuel comparison
- CO2 emission trend analysis

Important columns used:

```text
MAKE
MODEL
VEHICLECLASS
ENGINESIZE
CYLINDERS
FUELCONSUMPTION_CITY
FUELCONSUMPTION_HWY
FUELCONSUMPTION_COMB
FUELCONSUMPTION_COMB_MPG
CO2EMISSIONS
```

## Important Assumptions

The case study mentions warehouse and GPS tracking data, but the available dataset does not contain a separate warehouse table or a separate GPS tracking table.

Because of that:

- `Department Name` is used as a warehouse or department utilization proxy.
- Tracking-style ETL is created from shipment movement fields such as order region, country, city, shipping mode, delivery status, and delivery days.
- Fuel data is analyzed separately. It is not joined with shipment-level records because there is no common vehicle or fleet identifier between both datasets.

No fake GPS coordinates, warehouse IDs, or shipment-fuel joins were created.

## Project Structure

```text
CDAC_aicompute_casestudy/
â”œâ”€â”€ AI_Compute_CaseStudy1.ipynb
â”œâ”€â”€ DataCoSupplyChainDataset.csv
â”œâ”€â”€ fuel.csv
â”œâ”€â”€ screenshots/
â”œâ”€â”€ requirements.txt
```

## How to Run the Notebook

### Step 1: Clone the Repository

```bash
git clone https://github.com/priyansupattanaik/CDAC_aicompute_casestudy
cd CDAC_aicompute_casestudy
```

### Step 2: Open Notebook in Google Colab

Open:

```text
AI_Compute_CaseStudy1.ipynb
```

### Step 3: Upload or Verify Dataset Files

Make sure these files are available in the expected path used inside the notebook:

```text
DataCoSupplyChainDataset.csv
fuel.csv
```

For Google Colab, the notebook uses Google Drive paths. Update the base path if your folder location is different.

Example:

```python
base_path = "/content/drive/MyDrive/Colab Notebooks/AI Comupte CaseStudy"
```

### Step 4: Run All Cells in Order

Run the notebook from top to bottom.

The notebook flow is:

1. Mount Google Drive
2. Set dataset paths
3. Install and initialize PySpark
4. Load datasets
5. Inspect schema and records
6. Solve Q1 to Q7 step by step
7. Generate EDA outputs and plots
8. Build ETL outputs
9. Train and evaluate ML model
10. Print final checklist and conclusions

## Analysis Performed

### Q1: Spark Initialization and Data Loading

- Spark session initialized
- Shipment dataset loaded
- Fuel dataset loaded
- Rows and columns verified

### Q2: RDD Implementation

RDD operations used:

- `map`
- `filter`
- `flatMap`
- `reduceByKey`

Actions used:

- `count`
- `take`
- `collect`

### Q3: Key-Value Operations and Persistence

Implemented:

- Key-value RDDs
- `groupByKey`
- `reduceByKey`
- `sortByKey`
- `persist`

### Q4: Spark DataFrame Operations

Implemented:

- Filtering late deliveries
- Grouping by shipping mode
- Aggregations on sales and delivery days
- Join with department-level average sales

### Q5: EDA and Spark SQL

Spark SQL queries were used to analyze:

- Delivery performance
- Warehouse utilization
- Delayed shipments
- Fuel consumption trends
- Regional logistics reports

Plots were added for better interpretation of:

- Delivery status distribution
- Warehouse or department utilization
- Delayed shipments by shipping mode
- Fuel consumption by vehicle class
- Regional logistics performance
- Model feature coefficients

### Q6: ETL Pipeline Development

ETL outputs were created for:

- Shipment data
- Warehouse or department utilization data
- Tracking-style shipment movement data

The ETL output is written in Parquet format.

Generated output folder:

```text
etl_output/
```

This folder is generated by running the notebook and does not need to be manually edited.

### Q7: Machine Learning Implementation

A Logistic Regression model was implemented to predict delivery delay risk.

Target column:

```text
Late_delivery_risk
```

Features used include:

```text
Days for shipment (scheduled)
Sales
Order Item Quantity
Benefit per order
Order Item Profit Ratio
Shipping Mode
Department Name
Market
Type
Order Region
Customer Segment
```

Model evaluation includes:

- Accuracy
- AUC
- Prediction sample
- Feature coefficient analysis

## Key Findings

Based on the notebook outputs:

- Late delivery is the largest delivery status category.
- Standard Class has the highest number of delayed shipments.
- Fan Shop, Golf, and Apparel show the highest department-level utilization by quantity.
- Western Europe and Central America are among the strongest regions by shipment count and sales.
- Van and large vehicle classes show higher average fuel consumption and CO2 emissions.
- Shipping mode and scheduled shipment days are important signals in the delay prediction model.

## Limitations

- No separate warehouse master table is available.
- No separate GPS tracking dataset is available.
- No direct vehicle or fleet ID is available to connect shipment records with fuel consumption records.
- The ML model uses `StringIndexer` for categorical variables. This is acceptable for the case study, but OneHotEncoder would be better for stronger categorical interpretation.
- The completed Q1-Q7 notebook is preserved, and Docker, Kubernetes, and CI/CD deliverables have been added for the deployment stage.

## Docker, Kubernetes, and CI/CD Execution

This deployment stage starts from the completed Q1-Q7 notebook: `AI_Compute_CaseStudy1.ipynb`.

Docker image name:

```text
smart-logistics-spark:latest
```

Docker build command:

```bash
docker build -t smart-logistics-spark:latest .
```

Docker run command:

```bash
docker run --rm smart-logistics-spark:latest
```

Verified Docker run output values from `screenshots_outputs/docker_run.log`:

```text
shipment rows: 180519
shipment cols: 53
fuel rows: 1067
fuel cols: 13
etl completed
application completed
SparkContext stopped with exitCode 0
Successfully stopped SparkContext
```

Kubernetes manifest files:

```text
k8s/deployment.yaml
k8s/service.yaml
```

Kubernetes status:

```text
Kubernetes manifests are prepared. Local deployment is blocked because kubectl is installed but no Kubernetes context exists on this machine. Enable Docker Desktop Kubernetes or start a local cluster such as Minikube, then apply the manifests.
```

CI/CD workflow file:

```text
.github/workflows/ci-cd.yml
```

Evidence logs are stored in:

```text
screenshots_outputs/
```

The evidence files are terminal logs, not screenshots. No fake screenshots were created.
## Author

```text
Priyansu Pattanaik.
```
