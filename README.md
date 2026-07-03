# Smart Logistics and Supply Chain Analytics System using Apache Spark

## Project Overview

This project implements a Smart Logistics and Supply Chain Analytics System using Apache Spark and PySpark. It analyzes shipment records, delivery performance, delayed shipments, department-level warehouse utilization, fuel consumption trends, regional logistics performance, and late-delivery risk.

The notebook work from Q1 to Q7 is completed in `AI_Compute_CaseStudy1.ipynb`. The post-notebook deployment stage adds a Dockerized Spark application, Kubernetes manifests, a GitHub Actions CI/CD workflow, evidence logs, and verification documentation.

## Case Study Requirements Covered

| Question | Requirement | Status |
| --- | --- | --- |
| Q1 | Spark initialization and logistics dataset loading | Completed |
| Q2 | RDD transformations and actions | Completed |
| Q3 | Key-value RDD operations, shuffle operations, and persistence | Completed |
| Q4 | Spark DataFrame filtering, grouping, joins, and aggregations | Completed |
| Q5 | Exploratory Data Analysis and Spark SQL analytics | Completed |
| Q6 | ETL pipeline for shipment, warehouse, and tracking-style data | Completed |
| Q7 | Machine learning model for delivery delay prediction | Completed |
| Post-Q7 | Docker, Kubernetes, CI/CD, evidence logs, and documentation | Completed, with Kubernetes deployment pending local cluster setup |

## Datasets Used

### DataCo Supply Chain Dataset

File:

```text
DataCoSupplyChainDataset.csv
```

Purpose:

- Shipment analysis
- Delivery status analysis
- Late delivery risk analysis
- Department or warehouse utilization proxy
- Sales and regional logistics reporting
- Machine learning model for late delivery risk

Important columns used include:

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

### Fuel Consumption Dataset

File:

```text
fuel.csv
```

Purpose:

- Fuel consumption trend analysis
- Vehicle class comparison
- CO2 emissions analysis

Important columns used include:

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

The case study mentions warehouse and GPS tracking data, but the provided files do not include a separate warehouse master table or a GPS tracking table.

Because of that:

- `Department Name` is used as a warehouse or department utilization proxy.
- Tracking-style ETL is created from shipment movement fields such as order region, country, city, shipping mode, delivery status, and delivery days.
- Fuel data is analyzed separately because there is no common vehicle or fleet identifier between the shipment dataset and the fuel dataset.

No fake warehouse IDs, GPS coordinates, shipment-fuel joins, screenshots, logs, or metrics were created.

## Project Structure

```text
CDAC_aicompute_casestudy/
|-- AI_Compute_CaseStudy1.ipynb
|-- DataCoSupplyChainDataset.csv
|-- fuel.csv
|-- app/
|   |-- spark_app.py
|-- k8s/
|   |-- deployment.yaml
|   |-- service.yaml
|-- .github/
|   |-- workflows/
|       |-- ci-cd.yml
|-- screenshots_outputs/
|   |-- evidence logs and rendered case study screenshots
|-- Dockerfile
|-- .dockerignore
|-- README.md
|-- FINAL_VERIFICATION_REPORT.md
|-- PERSONAL_VIVA_DOCUMENTATION.md
```

## How to Run the Notebook

1. Clone the repository.

```bash
git clone https://github.com/priyansupattanaik/CDAC_aicompute_casestudy
cd CDAC_aicompute_casestudy
```

2. Open the notebook.

```text
AI_Compute_CaseStudy1.ipynb
```

3. Verify that the dataset files are available.

```text
DataCoSupplyChainDataset.csv
fuel.csv
```

4. Run all notebook cells in order.

The notebook flow is:

1. Mount or configure the dataset path.
2. Install and initialize PySpark.
3. Load shipment and fuel datasets.
4. Complete RDD operations.
5. Complete key-value RDD operations.
6. Complete Spark DataFrame operations.
7. Run Spark SQL and EDA analysis.
8. Generate ETL outputs.
9. Train and evaluate the machine learning model.
10. Print final outputs and conclusions.

## Notebook Work Summary

### Q1: Spark Initialization and Data Loading

- SparkSession initialized.
- Shipment dataset loaded.
- Fuel dataset loaded.
- Dataset row and column counts verified.

### Q2: RDD Implementation

RDD operations include `map`, `filter`, `flatMap`, `reduceByKey`, `count`, `take`, and `collect`.

### Q3: Key-Value Operations and Persistence

Implemented key-value RDDs, `groupByKey`, `reduceByKey`, sorting, shuffle-related operations, and persistence.

### Q4: Spark DataFrame Operations

Implemented filtering, grouping, aggregations, and joins on structured shipment data.

### Q5: EDA and Spark SQL

Spark SQL queries analyze:

- Delivery performance
- Warehouse or department utilization
- Delayed shipments
- Fuel consumption trends
- Regional logistics performance

### Q6: ETL Pipeline Development

ETL outputs are generated for:

- Shipment data
- Warehouse or department utilization data
- Tracking-style shipment movement data

The notebook writes local ETL outputs to `etl_output/`.

### Q7: Machine Learning Implementation

A Logistic Regression model predicts delivery delay risk using `Late_delivery_risk` as the target column.

Features include scheduled shipping days, sales, item quantity, benefit per order, profit ratio, shipping mode, department, market, order type, order region, and customer segment.

## Dockerized Spark Application

The post-Q7 Docker application is implemented in:

```text
app/spark_app.py
```

The application:

- Starts a SparkSession named `smart-logistics-spark`.
- Reads `/app/data/DataCoSupplyChainDataset.csv`.
- Reads `/app/data/fuel.csv`.
- Prints shipment and fuel row/column counts.
- Creates Spark SQL temp views named `shipments` and `fuel`.
- Runs delivery performance analysis.
- Runs warehouse utilization analysis.
- Runs delayed shipment analysis.
- Runs fuel trend analysis.
- Runs regional logistics analysis.
- Writes Parquet ETL outputs inside the container at `/app/output/shipments`, `/app/output/warehouse`, and `/app/output/tracking`.
- Prints `etl completed` and `application completed`.
- Stops Spark cleanly.

## Docker Execution

Docker image name:

```text
smart-logistics-spark:latest
```

Build command:

```bash
docker build -t smart-logistics-spark:latest .
```

Run command:

```bash
docker run --rm smart-logistics-spark:latest
```

Verified Docker output values from evidence logs:

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

Evidence files include:

```text
screenshots_outputs/18_docker_build.log
screenshots_outputs/19_docker_images.log
screenshots_outputs/21_docker_run.log
screenshots_outputs/22_docker_run_key_lines.log
```

## Kubernetes Files

Kubernetes manifests are stored in:

```text
k8s/deployment.yaml
k8s/service.yaml
```

The Deployment uses:

```text
image: smart-logistics-spark:latest
imagePullPolicy: Never
```

`imagePullPolicy: Never` is used because this project is designed for a local Docker Desktop or Minikube workflow where the image is already built locally.

The Service is a `NodePort` service exposing port `8080`.

Current Kubernetes status:

```text
Kubernetes YAML files are prepared. Local deployment is blocked on this machine because kubectl is installed but no Kubernetes context exists. Enable Docker Desktop Kubernetes or start Minikube before applying the manifests.
```

This is an honest environment blocker. The project does not claim that Kubernetes deployment passed without a running local cluster.

## CI/CD Workflow

The GitHub Actions workflow is stored in:

```text
.github/workflows/ci-cd.yml
```

The workflow runs on:

- Push to `main`
- Pull request to `main`
- Manual trigger using `workflow_dispatch`

The workflow steps are:

1. Checkout repository.
2. Validate required files.
3. Build the Docker image.
4. Run the Docker image.
5. Validate Kubernetes manifest content offline without requiring a live Kubernetes cluster.

The Kubernetes validation step is offline because GitHub-hosted runners do not have this project local Kubernetes cluster configured. Actual Kubernetes deployment still requires Docker Desktop Kubernetes or Minikube locally.

Latest verified CI/CD run:

```text
Status: Passed
Run URL: https://github.com/priyansupattanaik/CDAC_aicompute_casestudy/actions/runs/28652457208
Commit: d9a1370bc18f03373f8d8a019d79988d66a35f72
```

## Evidence and Screenshots

Evidence is stored in:

```text
screenshots_outputs/
```

This folder contains terminal logs and real rendered case study screenshots.

Important evidence files include:

```text
screenshots_outputs/21_docker_run.log
screenshots_outputs/22_docker_run_key_lines.log
screenshots_outputs/23_kubectl_client.log
screenshots_outputs/24_kubernetes_contexts.log
screenshots_outputs/27_kubernetes_status.log
screenshots_outputs/case_study_page-1.png
screenshots_outputs/case_study_page-2.png
```

The case study PDF screenshots were rendered using Poppler from the real `Case Study11.pdf` file. No fake screenshots were created.

## Final Verification Documents

Additional verification and explanation files:

```text
FINAL_VERIFICATION_REPORT.md
PERSONAL_VIVA_DOCUMENTATION.md
```

These files document what was verified, what passed, what is blocked, and how to explain the implementation.

## Key Findings

- Late delivery is the largest delivery status category in the analyzed shipment data.
- Standard Class has the highest number of delayed shipments.
- Fan Shop, Golf, and Apparel show high department-level utilization by quantity.
- Western Europe and Central America are among the stronger regions by shipment count and sales.
- Van and large vehicle classes show higher average fuel consumption and CO2 emissions.
- Shipping mode and scheduled shipment days are useful signals for delivery delay prediction.

## Limitations

- No separate warehouse master table is available.
- No separate GPS tracking dataset is available.
- No direct vehicle or fleet ID is available to join shipment records with fuel consumption records.
- Kubernetes deployment requires a local Kubernetes context such as Docker Desktop Kubernetes or Minikube.
- CI validates Kubernetes manifest content offline, but actual Kubernetes deployment must be tested on a real cluster.
- The ML model uses `StringIndexer` for categorical variables; OneHotEncoder could improve categorical feature handling.

## Author

```text
Priyansu Pattanaik
```
