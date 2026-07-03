# Personal Viva Documentation - CDAC AI Compute Case Study

Verification time: 2026-07-03 14:45:41 +05:30

## 1. Project Overview

This project is a Smart Logistics and Supply Chain Analytics System using Apache Spark and PySpark. The main notebook, AI_Compute_CaseStudy1.ipynb, contains the completed academic work from Q1 to Q7. After the notebook stage, Docker, Kubernetes, CI/CD, and evidence logs were added to show that the Spark application can be packaged, executed, and prepared for deployment.

In viva, I should explain that the notebook is the analytical and ML part of the case study, while the files added after Q7 are the engineering and deployment part. The Docker container runs a standalone Spark application using the same datasets and reproduces the important analytics and ETL outputs.

## 2. Dataset Explanation

DataCoSupplyChainDataset.csv is the main supply chain dataset. It contains order, customer, shipment, delivery status, late delivery risk, department, sales, region, country, city, and shipping mode fields. I used it to analyze shipment volume, delivery performance, delayed shipments, department-level warehouse utilization, and regional logistics performance.

uel.csv is a fuel consumption and CO2 emissions dataset. It is used to analyze fuel consumption and CO2 trends by vehicle class. This is relevant to logistics because vehicle fuel efficiency and emissions affect fleet cost, sustainability, and planning.

Both datasets are relevant because modern logistics analytics is not only about whether shipments arrived, but also about operational performance, cost, route/region behavior, resource utilization, and environmental impact. There is no common vehicle or fleet identifier between both datasets, so the project analyzes them separately instead of creating a fake join.

## 3. What I Did in Q1-Q7

Q1 covers Spark setup and loading the logistics and fuel datasets.

Q2 covers RDD operations such as map, filter, flatMap, reduceByKey, count, take, and collect.

Q3 covers key-value RDD transformations, shuffle operations, sorting, grouping, and persistence.

Q4 covers DataFrame operations such as filtering, grouping, aggregations, and joins.

Q5 covers Spark SQL analytics for delivery performance, warehouse utilization, delayed shipments, fuel trends, and regional logistics.

Q6 covers ETL output creation for shipment, warehouse or department utilization, and tracking-style movement data. The ETL output is written in Parquet format.

Q7 covers a machine learning model for late delivery risk prediction. The exact implementation, model features, outputs, and notebook details remain in AI_Compute_CaseStudy1.ipynb.

## 4. Docker Work

Docker was added so the Spark application can run in a repeatable environment without depending on a manually configured local Python or Spark installation.

The Dockerfile uses pache/spark-py:latest because it already contains Apache Spark and PySpark support. This avoids manually installing Spark inside the image.

The Dockerfile copies pp/spark_app.py into /app/app/. It also copies DataCoSupplyChainDataset.csv and uel.csv into /app/data/, because the container must have access to the input datasets at runtime.

The command in the Dockerfile runs:

`ash
/opt/spark/bin/spark-submit /app/app/spark_app.py
`

The Docker build command creates the image:

`ash
docker build --no-cache -t smart-logistics-spark:latest .
`

The Docker run command starts the container and runs the Spark job:

`ash
docker run --rm smart-logistics-spark:latest
`

The Docker run is proven successful by the evidence log screenshots_outputs/21_docker_run.log. Important verified output values are:

`	ext
shipment rows: 180519
shipment cols: 53
fuel rows: 1067
fuel cols: 13
etl completed
application completed
SparkContext is stopping with exitCode 0
Successfully stopped SparkContext
`

These values prove that both datasets were loaded, Spark queries ran, ETL outputs were written inside the container, and Spark shut down cleanly.

## 5. Spark Application Explanation

pp/spark_app.py starts by importing SparkSession and Spark SQL helper functions such as col, vg, sum, count, when, and ound.

It creates a SparkSession named smart-logistics-spark. This is the entry point for Spark DataFrame and SQL work.

It defines /app/data as the base path because the Dockerfile copies both datasets into that location inside the container.

It reads DataCoSupplyChainDataset.csv and uel.csv using Spark CSV reader with headers enabled and schema inference enabled.

It creates temporary SQL views named shipments and uel. These views allow Spark SQL queries to be written directly against the DataFrames.

It prints shipment row count, shipment column count, fuel row count, and fuel column count. These are basic validation checks that prove the files were read correctly.

The delivery performance query groups by Delivery Status, counts orders, calculates percentage share, and calculates average real shipping days.

The warehouse utilization query groups by Department Name, counts orders, sums item quantity, and sums sales. Because no separate warehouse table exists, department is used as a warehouse or utilization proxy.

The delayed shipments query filters records where Late_delivery_risk = 1, groups by shipping mode, and calculates delayed shipment counts and average real shipping days.

The fuel trends query groups the fuel dataset by VEHICLECLASS and calculates average fuel consumption and average CO2 emissions.

The regional logistics query groups shipments by Order Region, calculates shipment count, total sales, and late shipment count.

The ETL section writes three Parquet outputs inside /app/output: shipments, warehouse, and 	racking. Parquet is used because it is a columnar format that is efficient for analytics.

At the end, the application prints etl completed and pplication completed, then calls spark.stop() to shut down Spark cleanly.

## 6. Kubernetes Work

Kubernetes was added to show how the Dockerized Spark application could be deployed to a local cluster.

k8s/deployment.yaml defines a Deployment named logistics-spark. A Deployment manages the pod lifecycle and can recreate pods if they fail.

The container image is smart-logistics-spark:latest. imagePullPolicy: Never is used because this is intended for a local Docker Desktop or Minikube workflow where the image already exists locally and should not be pulled from Docker Hub.

Resource requests and limits are included so Kubernetes has scheduling information and the pod has controlled CPU and memory usage.

The command inside the pod runs Spark first, then starts a simple Python HTTP server on port 8080 serving /app/output. This makes the generated output directory reachable after the Spark job completes.

k8s/service.yaml defines a NodePort Service named logistics-spark-service. A Service provides stable network access to pods selected by the pp: logistics-spark label.

Actual Kubernetes deployment status from this verification: blocked by environment. kubectl is installed, but screenshots_outputs/24_kubernetes_contexts.log shows no configured Kubernetes context. The dry-run logs screenshots_outputs/25_k8s_deployment_dry_run.log and screenshots_outputs/26_k8s_service_dry_run.log show that kubectl could not contact an API server at localhost:8080. Therefore, Kubernetes deployment is not marked as passed. The next step is to enable Docker Desktop Kubernetes or start Minikube, then rerun the manifest validation and deployment commands.

## 7. CI/CD Work

GitHub Actions was added in .github/workflows/ci-cd.yml to automate validation on push and pull request to main.

The workflow checks out the repository, validates that required source files exist, builds the Docker image, runs the Docker image, and validates Kubernetes manifests with kubectl apply --dry-run=client.

The source-file validation step checks for the notebook, datasets, Dockerfile, Spark app, and Kubernetes YAML files.

The Docker build step confirms that the repository can produce the container image.

The Docker run step confirms that the Spark application can execute inside the image.

The Kubernetes validation step checks manifests in CI as far as the CI environment supports it.

## 8. Evidence Logs

Important evidence files from this verification are:

`	ext
screenshots_outputs/18_docker_build.log
screenshots_outputs/19_docker_images.log
screenshots_outputs/21_docker_run.log
screenshots_outputs/22_docker_run_key_lines.log
screenshots_outputs/23_kubectl_client.log
screenshots_outputs/24_kubernetes_contexts.log
screenshots_outputs/25_k8s_deployment_dry_run.log
screenshots_outputs/26_k8s_service_dry_run.log
screenshots_outputs/27_kubernetes_status.log
screenshots_outputs/37_readme_current.log
screenshots_outputs/38_git_status_before_final_commit.log
screenshots_outputs/39_evidence_log_list.log
screenshots_outputs/40_final_docker_image_check.log
screenshots_outputs/41_final_docker_success_check.log
`

No fake screenshots were created. The evidence is stored as terminal logs.

## 9. Viva Questions and Answers

1. What is Apache Spark?

Apache Spark is a distributed data processing engine used for large-scale batch processing, SQL analytics, streaming, machine learning, and graph processing.

2. Why did you use Spark for supply chain analytics?

Supply chain datasets can become very large. Spark supports distributed processing, DataFrames, SQL, and ML, which are useful for analyzing shipments, delays, sales, regions, and delivery performance.

3. What is PySpark?

PySpark is the Python API for Apache Spark. It lets me write Spark jobs using Python while Spark handles distributed execution.

4. What is the difference between RDD and DataFrame?

RDD is a low-level distributed collection API. DataFrame is a higher-level structured API with schema information and query optimization through Spark's Catalyst optimizer.

5. Why use DataFrames instead of only RDDs?

DataFrames are easier for structured data, support SQL-like operations, and are usually more optimized than manually written RDD transformations.

6. What is Spark SQL?

Spark SQL allows SQL queries to be run on Spark DataFrames or temporary views. It is useful when analysis can be expressed clearly using SQL.

7. What is ETL?

ETL means Extract, Transform, Load. In this project, data is extracted from CSV files, transformed into analytics-ready DataFrames, and loaded as Parquet outputs.

8. Why did you use Parquet?

Parquet is a columnar storage format. It is efficient for analytics because Spark can read only required columns and compress data well.

9. What is late delivery risk?

Late_delivery_risk is a field in the shipment dataset that indicates whether an order has risk of late delivery. It is used for delay analysis and ML prediction.

10. What does delivery performance analysis show?

It groups orders by delivery status, counts orders, calculates percentages, and shows average real shipping days.

11. What does warehouse utilization mean in this project?

Because no separate warehouse table exists, Department Name is used as a department or warehouse utilization proxy. The project analyzes order count, quantity, and sales by department.

12. What does the fuel analysis show?

It groups vehicles by vehicle class and compares average combined fuel consumption and average CO2 emissions.

13. What is Docker?

Docker is a containerization platform that packages an application with its runtime environment so it can run consistently across machines.

14. Why dockerize the Spark application?

Docker makes the Spark job reproducible. Anyone with Docker can build the image and run the same Spark application without manually installing Spark.

15. What is a Dockerfile?

A Dockerfile is a text file containing instructions to build a Docker image, such as base image, copied files, working directory, permissions, and startup command.

16. Why use pache/spark-py:latest?

It provides a ready Spark and Python environment, which is suitable for running a PySpark application with spark-submit.

17. What is .dockerignore?

.dockerignore excludes files and folders from the Docker build context. This keeps the image build cleaner and avoids copying unnecessary files such as git metadata, notebook checkpoints, and output folders.

18. What proves Docker worked?

The logs show the image was built and visible in docker images, and the container run printed expected dataset counts, analytics section headings, ETL completion, application completion, and SparkContext shutdown with exit code 0.

19. What is Kubernetes?

Kubernetes is a container orchestration platform that manages deployment, scaling, networking, and lifecycle of containerized applications.

20. What is a Kubernetes Deployment?

A Deployment manages pod replicas and keeps the desired application state running. If a pod fails, the Deployment can recreate it.

21. What is a Kubernetes Service?

A Service provides stable networking access to pods. In this project, a NodePort Service exposes port 8080 for the Spark output server.

22. Why use imagePullPolicy: Never?

It tells Kubernetes not to pull the image from a remote registry. This is useful for local clusters when the image is built locally as smart-logistics-spark:latest.

23. What was the Kubernetes blocker?

kubectl was installed, but no Kubernetes context existed. Without Docker Desktop Kubernetes or Minikube running, kubectl could not contact an API server, so deployment and dry-run validation were blocked.

24. What is CI/CD?

CI/CD means Continuous Integration and Continuous Delivery or Deployment. It automates checks such as building, testing, and validating project artifacts.

25. Why use GitHub Actions?

GitHub Actions runs validation automatically when code is pushed or a pull request is opened, reducing manual verification mistakes.

26. What does the workflow validate?

It validates required files, builds the Docker image, runs the Docker image, and attempts Kubernetes manifest validation.

27. What are project limitations?

There is no separate warehouse master table, no GPS tracking dataset, and no common vehicle identifier to join shipments with fuel records. Kubernetes deployment also requires a local cluster that was not configured during this verification.

28. What improvements can be made?

Possible improvements include adding real warehouse and GPS datasets, publishing the Docker image to a registry, enabling Kubernetes in Docker Desktop or Minikube, using OneHotEncoder for categorical ML features, and adding automated unit tests.

29. Why did you avoid fake joins or fake screenshots?

The project must be technically honest. Creating fake joins, fake screenshots, or fabricated deployment results would make the evidence unreliable.

30. How would you deploy after enabling Kubernetes?

Enable Docker Desktop Kubernetes or start Minikube, make sure the image is available to the cluster, then run kubectl apply -f k8s/deployment.yaml and kubectl apply -f k8s/service.yaml, followed by rollout and pod log checks.

## 10. Commands I Ran

Docker build:

`ash
docker build --no-cache -t smart-logistics-spark:latest .
`

Docker run:

`ash
docker run --rm smart-logistics-spark:latest
`

Kubernetes client checks:

`ash
kubectl version --client
kubectl config get-contexts
`

Kubernetes dry-run attempts:

`ash
kubectl apply --dry-run=client -f k8s\deployment.yaml
kubectl apply --dry-run=client -f k8s\service.yaml
`

Kubernetes deployment commands to run after enabling a local cluster:

`ash
kubectl config use-context docker-desktop
kubectl get nodes
kubectl apply -f k8s\deployment.yaml
kubectl apply -f k8s\service.yaml
kubectl rollout status deployment/logistics-spark --timeout=300s
kubectl get pods -o wide
kubectl get svc
kubectl logs deployment/logistics-spark --tail=200
`

Git commands for final delivery:

`ash
git status
git add app/spark_app.py Dockerfile .dockerignore k8s/deployment.yaml k8s/service.yaml .github/workflows/ci-cd.yml README.md PERSONAL_VIVA_DOCUMENTATION.md FINAL_VERIFICATION_REPORT.md screenshots_outputs/*.log
git commit -m "verify docker kubernetes cicd deliverables and add viva documentation"
git push
`

## 11. Final Status

| Item | Status | Evidence |
| --- | --- | --- |
| Notebook Q1-Q7 | Completed based on existing notebook and README coverage | AI_Compute_CaseStudy1.ipynb, README.md |
| Dockerfile | Completed | Dockerfile, screenshots_outputs/05_dockerfile_content.log |
| Docker build | Passed | screenshots_outputs/18_docker_build.log, screenshots_outputs/19_docker_images.log |
| Docker run | Passed | screenshots_outputs/21_docker_run.log, screenshots_outputs/22_docker_run_key_lines.log |
| Kubernetes YAML | Completed | k8s/deployment.yaml, k8s/service.yaml |
| Kubernetes dry-run | Blocked by missing Kubernetes API server/context | screenshots_outputs/25_k8s_deployment_dry_run.log, screenshots_outputs/26_k8s_service_dry_run.log |
| Kubernetes deployment | Blocked by missing Kubernetes context | screenshots_outputs/24_kubernetes_contexts.log, screenshots_outputs/27_kubernetes_status.log |
| CI/CD workflow | Completed | .github/workflows/ci-cd.yml |
| Git commit | Pending at time of writing this document | Final terminal output after commit |
| Git push | Pending at time of writing this document | Final terminal output after push |
