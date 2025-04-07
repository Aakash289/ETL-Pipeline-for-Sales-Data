# ETL-Pipeline-for-Sales-Data

This project demonstrates a modern, containerized ETL (Extract, Transform, Load) pipeline built using Apache Airflow. It automates the process of extracting raw sales data from a CSV file, transforming it through multiple clean-up and enrichment steps, and loading the final dataset into a PostgreSQL database. The entire pipeline is orchestrated using Airflow and runs inside Docker containers for consistency, portability, and ease of deployment.

🚀 What This Project Does
Extracts raw sales data from a CSV file stored in the Airflow DAGs directory.

Transforms the data through a series of modular steps:

Converts InvoiceDate to standardized string format.

Calculates revenue by multiplying Quantity and UnitPrice.

Filters out records with invalid or missing values.

Loads the cleaned data into a PostgreSQL table named cleaned_sales.

Makes the data available for analysis or visualization using tools like DBeaver.

🛠️ Tools and Technologies Used

Tool/Tech	Purpose
Apache Airflow	Orchestrates the ETL tasks as DAGs (Directed Acyclic Graphs). Modular, scalable, and production-ready.
Docker	Containers for Airflow and PostgreSQL ensure isolated, reproducible environments.
PostgreSQL	Stores the final cleaned sales data for further analysis.
Pandas	Data manipulation during the transformation steps.
