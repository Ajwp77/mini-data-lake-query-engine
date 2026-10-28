# Mini Data Lake & Query Engine

## About the Project
This project uses AWS services to process e-commerce data and run SQL queries on it.
I used the Olist Brazilian E-Commerce dataset. The raw CSV files are stored in Amazon S3, processed using AWS Glue, and saved in Parquet format. I then use Amazon Athena to query the processed data.
I also added a Lambda function to trigger the Glue job when a new CSV file is uploaded to the raw folder.

## AWS Services Used

- **Amazon S3** – Stores raw and processed data
- **AWS Glue** – Cleans and transforms the data
- **AWS Glue Data Catalog** – Stores table information
- **Amazon Athena** – Runs SQL queries on the processed data
- **AWS Lambda** – Triggers the Glue job when a CSV file is uploaded
- **Amazon CloudWatch** – Helps check logs and debug errors

## How It Works

1. Upload the raw CSV files to the S3 bucket.
2. AWS Glue reads the required files, joins the data, and performs transformations.
3. The processed data is stored in Parquet format, partitioned by year and month.
4. The Glue Data Catalog stores the table schema.
5. Athena is used to run queries on the processed data.
6. Lambda starts the Glue job when a new CSV file is uploaded to the `raw/` folder.

The current setup reruns the ETL job on the dataset. It does not yet support incremental processing.

## Sample Queries

The `sql/` folder contains queries for:

- Revenue by state
- Monthly revenue
- Top product categories by revenue
- Basic order and revenue metrics

## Dataset

[Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

## What I Learned

- Working with S3 buckets and folders
- Using AWS Glue with PySpark for ETL
- Saving data in Parquet format and using partitions
- Querying data with Athena
- Triggering AWS jobs using Lambda
- Debugging issues using CloudWatch logs
