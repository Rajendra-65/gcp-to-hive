CREATE DATABASE IF NOT EXISTS airflow_assignment2;

USE airflow_assignment2;

CREATE EXTERNAL TABLE IF NOT EXISTS products (
  product_id STRING,
  product_name STRING,
  category STRING,
  price DOUBLE,
  stock_quantity INT,
  sales ARRAY<STRUCT<
    sale_id: STRING,
    date: STRING,
    quantity_sold: INT,
    revenue: DOUBLE,
    customer_rating: DOUBLE
  >>
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/home/debasiskhuntia27/product_${hiveconf:date}.json'
TBLPROPERTIES ("skip.header.line.count"="1");
