# Project: Data Lake
This is an ETL, 'Extract, Transform, and Load', pipeline for the company Sparkify that extracts the data from S3, transforms the data in Apache Spark into a set of fact and dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.




## Table of contents
* [Significance](#Significance)
* [Files](#files)
* [Prerequisites](#prerequisites)
* [Launch](#launch)

## Significance
This move from legacy Data Warehouse to a Data Lake will give the analytics team at Sparkify faster transformation of the data, and reduce the cost of operation.


## Files
The files used to make the etl, , and the warehouse are:
* dl.cfg
	* This file stores the credentials to the IAM role, which opens the path to the json files where the data is originally stored.
* etl.py
	* This file automates the extracting, transformation, and loading into the fact and dimension tables in the data lake.


## Prerequisites
* Apache Spark
* PySpark
* AWS IAM User
* AWS IAM Role
* AWS EMR with Spark
* AWS S3


## Launch
Run the etl.py file to load and insert into the new fact and dimension tables.
