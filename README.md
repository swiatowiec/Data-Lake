# DATA LAKE

## Introduction

Due to the dynamic development, the startup called Sparkify collects lots of data. They want to move their processes and data onto the clound which is the most secure, accessible, fast and costeffective way to store and computute the data. 

## Goal of the project

The goal is to built an ETL pipeline that extracts their data from data lake hosted on S3, process data using Spark and load back into S3 into a set of dimensional table.

## Project structure
- dl.cfg file with AWS credentials
- etl.py reads data from S3, processes that data using Spark, and writes them back to S3
- README.md current file

## Star schema
![SCHEMA](SCHEMA.PNG)

## How to run process
1. Fill in and load credentials
2. Create an S3 Bucket 
3. Run command:
    `python etl.py`
    
