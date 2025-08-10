# Healthcare Pipeline Challenge - Stages 1 & 2

## Overview
This repository contains the implementation of Stage 1 (Data Extraction with Athena) and Stage 2 (Data Processing with Python) of the healthcare pipeline challenge.

## Stages Implemented

### Stage 1: Data Extraction with Athena
- Extract key facility metrics from nested JSON data in S3
- Create normalized tables from complex nested structures
- Generate facility metrics with UNNEST operations
- Save results to S3 in Parquet format

### Stage 2: Data Processing with Python
- Filter facilities with soon-to-expire accreditations (within 6 months)
- Process NDJSON data using boto3
- Implement proper error handling and logging
- Save filtered results to a separate S3 location

## Tools Used:
- **AWS Glue:** Schema detection and data catalog
- **Amazon Athena:** Serverless SQL analytics
- **Amazon S3:** Data storage and results
- **Python + boto3:** Custom business logic processing
