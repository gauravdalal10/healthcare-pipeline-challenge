# Stage 1: Data Extraction with Athena

## Overview
Extract key facility metrics from nested JSON healthcare data using AWS Athena. This stage demonstrates serverless analytics with automatic schema detection using AWS Glue Crawler and efficient handling of complex nested JSON structures through data normalization.

## Architecture Components

- **AWS Glue Crawler**: Automatically detects schema from NDJSON data in S3
- **Amazon Athena**: Serverless SQL query engine for analytics
- **Amazon S3**: Data storage for input, intermediate tables, and results
