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

## Architecture:
<img width="1884" height="933" alt="Image" src="https://github.com/user-attachments/assets/040644e9-1f07-404a-a96a-0742a8823e4f" />

## Demo Link:
https://www.loom.com/share/2df829707c8943a185c41945f2e91cad?sid=e3f0a463-1996-4ef0-a308-99a9810ad46a

# Implementation Summary

## Technical Approach

My solution implements a comprehensive healthcare data pipeline using AWS serverless technologies and follows data engineering best practices. For Stage 1, I converted the original JSON array to NDJSON format for optimal Athena processing, then used AWS Glue Crawler for automatic schema detection. Rather than working with complex nested structures directly in every query, I normalized the data into five separate tables (facilities, facility_services, facility_labs, lab_certifications, facility_accreditations). This normalization strategy eliminated the need for complex UNNEST operations in subsequent queries, improved query performance, and created a more maintainable data model suitable for various analytics use cases.

The Stage 1 extraction query efficiently joins these normalized tables using standard SQL operations to produce the required facility metrics (facility_id, facility_name, employee_count, number_of_offered_services, expiry_date_of_first_accreditation) and saves results to S3 in Parquet format for optimal analytics performance. For Stage 2, I developed a focused Python script using boto3 that implements proper business logic for identifying "soon-to-expire" accreditations by filtering for dates between the current date and six months in the future, excluding already expired accreditations.

## Challenges and Solutions

The primary technical challenge was handling the Glue Crawler's tendency to create empty rows when parsing NDJSON files, which I resolved by implementing consistent null filtering across all SQL queries using `WHERE facility_id IS NOT NULL AND facility_id != ''`. The normalization approach, while requiring more initial setup than direct nested queries, proved valuable for creating a scalable foundation that supports complex analytics queries without performance penalties. Finally, ensuring proper AWS permissions across S3 buckets, Glue crawlers, and Athena required careful IAM policy configuration to enable seamless cross-service data access.
