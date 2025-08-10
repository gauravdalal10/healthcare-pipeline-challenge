-- 1. Main facilities table (flattened location data)
CREATE TABLE healthcare_db_medlaunch.facilities 
WITH (
    external_location = 's3://healthcare-data-gd-1234/normalized-tables/facilities/',
    format = 'PARQUET'
) AS
SELECT 
    facility_id,
    facility_name,
    location.address as address,
    location.city as city,
    location.state as state,
    location.zip as zip,
    employee_count
FROM healthcare_data_gd_1234
WHERE facility_id IS NOT NULL 
  AND facility_id != '';

-- 2. Facility services (one row per facility-service combination)
CREATE TABLE healthcare_db_medlaunch.facility_services
WITH (
    external_location = 's3://healthcare-data-gd-1234/normalized-tables/facility_services/',
    format = 'PARQUET'
) AS
SELECT 
    facility_id,
    facility_name,
    service
FROM healthcare_data_gd_1234
CROSS JOIN UNNEST(services) AS t(service)
WHERE facility_id IS NOT NULL 
  AND facility_id != '';

-- 3. Facility labs (one row per facility-lab combination)
CREATE TABLE healthcare_db_medlaunch.facility_labs
WITH (
    external_location = 's3://healthcare-data-gd-1234/normalized-tables/facility_labs/',
    format = 'PARQUET'
) AS
SELECT 
    facility_id,
    facility_name,
    lab.lab_name
FROM healthcare_data_gd_1234
CROSS JOIN UNNEST(labs) AS t(lab)
WHERE facility_id IS NOT NULL 
  AND facility_id != '';

-- 4. Lab certifications (double UNNEST for nested arrays)
CREATE TABLE healthcare_db_medlaunch.lab_certifications
WITH (
    external_location = 's3://healthcare-data-gd-1234/normalized-tables/lab_certifications/',
    format = 'PARQUET'
) AS
SELECT 
    facility_id,
    facility_name,
    lab.lab_name,
    certification
FROM healthcare_data_gd_1234
CROSS JOIN UNNEST(labs) AS lab_table(lab)
CROSS JOIN UNNEST(lab.certifications) AS cert_table(certification)
WHERE facility_id IS NOT NULL 
  AND facility_id != '';

-- 5. Facility accreditations (one row per facility-accreditation combination)
CREATE TABLE healthcare_db_medlaunch.facility_accreditations
WITH (
    external_location = 's3://healthcare-data-gd-1234/normalized-tables/facility_accreditations/',
    format = 'PARQUET'
) AS
SELECT 
    facility_id,
    facility_name,
    accred.accreditation_body,
    accred.accreditation_id,
    accred.valid_until
FROM healthcare_data_gd_1234
CROSS JOIN UNNEST(accreditations) AS t(accred)
WHERE facility_id IS NOT NULL 
  AND facility_id != '';

-- Verification: Check row counts for all normalized tables
SELECT 'facilities' as table_name, COUNT(*) as row_count FROM facilities
UNION ALL
SELECT 'facility_services', COUNT(*) FROM facility_services
UNION ALL
SELECT 'facility_labs', COUNT(*) FROM facility_labs
UNION ALL
SELECT 'lab_certifications', COUNT(*) FROM lab_certifications
UNION ALL
SELECT 'facility_accreditations', COUNT(*) FROM facility_accreditations;
