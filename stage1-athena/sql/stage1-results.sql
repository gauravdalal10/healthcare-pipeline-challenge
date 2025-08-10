

CREATE TABLE healthcare_db_medlaunch.stage1_facility_metrics
WITH (
    external_location = 's3://healthcare-results-gd-1234/athena-results/stage-1-results/',
    format = 'PARQUET'
) AS
SELECT 
    f.facility_id,
    f.facility_name,
    f.employee_count,
    COALESCE(service_counts.number_of_offered_services, 0) as number_of_offered_services,
    first_accred.expiry_date_of_first_accreditation
FROM facilities f
LEFT JOIN (
    -- Count services per facility
    SELECT 
        facility_id,
        COUNT(*) as number_of_offered_services
    FROM facility_services
    GROUP BY facility_id
) service_counts ON f.facility_id = service_counts.facility_id
LEFT JOIN (
    SELECT 
        facility_id,
        MIN(valid_until) as expiry_date_of_first_accreditation
    FROM facility_accreditations
    GROUP BY facility_id
) first_accred ON f.facility_id = first_accred.facility_id
ORDER BY f.facility_id;
