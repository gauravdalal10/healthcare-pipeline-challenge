#!/usr/bin/env python3
"""
Stage 2: Simple script to filter facilities with expiring accreditations
"""

import json
import boto3
from datetime import datetime, timedelta

# Configuration
SOURCE_BUCKET = "healthcare-data-gd-1234"
SOURCE_KEY = "healthcare_facilities.ndjson"
TARGET_BUCKET = "healthcare-results-gd-1234"
TARGET_KEY = "athena-results/stage-2-results/expiring_accreditations.ndjson"

def main():
    s3 = boto3.client('s3')
    
    # Calculate date range for "soon to expire" (between now and 6 months from now)
    today = datetime.now()
    cutoff_date = today + timedelta(days=180)
    print(f"Finding accreditations expiring between {today.strftime('%Y-%m-%d')} and {cutoff_date.strftime('%Y-%m-%d')}")
    
    try:
        # 1. Read NDJSON from S3
        print(f"Reading from s3://{SOURCE_BUCKET}/{SOURCE_KEY}")
        response = s3.get_object(Bucket=SOURCE_BUCKET, Key=SOURCE_KEY)
        content = response['Body'].read().decode('utf-8')
        
        # Parse each line as JSON
        facilities = []
        for line in content.strip().split('\n'):
            if line.strip():
                facilities.append(json.loads(line))
        
        print(f"Loaded {len(facilities)} facilities")
        
        # 2. Filter facilities with soon-to-expire accreditations (not already expired)
        expiring_facilities = []
        
        for facility in facilities:
            has_expiring = False
            
            # Check each accreditation
            for accred in facility.get('accreditations', []):
                expiry_date = datetime.strptime(accred['valid_until'], '%Y-%m-%d')
                
                # Only include if expiring SOON (between today and 6 months from now)
                if today <= expiry_date <= cutoff_date:
                    has_expiring = True
                    break
            
            if has_expiring:
                expiring_facilities.append(facility)
                print(f"Found expiring: {facility['facility_id']} - {facility['facility_name']}")
        
        print(f"Found {len(expiring_facilities)} facilities with expiring accreditations")
        
        # 3. Write filtered results to S3
        if expiring_facilities:
            # Convert back to NDJSON
            ndjson_content = '\n'.join([json.dumps(facility) for facility in expiring_facilities])
            
            s3.put_object(
                Bucket=TARGET_BUCKET,
                Key=TARGET_KEY,
                Body=ndjson_content.encode('utf-8')
            )
            
            print(f"Saved results to s3://{TARGET_BUCKET}/{TARGET_KEY}")
        else:
            print("No facilities with expiring accreditations found")
        
        print("Processing completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()
