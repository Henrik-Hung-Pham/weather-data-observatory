#!/bin/bash
# Initialize S3 bucket in LocalStack

echo "Creating S3 bucket for Data Observatory..."

awslocal s3 mb s3://data-observatory

# Create folder structure for medallion architecture
awslocal s3api put-object --bucket data-observatory --key bronze/weather/.gitkeep
awslocal s3api put-object --bucket data-observatory --key silver/weather/.gitkeep
awslocal s3api put-object --bucket data-observatory --key gold/weather/.gitkeep

echo "S3 bucket 'data-observatory' created with medallion architecture folders."
