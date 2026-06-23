output "data_lake_bucket" {
  description = "Name of the S3 data lake bucket."
  value       = aws_s3_bucket.data_lake.bucket
}

output "data_lake_bucket_arn" {
  description = "ARN of the S3 data lake bucket."
  value       = aws_s3_bucket.data_lake.arn
}

output "pipeline_ecr_repository_url" {
  description = "ECR repository URL for the pipeline image."
  value       = aws_ecr_repository.pipeline.repository_url
}

output "dashboard_ecr_repository_url" {
  description = "ECR repository URL for the dashboard image."
  value       = aws_ecr_repository.dashboard.repository_url
}

output "serving_db_endpoint" {
  description = "Connection endpoint for the Postgres serving layer."
  value       = aws_db_instance.serving.endpoint
}
