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

output "serving_db_master_secret_arn" {
  description = "Secrets Manager ARN holding the AWS-managed master credentials. Read the password from here; it is not in the Terraform state."
  value       = aws_db_instance.serving.master_user_secret[0].secret_arn
}

output "serving_db_security_group_id" {
  description = "Security group guarding Postgres. Attach the pipeline and dashboard workloads to something this group's ingress rules allow."
  value       = aws_security_group.serving.id
}
