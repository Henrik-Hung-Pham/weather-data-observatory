variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name, used to prefix resource names."
  type        = string
  default     = "data-observatory"
}

variable "environment" {
  description = "Deployment environment (e.g. staging, production)."
  type        = string
  default     = "staging"
}

variable "data_lake_bucket_name" {
  description = "Globally unique S3 bucket name for the medallion data lake."
  type        = string
}

variable "db_instance_class" {
  description = "RDS instance class for the Postgres serving layer."
  type        = string
  default     = "db.t3.micro"
}

variable "db_allocated_storage" {
  description = "Allocated storage (GiB) for the RDS instance."
  type        = number
  default     = 20
}

variable "db_name" {
  description = "Initial database name."
  type        = string
  default     = "observatory"
}

variable "db_username" {
  description = "Master username for the Postgres serving layer."
  type        = string
  default     = "observatory"
}

variable "db_password" {
  description = "Master password for the Postgres serving layer. Provide via TF_VAR_db_password; never commit it."
  type        = string
  sensitive   = true
}
