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

# db_password is gone on purpose. The instance now uses
# manage_master_user_password, so AWS generates and rotates the credential in
# Secrets Manager and it never enters the Terraform state.

variable "vpc_id" {
  description = "VPC the serving layer's security group belongs to."
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for the serving layer's DB subnet group. At least two, in different availability zones."
  type        = list(string)

  validation {
    condition     = length(var.private_subnet_ids) >= 2
    error_message = "RDS requires subnets in at least two availability zones, even for a single-AZ instance."
  }
}

variable "allowed_postgres_cidr_blocks" {
  description = "CIDR blocks allowed to reach Postgres on 5432. Scope this to the subnets running the pipeline and dashboard."
  type        = list(string)

  validation {
    condition     = !contains(var.allowed_postgres_cidr_blocks, "0.0.0.0/0")
    error_message = "0.0.0.0/0 would expose the serving layer to the whole VPC peering surface; list the workload subnets instead."
  }
}

variable "db_multi_az" {
  description = "Run the serving layer across two availability zones. Costs roughly double; leave false outside production."
  type        = bool
  default     = false
}

variable "db_backup_retention_days" {
  description = "Days of automated backups to retain (0 disables them)."
  type        = number
  default     = 7

  validation {
    condition     = var.db_backup_retention_days >= 1
    error_message = "Backups must be retained for at least one day; 0 disables automated backups entirely."
  }
}

variable "db_deletion_protection" {
  description = "Refuse to delete the instance, and take a final snapshot when it is deleted. Set false only to tear an environment down deliberately."
  type        = bool
  default     = true
}

variable "db_performance_insights_enabled" {
  description = "Enable RDS Performance Insights. Free for 7 days of retention on supported classes; not supported on db.t3.micro."
  type        = bool
  default     = false
}
