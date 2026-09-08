locals {
  name_prefix = "${var.project_name}-${var.environment}"
}

# ---------------------------------------------------------------------------
# Data lake: a single S3 bucket holding the bronze/silver/gold prefixes.
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "data_lake" {
  bucket = var.data_lake_bucket_name
}

# Customer-managed key for the data lake. SSE-S3 (AES256) leaves key policy
# and rotation entirely with AWS; a CMK lets the key be scoped, audited in
# CloudTrail, and rotated on a schedule.
resource "aws_kms_key" "data_lake" {
  description             = "${local.name_prefix} data lake encryption key"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_kms_alias" "data_lake" {
  name          = "alias/${local.name_prefix}-data-lake"
  target_key_id = aws_kms_key.data_lake.key_id
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.data_lake.arn
      sse_algorithm     = "aws:kms"
    }

    # Cuts KMS request charges by reusing one data key per prefix rather than
    # calling KMS for every object the pipeline writes.
    bucket_key_enabled = true
  }
}

# ---------------------------------------------------------------------------
# Container registries for the pipeline and dashboard images.
# ---------------------------------------------------------------------------
resource "aws_ecr_repository" "pipeline" {
  name                 = "${local.name_prefix}-pipeline"
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_repository" "dashboard" {
  name                 = "${local.name_prefix}-dashboard"
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

# ---------------------------------------------------------------------------
# Postgres serving layer (Gold). RDS instance for the dashboard to read from.
# ---------------------------------------------------------------------------
resource "aws_db_instance" "serving" {
  identifier     = "${local.name_prefix}-serving"
  engine         = "postgres"
  engine_version = "16"

  instance_class    = var.db_instance_class
  allocated_storage = var.db_allocated_storage
  storage_type      = "gp3"
  storage_encrypted = true

  db_name  = var.db_name
  username = var.db_username
  password = var.db_password

  publicly_accessible = false
  multi_az            = false
  skip_final_snapshot = true

  backup_retention_period = 7
}
