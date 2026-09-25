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

# Versioning is on, which means every overwrite keeps the old object forever
# and nothing ever ages out. Without lifecycle rules the bill grows without
# limit, and a bucket that is append-only by design becomes append-only by
# accident.
#
# The layers age differently on purpose. Bronze is immutable raw capture --
# written once, read rarely, kept for replay -- so it moves to colder storage
# but is never deleted. Silver and Gold are derived and can be rebuilt from
# Bronze, so their superseded versions expire.
resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  # Storage that is charged for and that no completed upload will ever claim.
  # Nothing reads these; they accumulate after interrupted writes.
  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "bronze-archive"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }

    # Bronze is the replay source, so the current object is kept forever --
    # there is deliberately no expiration rule here. Only superseded versions
    # go, and a write-once object should not have many of those.
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  rule {
    id     = "silver-archive"
    status = "Enabled"

    filter {
      prefix = "silver/"
    }

    transition {
      days          = 60
      storage_class = "STANDARD_IA"
    }

    noncurrent_version_expiration {
      noncurrent_days = 14
    }
  }

  rule {
    id     = "gold-expire-old-versions"
    status = "Enabled"

    filter {
      prefix = "gold/"
    }

    noncurrent_version_expiration {
      noncurrent_days = 14
    }
  }

  # Quarantine is a dead-letter queue, not an archive. If nobody has looked at
  # a rejected record in 90 days, nobody is going to.
  rule {
    id     = "quarantine-expire"
    status = "Enabled"

    filter {
      prefix = "quarantine/"
    }

    expiration {
      days = 90
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }

  depends_on = [aws_s3_bucket_versioning.data_lake]
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

# Tags are immutable, so every build pushes a new image and none are ever
# replaced. deploy.yml pushes on every commit to main; without expiry these
# repositories grow without limit.
#
# Untagged images go first -- they are layers orphaned by a re-push,
# referenced by nothing. Tagged images are kept generously: a rollback target
# is worth more than the storage it costs.
locals {
  ecr_lifecycle_policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire untagged images after 14 days"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 14
        }
        action = {
          type = "expire"
        }
      },
      {
        rulePriority = 2
        description  = "Keep the 30 most recent images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 30
        }
        action = {
          type = "expire"
        }
      },
    ]
  })
}

resource "aws_ecr_lifecycle_policy" "pipeline" {
  repository = aws_ecr_repository.pipeline.name
  policy     = local.ecr_lifecycle_policy
}

resource "aws_ecr_lifecycle_policy" "dashboard" {
  repository = aws_ecr_repository.dashboard.name
  policy     = local.ecr_lifecycle_policy
}

# ---------------------------------------------------------------------------
# Network placement for the serving layer.
#
# The instance used to land wherever the account default VPC put it, and
# publicly_accessible = false was the only thing between the database and the
# internet. A default VPC's default security group allows all traffic from
# anything else carrying that group, so "not public" was not the same as
# "only the pipeline and the dashboard can reach it".
#
# vpc_id and private_subnet_ids are required inputs rather than resources
# created here: which VPC a database belongs in is an account-level decision,
# and a module that invents its own VPC per environment is how an account ends
# up with eleven of them.
# ---------------------------------------------------------------------------
resource "aws_db_subnet_group" "serving" {
  name        = "${local.name_prefix}-serving"
  description = "Private subnets for the ${local.name_prefix} serving layer"
  subnet_ids  = var.private_subnet_ids
}

resource "aws_security_group" "serving" {
  name        = "${local.name_prefix}-serving"
  description = "Postgres access for the ${local.name_prefix} serving layer"
  vpc_id      = var.vpc_id

  # No egress rules: a database initiates nothing, and any outbound path is a
  # path for data to leave.

  lifecycle {
    create_before_destroy = true
  }
}

# A standalone rule rather than an inline ingress block. Inline blocks are
# authoritative for the whole group, so adding a second source later silently
# revokes the first unless every source is declared in the same place.
resource "aws_vpc_security_group_ingress_rule" "serving_postgres" {
  for_each = toset(var.allowed_postgres_cidr_blocks)

  security_group_id = aws_security_group.serving.id
  description       = "Postgres from ${each.value}"
  cidr_ipv4         = each.value
  from_port         = 5432
  to_port           = 5432
  ip_protocol       = "tcp"
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

  # The master password is generated and rotated by AWS in Secrets Manager
  # instead of being passed in. A `password` input is written to the state
  # file in plaintext, so every holder of the state -- and every backup of it
  # -- holds the database credentials. Read it from the ARN exported as
  # serving_db_master_secret_arn.
  manage_master_user_password = true

  db_subnet_group_name   = aws_db_subnet_group.serving.name
  vpc_security_group_ids = [aws_security_group.serving.id]
  publicly_accessible    = false

  multi_az                   = var.db_multi_az
  backup_retention_period    = var.db_backup_retention_days
  copy_tags_to_snapshot      = true
  auto_minor_version_upgrade = true

  # A `terraform destroy` pointed at the wrong workspace should not be able to
  # take the serving layer with it, and anything that does delete this
  # instance should leave a snapshot behind. Both are deliberately
  # inconvenient: set db_deletion_protection = false to tear an environment
  # down on purpose.
  deletion_protection       = var.db_deletion_protection
  skip_final_snapshot       = !var.db_deletion_protection
  final_snapshot_identifier = var.db_deletion_protection ? "${local.name_prefix}-serving-final" : null

  # Postgres logs otherwise live on the instance and die with it.
  enabled_cloudwatch_logs_exports = ["postgresql", "upgrade"]

  performance_insights_enabled = var.db_performance_insights_enabled
}
