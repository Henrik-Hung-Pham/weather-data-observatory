terraform {
  # 1.10 is the floor for S3-native state locking (use_lockfile), which is
  # what the backend below relies on instead of a DynamoDB table.
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Local state means the only copy of "what exists in AWS" lives on one
  # laptop: nobody else can apply, a lost file orphans every resource, and two
  # people applying at once corrupt each other's work silently. It also puts
  # every value Terraform reads -- historically including the database
  # password -- in an unencrypted file next to the code.
  #
  # Deliberately a *partial* configuration. Bucket and key differ per
  # environment and must not be committed, so they are passed at init time:
  #
  #   terraform init -backend-config=backend.hcl
  #
  # See backend.hcl.example. CI runs `terraform init -backend=false`, so
  # validation never needs any of this.
  backend "s3" {
    encrypt = true

    # S3-native locking. The old DynamoDB lock table is deprecated as of
    # Terraform 1.11 and needs a second resource to exist before the backend
    # can be used at all.
    use_lockfile = true
  }
}

provider "aws" {
  region = var.aws_region

  # Tag every managed resource so cost/ownership is attributable.
  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}
