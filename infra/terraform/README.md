# Terraform — AWS Deployment

Minimal infrastructure for running the Data Observatory on AWS. This is the
real-cloud counterpart to the LocalStack setup used for local development.

## What it provisions

| Resource | Purpose |
|----------|---------|
| `aws_s3_bucket` (+ versioning, encryption, public-access block) | Medallion data lake (bronze/silver/gold prefixes) |
| `aws_ecr_repository` ×2 | Registries for the pipeline and dashboard images |
| `aws_db_instance` (Postgres) | Gold serving layer the dashboard reads from |

## Usage

```bash
cd infra/terraform

cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars: set a globally-unique data_lake_bucket_name

export TF_VAR_db_password="$(openssl rand -base64 24)"   # never commit this

terraform init
terraform plan
terraform apply
```

## Notes

- `db_password` is marked `sensitive` and has no default — supply it via the
  `TF_VAR_db_password` environment variable.
- No remote backend is configured; add an S3 backend block before using this
  for anything shared/long-lived.
- The `validate-infrastructure` job in `.github/workflows/deploy.yml` runs
  `terraform fmt -check` and `terraform validate` against this directory.
