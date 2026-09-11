# Terraform — AWS Deployment

Minimal infrastructure for running the Data Observatory on AWS. This is the
real-cloud counterpart to the LocalStack setup used for local development.

## What it provisions

| Resource | Purpose |
|----------|---------|
| `aws_s3_bucket` (+ versioning, encryption, public-access block) | Medallion data lake (bronze/silver/gold prefixes) |
| `aws_kms_key` (+ alias) | Customer-managed key encrypting the data lake, rotation enabled |
| `aws_ecr_repository` ×2 | Registries for the pipeline and dashboard images (immutable tags) |
| `aws_db_instance` (Postgres) | Gold serving layer the dashboard reads from |

## What it does *not* provision

This is storage, a registry and a database — not a running deployment. There is
deliberately no:

- **Compute.** No ECS/Fargate service, Lambda or Batch job, so nothing here
  runs the pipeline or serves the dashboard in AWS. The images are built and
  pushed, but nothing pulls them.
- **Networking.** No VPC, subnets or security groups, so `aws_db_instance`
  lands in the account's default VPC. It is `publicly_accessible = false`, but
  reaching it still depends on whatever that default VPC looks like.
- **IAM.** No task/execution roles or least-privilege policies for the
  pipeline's S3 and RDS access.
- **Remote state.** See the note below.

Treat `terraform apply` here as "the storage layer exists", not "the platform
is deployed".

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
- Older Terraform releases fail `terraform init` with `openpgp: key expired`
  because they can't handle HashiCorp's renewed provider-signing key. CI pins
  1.15.6, which is verified to work; use it (or newer) locally if you hit
  that error.
