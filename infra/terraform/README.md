# Terraform — AWS Deployment

Minimal infrastructure for running the Data Observatory on AWS. This is the
real-cloud counterpart to the LocalStack setup used for local development.

## What it provisions

| Resource | Purpose |
|----------|---------|
| `aws_s3_bucket` (+ versioning, encryption, public-access block, lifecycle) | Medallion data lake (bronze/silver/gold prefixes) |
| `aws_kms_key` (+ alias) | Customer-managed key encrypting the data lake, rotation enabled |
| `aws_ecr_repository` ×2 (+ lifecycle policies) | Registries for the pipeline and dashboard images (immutable tags) |
| `aws_db_subnet_group`, `aws_security_group` (+ ingress rules) | Private network placement for the serving layer |
| `aws_db_instance` (Postgres) | Gold serving layer the dashboard reads from |

## What it does *not* provision

This is storage, a registry and a database — not a running deployment. There is
deliberately no:

- **Compute.** No ECS/Fargate service, Lambda or Batch job, so nothing here
  runs the pipeline or serves the dashboard in AWS. The images are built and
  pushed, but nothing pulls them.
- **VPC.** The subnets and VPC are *inputs*, not resources. Which VPC a
  database belongs in is an account-level decision, and a module that invents
  its own VPC per environment is how an account ends up with eleven of them.
- **IAM.** No task/execution roles or least-privilege policies for the
  pipeline's S3 and RDS access.
- **The state bucket.** A backend cannot store the state that describes
  itself; create it once out-of-band (see below).

Treat `terraform apply` here as "the storage layer exists", not "the platform
is deployed".

## Usage

```bash
cd infra/terraform

cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars: bucket name, vpc_id, private_subnet_ids,
# allowed_postgres_cidr_blocks

cp backend.hcl.example backend.hcl
# edit backend.hcl: your state bucket and a per-environment key

terraform init -backend-config=backend.hcl
terraform plan
terraform apply
```

There is no `TF_VAR_db_password` step any more — see below.

## Remote state

State lives in S3, configured *partially* in `versions.tf`: the bucket and key
differ per environment and are passed at init time via `backend.hcl`, which is
gitignored.

Locking uses S3 natively (`use_lockfile = true`, Terraform ≥ 1.10) rather than
a DynamoDB table. The table approach is deprecated as of 1.11 and required a
second resource to exist before the backend would work at all.

Create the state bucket once, by hand or in a separate bootstrap
configuration, **with versioning enabled** — versioning is what lets you
recover from a corrupted or truncated state push.

## The database credential

There is no `db_password` variable. The instance sets
`manage_master_user_password = true`, so AWS generates and rotates the master
credential in Secrets Manager.

This is not a convenience: a `password` input is written into the Terraform
state in plaintext, so every holder of the state — and every backup of it —
held the database credentials. Read it from the ARN in the
`serving_db_master_secret_arn` output instead.

## Defaults that assume production

| Setting | Default | Why |
|---------|---------|-----|
| `db_deletion_protection` | `true` | A `destroy` against the wrong workspace can't take the serving layer with it. Also drives `skip_final_snapshot` — with protection on, deleting the instance leaves a final snapshot. |
| `db_backup_retention_days` | `7` | Validated to be ≥ 1; `0` silently disables automated backups. |
| `db_multi_az` | `false` | Roughly doubles the cost. Turn it on for production. |
| `allowed_postgres_cidr_blocks` | *(required)* | Validated to reject `0.0.0.0/0`. |

Set `db_deletion_protection = false` to tear an environment down on purpose.

## Lifecycle rules

Versioning without expiry means nothing ever ages out and the bill grows
without limit. The layers age differently on purpose:

- **Bronze** → Standard-IA at 30 days, Glacier-IR at 90. The current object is
  **never deleted** — it is the replay source. Only superseded versions go.
- **Silver** → Standard-IA at 60 days; superseded versions expire at 14.
- **Gold** → superseded versions expire at 14 days.
- **Quarantine** → deleted at 90 days. It is a dead-letter queue, not an
  archive.
- Incomplete multipart uploads are aborted after 7 days.

ECR keeps the 30 most recent images and expires untagged ones after 14 days.

## Notes

- The `validate-infrastructure` job in `.github/workflows/deploy.yml` runs
  `terraform fmt -check` and `terraform validate` against this directory. It
  does **not** run `terraform plan`: a plan needs real AWS credentials, which
  would mean an OIDC role in an account this repository does not have.
- Terraform misconfiguration is scanned by Trivy in the `Security` workflow
  (`scanners: vuln,secret,misconfig`), currently advisory.
- Older Terraform releases fail `terraform init` with `openpgp: key expired`
  because they can't handle HashiCorp's renewed provider-signing key. CI pins
  1.15.6, which is verified to work; use it (or newer) locally if you hit
  that error.
