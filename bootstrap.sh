#!/usr/bin/env bash
# bootstrap.sh - scaffolds the whole Spark project:
#   * local Spark cluster in Docker (custom image with GCS connector baked in)
#   * Terraform for GCS bucket + service account + Dataproc cluster
#   * run.sh: one CLI for the whole workflow (local run, terraform, GCP run)
#
# Usage:  ./bootstrap.sh /path/to/pizza_sales_etl.py
#         PROJECT_DIR=my-dir ./bootstrap.sh /path/to/pizza_sales_etl.py
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-spark-gcp-etl}"
ETL_SRC="${1:-}"

if [ -e "$PROJECT_DIR" ]; then
  echo "ERROR: '$PROJECT_DIR' already exists. Remove it or set PROJECT_DIR=<other>." >&2
  exit 1
fi

mkdir -p "$PROJECT_DIR"/{apps,data,keys,terraform}
cd "$PROJECT_DIR"
chmod 777 data   # the 'spark' user inside the container must be able to write here

# ---------------------------------------------------------------- .env
cat > .env <<'EOF'
# Edit these four values, then everything else just works.
PROJECT_ID=your-gcp-project-id
REGION=europe-west1
BUCKET=your-gcp-project-id-datalake
CLUSTER_NAME=pizza-etl
EOF

cat > .gitignore <<'EOF'
.env
keys/
data/
terraform/.terraform/
terraform/*.tfstate
terraform/*.tfstate.*
EOF

# ---------------------------------------------------------------- Docker image
cat > Dockerfile <<'EOF'
FROM apache/spark:4.0.0-python3

USER root

# GCS connector so gs:// paths work out of the box
ADD https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/gcs-connector-hadoop3-latest.jar

# Python deps for your jobs
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Default Spark + GCS config for every container
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

RUN mkdir -p /opt/apps /opt/data /opt/keys && chown -R spark:spark /opt/apps /opt/data

USER spark
WORKDIR /opt/apps
EOF

cat > spark-defaults.conf <<'EOF'
spark.master                                                 spark://spark-master:7077
spark.hadoop.fs.gs.impl                                      com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
spark.hadoop.fs.AbstractFileSystem.gs.impl                   com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS
spark.hadoop.google.cloud.auth.service.account.enable        true
spark.hadoop.google.cloud.auth.service.account.json.keyfile  /opt/keys/sa.json
spark.driver.memory                                          1g
spark.executor.memory                                        1g
spark.sql.adaptive.enabled                                   true
EOF

cat > requirements.txt <<'EOF'
pandas
pyarrow
EOF

cat > docker-compose.yml <<'EOF'
x-spark-common: &spark-common
  build: .
  image: my-spark:4.0.0
  volumes:
    - ./apps:/opt/apps
    - ./data:/opt/data
    - ./keys:/opt/keys:ro

services:
  spark-master:
    <<: *spark-common
    container_name: spark-master
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
    environment:
      - SPARK_MASTER_HOST=spark-master
    ports:
      - "8080:8080"   # master UI
      - "7077:7077"   # cluster port
      - "4040:4040"   # live job UI (driver)

  spark-worker:
    <<: *spark-common
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
    environment:
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=2g
    depends_on:
      - spark-master
    deploy:
      replicas: 2
EOF

# ---------------------------------------------------------------- Terraform
cat > terraform/main.tf <<'EOF'
terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_project_service" "apis" {
  for_each = toset([
    "dataproc.googleapis.com",
    "compute.googleapis.com",
    "storage.googleapis.com",
  ])
  service            = each.key
  disable_on_destroy = false
}

# Data lake bucket: raw/ (input CSV), jobs/ (the ETL script), output/ (Parquet)
resource "google_storage_bucket" "datalake" {
  name                        = var.bucket_name
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = var.force_destroy
  depends_on                  = [google_project_service.apis]
}

# Dedicated service account for the cluster VMs (no keys needed on GCP)
resource "google_service_account" "dataproc" {
  account_id   = "dataproc-etl"
  display_name = "Dataproc ETL cluster"
  depends_on   = [google_project_service.apis]
}

resource "google_project_iam_member" "dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.dataproc.email}"
}

resource "google_storage_bucket_iam_member" "datalake_rw" {
  bucket = google_storage_bucket.datalake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataproc.email}"
}

# Single-node Dataproc cluster (master only, zero workers)
resource "google_dataproc_cluster" "etl" {
  name   = var.cluster_name
  region = var.region

  cluster_config {
    software_config {
      image_version = var.image_version
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }

    gce_cluster_config {
      service_account        = google_service_account.dataproc.email
      service_account_scopes = ["cloud-platform"]
    }

    master_config {
      num_instances = 1
      machine_type  = var.machine_type
      disk_config {
        boot_disk_type    = "pd-balanced"
        boot_disk_size_gb = 100
      }
    }

    # Auto-delete when idle so you don't pay for a forgotten cluster
    lifecycle_config {
      idle_delete_ttl = var.idle_delete_ttl
    }
  }

  depends_on = [
    google_project_iam_member.dataproc_worker,
    google_storage_bucket_iam_member.datalake_rw,
  ]
}
EOF

cat > terraform/variables.tf <<'EOF'
variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "europe-west1"
}

variable "bucket_name" {
  type = string
}

variable "cluster_name" {
  type    = string
  default = "pizza-etl"
}

variable "machine_type" {
  type    = string
  default = "n2-standard-4"
}

variable "image_version" {
  type    = string
  default = "2.2-debian12"
}

variable "idle_delete_ttl" {
  type    = string
  default = "1800s"
}

variable "force_destroy" {
  description = "Allow terraform destroy to delete the bucket even if it has objects"
  type        = bool
  default     = false
}
EOF

cat > terraform/outputs.tf <<'EOF'
output "bucket" {
  value = google_storage_bucket.datalake.name
}

output "cluster" {
  value = google_dataproc_cluster.etl.name
}

output "service_account" {
  value = google_service_account.dataproc.email
}
EOF

# ---------------------------------------------------------------- workflow CLI
cat > run.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
source .env

export TF_VAR_project_id="$PROJECT_ID"
export TF_VAR_region="$REGION"
export TF_VAR_bucket_name="$BUCKET"
export TF_VAR_cluster_name="$CLUSTER_NAME"

JOB=pizza_sales_etl.py
SUBMIT=/opt/spark/bin/spark-submit

case "${1:-help}" in
  # ---- Phase 1: local Docker cluster
  up)         docker compose up -d --build ;;
  down)       docker compose down ;;
  logs)       docker compose logs -f ;;
  local)      docker exec -it spark-master $SUBMIT /opt/apps/$JOB \
                /opt/data/pizza_sales.csv /opt/data/output/pizza_sales 8 ;;
  local-gcs)  docker exec -it spark-master $SUBMIT /opt/apps/$JOB \
                "gs://$BUCKET/raw/pizza_sales.csv" "gs://$BUCKET/output/pizza_sales_local" 8 ;;

  # ---- Phase 2: provision GCP with Terraform
  tf-init)    terraform -chdir=terraform init ;;
  tf-plan)    terraform -chdir=terraform plan ;;
  tf-apply)   terraform -chdir=terraform apply ;;
  tf-destroy) terraform -chdir=terraform destroy ;;

  # ---- Phase 3: run the same script on Dataproc
  upload)     gcloud storage cp data/pizza_sales.csv "gs://$BUCKET/raw/pizza_sales.csv"
              gcloud storage cp apps/$JOB "gs://$BUCKET/jobs/$JOB" ;;
  gcp)        gcloud dataproc jobs submit pyspark "gs://$BUCKET/jobs/$JOB" \
                --project="$PROJECT_ID" --region="$REGION" --cluster="$CLUSTER_NAME" \
                -- "gs://$BUCKET/raw/pizza_sales.csv" "gs://$BUCKET/output/pizza_sales" 16 ;;
  deploy)     "$0" upload && "$0" gcp ;;

  *)
    cat <<'USAGE'
Usage: ./run.sh <command>

  Local (Docker)           GCP (Terraform + Dataproc)
  up         build+start   tf-init / tf-plan / tf-apply / tf-destroy
  down       stop          upload   copy CSV + ETL script to the bucket
  local      run ETL       gcp      run ETL on Dataproc
  local-gcs  run ETL on    deploy   upload + gcp
             gs:// paths
USAGE
    ;;
esac
EOF
chmod +x run.sh

# ---------------------------------------------------------------- ETL script
# Make the script take its paths from the command line so the SAME file runs
# locally and on Dataproc:  etl.py <source> <output> [shuffle_partitions]
if [ -n "$ETL_SRC" ] && [ -f "$ETL_SRC" ]; then
  cp "$ETL_SRC" apps/pizza_sales_etl.py
  sed -i 's/^import logging$/import logging\nimport sys/' apps/pizza_sales_etl.py
  sed -i -E 's|^SOURCE_PATH = .*|SOURCE_PATH = sys.argv[1] if len(sys.argv) > 1 else "/opt/data/pizza_sales.csv"|' apps/pizza_sales_etl.py
  sed -i -E 's|^OUTPUT_PATH = .*|OUTPUT_PATH = sys.argv[2] if len(sys.argv) > 2 else "/opt/data/output/pizza_sales"|' apps/pizza_sales_etl.py
  sed -i -E 's|^SHUFFLE_PARTITIONS = .*|SHUFFLE_PARTITIONS = int(sys.argv[3]) if len(sys.argv) > 3 else 8|' apps/pizza_sales_etl.py
  grep -nE '^(import sys|SOURCE_PATH|OUTPUT_PATH|SHUFFLE_PARTITIONS)' apps/pizza_sales_etl.py
else
  echo "NOTE: no ETL script given. Copy it to apps/pizza_sales_etl.py yourself."
fi

cat <<EOF

Project created in: $(pwd)

Next steps:
  1. Edit .env (project id, region, bucket, cluster name)
  2. Put pizza_sales.csv in data/
  3. gcloud auth login && gcloud auth application-default login
  4. ./run.sh up && ./run.sh local          # test locally first
  5. ./run.sh tf-init && ./run.sh tf-apply   # build bucket + Dataproc
  6. ./run.sh deploy                         # run the same script on GCP
  7. ./run.sh tf-destroy                     # tear down when finished
EOF