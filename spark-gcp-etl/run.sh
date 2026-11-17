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
