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
data "google_storage_bucket" "datalake" {
  name = var.bucket_name
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
  bucket = data.google_storage_bucket.datalake.name
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
