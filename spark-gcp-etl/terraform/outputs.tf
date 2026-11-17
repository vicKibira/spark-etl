output "bucket" {
  value = data.google_storage_bucket.datalake.name
}

output "cluster" {
  value = google_dataproc_cluster.etl.name
}

output "service_account" {
  value = google_service_account.dataproc.email
}
