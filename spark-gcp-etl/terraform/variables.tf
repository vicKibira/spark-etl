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
