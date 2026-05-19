variable "project_name" {
  type = string
}

variable "environment_name" {
  type = string
}

variable "aws_region" {
  type    = string
  default = "eu-west-1"
}

variable "availability_zones" {
  type = list(string)
}

variable "vpc_cidr" {
  type = string
}

variable "private_subnet_cidrs" {
  type = list(string)
}

variable "public_subnet_cidrs" {
  type = list(string)
}

variable "data_bucket_name" {
  type = string
}

variable "artifact_bucket_name" {
  type = string
}

variable "container_image_uri" {
  type = string
}

variable "mwaa_dag_s3_path" {
  type    = string
  default = "airflow/dags"
}

variable "mwaa_requirements_s3_path" {
  type    = string
  default = "airflow/mwaa-requirements.txt"
}

variable "mwaa_plugins_s3_path" {
  type    = string
  default = "airflow/plugins.zip"
}

variable "audit_db_name" {
  type    = string
  default = "pipeline_audit"
}

variable "audit_db_username" {
  type    = string
  default = "pipeline_audit"
}

variable "audit_db_password" {
  type      = string
  sensitive = true
}

variable "control_plane_report_prefix" {
  type    = string
  default = "control-plane"
}
