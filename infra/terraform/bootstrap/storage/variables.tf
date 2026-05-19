variable "aws_region" {
  type    = string
  default = "eu-west-1"
}

variable "project_name" {
  type    = string
  default = "nyc-data-platform"
}

variable "test_data_bucket_name" {
  type    = string
  default = "nyc-data-platform-test"
}

variable "test_artifact_bucket_name" {
  type    = string
  default = "nyc-data-platform-test-artifacts"
}

variable "prod_data_bucket_name" {
  type    = string
  default = "nyc-data-platform-prod"
}

variable "prod_artifact_bucket_name" {
  type    = string
  default = "nyc-data-platform-prod-artifacts"
}
