variable "aws_region" {
  type    = string
  default = "eu-west-1"
}

variable "project_name" {
  type    = string
  default = "nyc-data-platform"
}

variable "test_repository_name" {
  type    = string
  default = "nyc-data-platform-test"
}

variable "prod_repository_name" {
  type    = string
  default = "nyc-data-platform-prod"
}
