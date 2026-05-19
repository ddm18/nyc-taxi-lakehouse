variable "container_image_uri" {
  type    = string
  default = "111111111111.dkr.ecr.eu-west-1.amazonaws.com/nyc-data-platform-test@sha256:replace-me"
}

variable "audit_db_password" {
  type      = string
  sensitive = true
  default   = "replace-me-before-apply"
}

