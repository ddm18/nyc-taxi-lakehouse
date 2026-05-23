variable "container_image_uri" {
  type    = string
  default = "public.ecr.aws/docker/library/busybox:stable"
}

variable "audit_db_password" {
  type      = string
  sensitive = true
  default   = "replace-me-before-apply"
}
