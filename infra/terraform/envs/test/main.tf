module "cloud_validation_env" {
  source = "../../modules/cloud_validation_env"

  project_name         = "nyc-data-platform"
  environment_name     = "test"
  aws_region           = "eu-west-1"
  availability_zones   = ["eu-west-1a", "eu-west-1b"]
  vpc_cidr             = "10.20.0.0/16"
  private_subnet_cidrs = ["10.20.10.0/24", "10.20.20.0/24"]
  public_subnet_cidrs  = ["10.20.110.0/24", "10.20.120.0/24"]
  data_bucket_name     = "nyc-data-platform-test"
  artifact_bucket_name = "nyc-data-platform-test-artifacts"
  container_image_uri  = var.container_image_uri
  audit_db_password    = var.audit_db_password
}
