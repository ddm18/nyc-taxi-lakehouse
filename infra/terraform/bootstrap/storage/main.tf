locals {
  common_tags = {
    Project   = var.project_name
    ManagedBy = "terraform"
    Scope     = "bootstrap"
  }
}

module "test_data_bucket" {
  source = "../../modules/platform_bucket"

  bucket_name       = var.test_data_bucket_name
  enable_versioning = true
  expiration_days   = 0
  tags = merge(local.common_tags, {
    Environment = "test"
    BucketRole  = "data"
  })
}

module "test_artifact_bucket" {
  source = "../../modules/platform_bucket"

  bucket_name       = var.test_artifact_bucket_name
  enable_versioning = true
  expiration_days   = 0
  tags = merge(local.common_tags, {
    Environment = "test"
    BucketRole  = "artifact"
  })
}

module "prod_data_bucket" {
  source = "../../modules/platform_bucket"

  bucket_name       = var.prod_data_bucket_name
  enable_versioning = true
  expiration_days   = 0
  tags = merge(local.common_tags, {
    Environment = "prod"
    BucketRole  = "data"
  })
}

module "prod_artifact_bucket" {
  source = "../../modules/platform_bucket"

  bucket_name       = var.prod_artifact_bucket_name
  enable_versioning = true
  expiration_days   = 0
  tags = merge(local.common_tags, {
    Environment = "prod"
    BucketRole  = "artifact"
  })
}
