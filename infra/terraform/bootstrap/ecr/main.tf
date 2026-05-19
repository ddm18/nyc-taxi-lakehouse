locals {
  common_tags = {
    Project   = var.project_name
    ManagedBy = "terraform"
    Scope     = "bootstrap"
  }
}

module "test_repository" {
  source = "../../modules/ecr_repository"

  name = var.test_repository_name
  tags = merge(local.common_tags, {
    Environment = "test"
  })
}

module "prod_repository" {
  source = "../../modules/ecr_repository"

  name = var.prod_repository_name
  tags = merge(local.common_tags, {
    Environment = "prod"
  })
}
