terraform {
  backend "s3" {
    bucket       = "nyc-data-platform-terraform-state"
    key          = "bootstrap/ecr/terraform.tfstate"
    region       = "eu-west-1"
    use_lockfile = true
  }
}
