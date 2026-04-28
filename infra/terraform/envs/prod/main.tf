module "lakehouse_storage" {
  source = "../../modules/s3_bucket"
  bucket_name = "nyc-data-platform-prod"
}
