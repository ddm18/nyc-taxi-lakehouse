output "test_data_bucket_name" {
  value = module.test_data_bucket.bucket_name
}

output "test_artifact_bucket_name" {
  value = module.test_artifact_bucket.bucket_name
}

output "prod_data_bucket_name" {
  value = module.prod_data_bucket.bucket_name
}

output "prod_artifact_bucket_name" {
  value = module.prod_artifact_bucket.bucket_name
}
