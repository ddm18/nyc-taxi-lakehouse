output "lakehouse_bucket_name" {
  value = module.cloud_validation_env.data_bucket_name
}

output "artifact_bucket_name" {
  value = module.cloud_validation_env.artifact_bucket_name
}

output "ecs_cluster_arn" {
  value = module.cloud_validation_env.ecs_cluster_arn
}

output "ecs_task_definition_arn" {
  value = module.cloud_validation_env.ecs_task_definition_arn
}

output "lambda_function_name" {
  value = module.cloud_validation_env.lambda_function_name
}

output "mwaa_environment_name" {
  value = module.cloud_validation_env.mwaa_environment_name
}

output "private_subnet_ids" {
  value = module.cloud_validation_env.private_subnet_ids
}

output "ecs_security_group_id" {
  value = module.cloud_validation_env.ecs_security_group_id
}
