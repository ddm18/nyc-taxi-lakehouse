output "data_bucket_name" {
  value = var.data_bucket_name
}

output "artifact_bucket_name" {
  value = var.artifact_bucket_name
}

output "ecs_cluster_arn" {
  value = aws_ecs_cluster.this.arn
}

output "ecs_task_definition_arn" {
  value = aws_ecs_task_definition.pipeline.arn
}

output "lambda_function_name" {
  value = aws_lambda_function.control_plane.function_name
}

output "mwaa_environment_name" {
  value = aws_mwaa_environment.this.name
}

output "private_subnet_ids" {
  value = module.network.private_subnet_ids
}

output "ecs_security_group_id" {
  value = aws_security_group.ecs_tasks.id
}
