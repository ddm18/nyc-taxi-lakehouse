locals {
  name_prefix         = "${var.project_name}-${var.environment_name}"
  data_bucket_arn     = "arn:aws:s3:::${var.data_bucket_name}"
  artifact_bucket_arn = "arn:aws:s3:::${var.artifact_bucket_name}"
  common_tags = {
    Project     = var.project_name
    Environment = var.environment_name
    ManagedBy   = "terraform"
  }
}

module "network" {
  source = "../network"

  name_prefix          = local.name_prefix
  vpc_cidr             = var.vpc_cidr
  availability_zones   = var.availability_zones
  private_subnet_cidrs = var.private_subnet_cidrs
  public_subnet_cidrs  = var.public_subnet_cidrs
  tags                 = local.common_tags
}

resource "aws_security_group" "ecs_tasks" {
  name        = "${local.name_prefix}-ecs"
  description = "ECS task security group"
  vpc_id      = module.network.vpc_id
  tags        = local.common_tags
}

resource "aws_security_group" "lambda" {
  name        = "${local.name_prefix}-lambda"
  description = "Lambda control plane security group"
  vpc_id      = module.network.vpc_id
  tags        = local.common_tags
}

resource "aws_security_group" "mwaa" {
  name        = "${local.name_prefix}-mwaa"
  description = "MWAA environment security group"
  vpc_id      = module.network.vpc_id
  tags        = local.common_tags
}

resource "aws_security_group" "rds" {
  name        = "${local.name_prefix}-rds"
  description = "RDS security group"
  vpc_id      = module.network.vpc_id
  tags        = local.common_tags
}

resource "aws_security_group_rule" "rds_from_ecs" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = aws_security_group.rds.id
  source_security_group_id = aws_security_group.ecs_tasks.id
}

resource "aws_security_group_rule" "rds_from_lambda" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = aws_security_group.rds.id
  source_security_group_id = aws_security_group.lambda.id
}

resource "aws_security_group_rule" "ecs_all_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.ecs_tasks.id
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "lambda_all_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.lambda.id
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "mwaa_self_ingress" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
  security_group_id        = aws_security_group.mwaa.id
  source_security_group_id = aws_security_group.mwaa.id
}

resource "aws_security_group_rule" "mwaa_all_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.mwaa.id
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_cloudwatch_log_group" "ecs" {
  name              = "/aws/ecs/${local.name_prefix}"
  retention_in_days = 7
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${local.name_prefix}-control-plane"
  retention_in_days = 7
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "mwaa" {
  name              = "/aws/mwaa/${local.name_prefix}"
  retention_in_days = 7
  tags              = local.common_tags
}

resource "aws_secretsmanager_secret" "audit_db" {
  name = "${local.name_prefix}/audit-db"
  tags = local.common_tags
}

resource "aws_secretsmanager_secret_version" "audit_db" {
  secret_id     = aws_secretsmanager_secret.audit_db.id
  secret_string = "postgresql://${var.audit_db_username}:${var.audit_db_password}@${module.rds.address}:${module.rds.port}/${var.audit_db_name}"
}

module "rds" {
  source = "../rds_postgres"

  identifier         = "${replace(local.name_prefix, "_", "-")}-audit"
  db_name            = var.audit_db_name
  username           = var.audit_db_username
  password           = var.audit_db_password
  subnet_ids         = module.network.private_subnet_ids
  security_group_ids = [aws_security_group.rds.id]
  tags               = local.common_tags
}

resource "aws_ecs_cluster" "this" {
  name = "${local.name_prefix}-cluster"
  tags = local.common_tags
}

resource "aws_iam_role" "ecs_execution" {
  name = "${local.name_prefix}-ecs-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_execution" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "ecs_task" {
  name = "${local.name_prefix}-ecs-task"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "ecs_task" {
  name = "${local.name_prefix}-ecs-task"
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [
          local.data_bucket_arn,
          "${local.data_bucket_arn}/*",
          local.artifact_bucket_arn,
          "${local.artifact_bucket_arn}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = [aws_secretsmanager_secret.audit_db.arn]
      }
    ]
  })
}

resource "aws_ecs_task_definition" "pipeline" {
  family                   = "${local.name_prefix}-pipeline"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = 2048
  memory                   = 8192
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([
    {
      name      = "nyc-pipeline"
      image     = var.container_image_uri
      essential = true
      environment = [
        { name = "AWS_REGION", value = var.aws_region },
        { name = "PIPELINE_RUNTIME", value = "cloud" },
        { name = "LAKEHOUSE_BUCKET_URI", value = "s3://${var.data_bucket_name}" },
        { name = "LAKEHOUSE_ENV", value = var.environment_name },
        { name = "LAKEHOUSE_ROOT", value = "s3://${var.data_bucket_name}/${var.environment_name}" },
        { name = "SPARK_WAREHOUSE_DIR", value = "s3a://${var.data_bucket_name}/${var.environment_name}/warehouse" },
      ]
      secrets = [
        { name = "AUDIT_DB_DSN", valueFrom = aws_secretsmanager_secret.audit_db.arn }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.ecs.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])
  tags = local.common_tags
}

data "archive_file" "control_plane_lambda" {
  type        = "zip"
  source_file = "${path.root}/../../../../orchestration/cloud/control_plane_lambda.py"
  output_path = "${path.root}/.terraform/control-plane-lambda.zip"
}

resource "aws_iam_role" "lambda" {
  name = "${local.name_prefix}-control-plane"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_vpc" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "lambda" {
  name = "${local.name_prefix}-control-plane"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "mwaa:CreateCliToken",
          "mwaa:GetEnvironment"
        ]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["${local.artifact_bucket_arn}/*"]
      }
    ]
  })
}

resource "aws_lambda_function" "control_plane" {
  function_name = "${local.name_prefix}-control-plane"
  role          = aws_iam_role.lambda.arn
  runtime       = "python3.11"
  handler       = "control_plane_lambda.lambda_handler"
  filename      = data.archive_file.control_plane_lambda.output_path
  timeout       = 900
  memory_size   = 512

  vpc_config {
    subnet_ids         = module.network.private_subnet_ids
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      MWAA_ENVIRONMENT_NAME       = "${local.name_prefix}-mwaa"
      MWAA_DAG_ID                 = "nyc_taxi_pipeline"
      CONTROL_PLANE_REPORT_BUCKET = var.artifact_bucket_name
    }
  }

  tags = local.common_tags
}

resource "aws_iam_role" "mwaa_execution" {
  name = "${local.name_prefix}-mwaa-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = [
          "airflow.amazonaws.com",
          "airflow-env.amazonaws.com",
        ]
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "mwaa_execution" {
  name = "${local.name_prefix}-mwaa-execution"
  role = aws_iam_role.mwaa_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["airflow:PublishMetrics"]
        Resource = [
          "arn:aws:airflow:${var.aws_region}:${data.aws_caller_identity.current.account_id}:environment/${local.name_prefix}-mwaa",
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "ecs:RunTask",
          "ecs:DescribeTasks",
          "ecs:DescribeTaskDefinition",
          "iam:PassRole",
          "lambda:InvokeFunction"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject*",
          "s3:GetBucket*",
          "s3:List*",
        ]
        Resource = [
          local.artifact_bucket_arn,
          "${local.artifact_bucket_arn}/*",
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:CreateLogGroup",
          "logs:PutLogEvents",
          "logs:GetLogEvents",
          "logs:GetLogRecord",
          "logs:GetLogGroupFields",
          "logs:GetQueryResults",
        ]
        Resource = [
          "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:airflow-${local.name_prefix}-*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:DescribeLogGroups"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetAccountPublicAccessBlock"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "sqs:ChangeMessageVisibility",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl",
          "sqs:ReceiveMessage",
          "sqs:SendMessage",
        ]
        Resource = "arn:aws:sqs:${var.aws_region}:*:airflow-celery-*"
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey",
          "kms:GenerateDataKey*",
          "kms:Encrypt",
        ]
        NotResource = "arn:aws:kms:*:${data.aws_caller_identity.current.account_id}:key/*"
        Condition = {
          StringLike = {
            "kms:ViaService" = [
              "sqs.${var.aws_region}.amazonaws.com",
            ]
          }
        }
      }
    ]
  })
}

resource "aws_mwaa_environment" "this" {
  name                  = "${local.name_prefix}-mwaa"
  airflow_version       = "2.10.3"
  environment_class     = "mw1.micro"
  execution_role_arn    = aws_iam_role.mwaa_execution.arn
  source_bucket_arn     = local.artifact_bucket_arn
  dag_s3_path           = var.mwaa_dag_s3_path
  requirements_s3_path  = var.mwaa_requirements_s3_path
  plugins_s3_path       = var.mwaa_plugins_s3_path
  webserver_access_mode = "PRIVATE_ONLY"

  airflow_configuration_options = {
    "core.dag_run_conf_overrides_params" = "True"
    "core.load_examples"                 = "False"
  }

  network_configuration {
    security_group_ids = [aws_security_group.mwaa.id]
    subnet_ids         = module.network.private_subnet_ids
  }

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "INFO"
    }
    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }

  tags = local.common_tags
}

data "aws_caller_identity" "current" {}
