resource "aws_db_subnet_group" "this" {
  name       = "${var.identifier}-subnets"
  subnet_ids = var.subnet_ids
  tags       = var.tags
}

resource "aws_db_instance" "this" {
  identifier                 = var.identifier
  engine                     = "postgres"
  engine_version             = "16.14"
  instance_class             = "db.t4g.micro"
  allocated_storage          = 20
  db_name                    = var.db_name
  username                   = var.username
  password                   = var.password
  db_subnet_group_name       = aws_db_subnet_group.this.name
  vpc_security_group_ids     = var.security_group_ids
  publicly_accessible        = false
  multi_az                   = false
  skip_final_snapshot        = true
  deletion_protection        = false
  backup_retention_period    = 0
  auto_minor_version_upgrade = true
  tags                       = var.tags
}
