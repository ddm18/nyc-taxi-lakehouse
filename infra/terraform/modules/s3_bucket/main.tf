resource "aws_s3_bucket" "data_lake" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "block" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "landing_cleanup" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "landing_cleanup"
    status = "Enabled"

    filter {
      prefix = "landing/"
    }

    expiration {
      days = 14
    }
  }
}
