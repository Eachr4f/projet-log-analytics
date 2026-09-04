terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

resource "aws_s3_bucket" "log_analytics" {
  bucket = var.bucket_name

  tags = {
    Project = "log-analytics-platform"
  }
}

resource "aws_s3_bucket_public_access_block" "log_analytics_block" {
  bucket = aws_s3_bucket.log_analytics.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

