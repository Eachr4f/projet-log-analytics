variable "aws_region" {
  description = "Région AWS"
  type        = string
  default     = "eu-west-3"
}

variable "bucket_name" {
  description = "Nom du bucket S3 (doit être unique globalement)"
  type        = string
}
