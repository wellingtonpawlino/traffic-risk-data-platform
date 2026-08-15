output "bucket_name" {
  description = "Nome do bucket criado"
  value       = aws_s3_bucket.data_lake.bucket
}

output "bucket_arn" {
  description = "ARN do bucket"
  value       = aws_s3_bucket.data_lake.arn
}
