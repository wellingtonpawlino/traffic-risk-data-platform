output "rds_endpoint" {
  description = "Endpoint do RDS PostgreSQL"
  value       = module.rds.endpoint
}

output "s3_bucket" {
  description = "Nome do bucket S3 do data lake"
  value       = module.s3.bucket_name
}
