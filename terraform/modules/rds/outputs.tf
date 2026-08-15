output "endpoint" {
  description = "Endpoint do RDS PostgreSQL"
  value       = aws_db_instance.postgres.endpoint
}

output "db_name" {
  description = "Nome do database principal no RDS"
  value       = aws_db_instance.postgres.db_name
}
