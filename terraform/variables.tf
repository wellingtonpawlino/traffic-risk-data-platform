variable "db_password" {
  description = "Senha do banco RDS PostgreSQL"
  type        = string
  sensitive   = true
}