variable "db_password" {
  description = "Senha do banco RDS PostgreSQL"
  type        = string
  sensitive   = true
}

variable "allowed_cidr" {
  description = "CIDR autorizado a conectar na porta 5432 do RDS"
  type        = string
}
