resource "aws_security_group" "postgres_sg" {
  name        = "traffic-risk-postgres-sg"
  description = "Permite acesso ao RDS Postgres"

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_instance" "postgres" {
  identifier        = "traffic-risk-postgres"
  engine            = "postgres"
  engine_version    = "15"
  instance_class    = "db.t3.micro"
  allocated_storage = 20

  db_name  = "airflow"
  username = "airflow"
  password = var.db_password

  skip_final_snapshot    = true
  publicly_accessible    = true

  vpc_security_group_ids = [aws_security_group.postgres_sg.id]
}

resource "null_resource" "create_databases" {
  depends_on = [aws_db_instance.postgres]

  triggers = {
    rds_id = aws_db_instance.postgres.id
  }

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command     = <<-EOT
      docker run --rm -e PGPASSWORD='${var.db_password}' postgres:15 \
        psql -h ${aws_db_instance.postgres.address} -U airflow -d airflow \
        -c "CREATE DATABASE analytics;" 2>/dev/null || true
      docker run --rm -e PGPASSWORD='${var.db_password}' postgres:15 \
        psql -h ${aws_db_instance.postgres.address} -U airflow -d airflow \
        -c "CREATE DATABASE superset;" 2>/dev/null || true
    EOT
  }
}
