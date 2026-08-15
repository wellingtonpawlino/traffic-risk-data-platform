terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket       = "traffic-risk-tfstate"
    key          = "traffic-risk-data-platform/terraform.tfstate"
    region       = "us-east-1"
    use_lockfile = true
    encrypt      = true
  }
}

provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "data_lake" {
  bucket = "traffic-risk-datalake-infosiga"
}

resource "aws_security_group" "postgres_sg" {
  name        = "traffic-risk-postgres-sg"
  description = "Permite acesso ao RDS Postgres"

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["177.170.44.4/32"]
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

  skip_final_snapshot = true
  publicly_accessible  = true

  vpc_security_group_ids = [aws_security_group.postgres_sg.id]
}