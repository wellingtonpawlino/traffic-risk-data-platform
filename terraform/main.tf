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

module "s3" {
  source      = "./modules/s3"
  bucket_name = "traffic-risk-datalake-infosiga"
}

module "rds" {
  source       = "./modules/rds"
  db_password  = var.db_password
  allowed_cidr = var.allowed_cidr
}
