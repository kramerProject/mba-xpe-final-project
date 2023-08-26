variable "region" {
  description = "AWS region"
  type = string
  default = "us-east-1"
}

variable "glue_crawler_name" {
  default = "glue_crawler_cvm"
}

variable "database_name" {
  default = "cvm-db"
}

variable "glue_role" {
  default = "arn:aws:iam::401868797180:role/service-role/AWSGlueServiceRole-kramer-test-igti"
}

variable "account_number" {
  default = "401868797180"
}