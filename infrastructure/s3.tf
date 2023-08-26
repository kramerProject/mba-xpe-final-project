resource "aws_s3_bucket" "landing-bucket" {
  bucket = "dl-cvm-landing-${var.account_number}"
  acl    = "private"
}

resource "aws_s3_bucket" "processing-bucket" {
  bucket = "dl-cvm-processing-${var.account_number}"
  acl    = "private"
}