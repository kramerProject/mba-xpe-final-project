resource "aws_glue_crawler" "glue_crawler_cvm" {
  database_name = var.database_name
  name          = var.glue_crawler_name
  role          = var.glue_role

  s3_target {
    path = "s3://dl-processing-zone-401868797180/cvm/"
  }
}