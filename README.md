## Description

This project is part of the MBA in Data Engineering from XPE. The job consists in extracting investment funds information from 
[CVM](https://dados.cvm.gov.br/dataset/fi-doc-inf_diario). All raw Data is storaged in an AWS bucket, then transformed and finally saved in postgres SQL data base. The pipeline is entirely orchestrated using AIRFLOW.

## About the source

CVM is an autonomous entity under a special, linked to the Ministry of Finance, with legal personality and assets
own, endowed with independent administrative authority, absence of subordination hierarchical structure, fixed mandate and stability of its directors, and financial and budget.

Its mission is to develop, regulate and supervise the Securities Market, as an instrument fundraising for companies, protecting the interests of investors and
ensuring wide dissemination of information about issuers and their values furniture

## Requirements

- Docker
- docker-compose
- VS Code

## Inputs

Should define YEAR and Month reference you wish to collect

```
reference = "YYYYMM"
```

## Outputs

A data base with the schema bellow:

```
CREATE TABLE IF NOT EXISTS cvm (
    cnpj VARCHAR(255),
    nm_fundo VARCHAR(255),
    classificacao_cvm VARCHAR(255),
    dt_comptc VARCHAR(255),
    vl_total FLOAT,
    vl_cota FLOAT,
    vl_pl_liq FLOAT,
    captc_dia FLOAT,
    resg_dia FLOAT,
    nr_cotst VARCHAR(255)
);
```
## What was Done

- [X] Developed a crawler to scrape the source
- [X] Use AIRFLOW to orchestrate the whole process
- [X] Create an image for the pipeline
- [X] Use postgres to load the data


## How to run

1 - Run the service
```
docker-compose up
```

2 - Access localhost:8080


3 - Type your airflow user and credentials which should be airflow


4 - Import your aws variables in admin > variables

Use the file variables_template.json as a model

```
{
    "AWS_ACCESS_KEY_ID": "KEY",
    "AWS_SECRET_ACCESS_KEY": "SECRET"
}
```

5 - Run the dags and wait for the magic happen

## Architecture

![CVM ARQ](/img/p_arq.png)


## Next steps

- [] 
- [] Use kubernetes to deploy the solution