# Webscraping and bigquery


## Visão Geral

* Realize a extração das informações que conseguir da base de dados listada no website: https://steamdb.info/sales/ 

* Armazene estes dados no Google BigQuery

* Em seguida exporte ou conecte esses dados em um Google Sheets e nos envie o link.

### Arquitetura de Dados

Este projeto utiliza uma arquitetura do tipo Medallion para o gerenciamento de dados, organizada em camadas:

* Raw: Dados extraídos diretamente da fonte, sem processamento.
  Para a camada raw foi uma boa prática escolhida de manter os dados exatamente como foram recebidos, sem modificações. Isso serve como um "backup" dos dados originais e permite reprocessamento se necessário.
* Processed: Dados que foram limpos e transformados, prontos para análise.
  Esta camada contém dados que passaram por alguma transformação e limpeza. Os dados estão dispostos para que seja reprocessados se necessário e como backap das constantes mdificações de formato 
* Trusted: Dados que passaram por validação e estão prontos para uso em decisões críticas.
  Uma única tabela principal


### Visualização dos Dados

![Dashboard](docs/img/dashboard-sales.png)

Para visualização completa do [Dashboard](https://lookerstudio.google.com/reporting/48ffd759-acd5-45ce-be7c-94536869e41f)


### Workflow

![Fluxograma](docs/img/fluxograma.png)


## Pre-requisitos

- Python (versão 3.12)
- Bibliotecas Python:
  - requests
  - pandas
  - google-cloud-bigquery
  - bs4

## Etapas 

1. Criação de projeto do Google Cloud Platform (GCP) 
2. Configuração do ambiente, instalando as bibliotecas no python
3. Criação de scripts que irão fazer extração, tratamento e carregamento dos dados
4. Criação do conjunto de dados para o BigQuery
5. Execução do script main.py
6. Query na BigQuery
![Query](docs/img/query.png)
7. Criação e conexão da planilha no Google sheets com o BigQuery
![Google Sheets](docs/img/google-sheets.png)
8. Atualização programada do Google Sheets

![Google Sheets Atualização](docs/img/sheets-atualizacao.png)

9. Criação de script de automação do processo


## Arquivo Google Sheets

Os dados dessa ETL estão no arquivo [Google Sheets](https://docs.google.com/spreadsheets/d/1siFjaCa92INpVe-cp2kr8vIAiNvs5thCLkI4VxBTjC8/edit?usp=sharing)

## Contato

Se tiver alguma duvida, sinta-se à vontade para entrar em contato comigo em: 

<div> 
  <a href = "mailto:nayyarabernardo@gmail.com"><img src="https://img.shields.io/badge/-Gmail-%23333?style=for-the-badge&logo=gmail&logoColor=white" target="_blank"></a>
  <a href="https://www.linkedin.com/in/nayyarabernardo" target="_blank"><img src="https://img.shields.io/badge/-LinkedIn-%230077B5?style=for-the-badge&logo=linkedin&logoColor=white" target="_blank"></a> 
  
</div>
