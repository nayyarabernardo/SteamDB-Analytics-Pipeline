# Desafio Engenheiro de Dados beAnalytic

Desafio para a vaga de Engenheiro de Dados da beAnalytic

## Visão Geral

Foi solicitado a resolução de um caso técinico que consiste em:

* Realize a extração das informações que conseguir da base de dados listada no website: https://steamdb.info/sales/ 

* Armazene estes dados no Google BigQuery

* Em seguida exporte ou conecte esses dados em um Google Sheets e nos envie o link.

### Visualização dos Dados

![Dashboard](data/img/dashboard-sales.png)

Para visualização completa do [Dashboard](https://lookerstudio.google.com/s/lfV7_qzYQzc)


### Workflow

![Fluxograma](data/img/fluxograma.jpg)

If you decide to include this, you should also talk a bit about why you chose the architecture and tools you did for this project.

## Pre-requisitos

- Python (versão 3.11)
- Bibliotecas Python:
  - requests
  - pandas
  - google-cloud-bigquery
  - bs4

## Etapas do Desafio

1. Criação de projeto do Google Cloud Platform (GCP) 
2. Configuração do ambiente, instalando as bibliotecas no python
3. Criação de scripts que vai fazer extraçãom, tratamento e carregamento dos dados
4. Criação do conjunto de dados para o BigQuery
5. Execução do script main.py
6. Query na BigQuery
![Query](data/img/query.png)
7. Criação e da planilha no Google sheeats e conecção da mesma com o BigQuery
![Google Sheets](data/img/google-sheets.png)
8. Atualização programada do Google Sheets
![Google Sheets Atualização](data/img/sheets-atualizacao.png)
9. Criação de script de automação do processo


## Lessons Learned

Os dados dessa ETL estão no arquivo [Google Sheets](https://docs.google.com/spreadsheets/d/107E1cQSG64BBLDP2_S5-IDIMAwNifvYccLq1XUwSwPM/edit?usp=sharing)

## Contact

Sinta-se à vontade para entrar em contato comigo se tiver alguma dúvida em: 

<div> 
  <a href = "mailto:nayyarabernardo@gmail.com"><img src="https://img.shields.io/badge/-Gmail-%23333?style=for-the-badge&logo=gmail&logoColor=white" target="_blank"></a>
  <a href="https://www.linkedin.com/in/nayyarabernardo" target="_blank"><img src="https://img.shields.io/badge/-LinkedIn-%230077B5?style=for-the-badge&logo=linkedin&logoColor=white" target="_blank"></a> 
  
</div>