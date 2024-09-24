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

1. Configuração do ambiente de desenvolvimento:
* Criação de um ambiente virtual (venv)
* Instalação das bibliotecas necessárias através do arquivo requirements.txt
* Configuração das variáveis de ambiente no arquivo .env
2. Extração de dados:
* Acesso ao site https://steamdb.info/sales/
* Utilização da biblioteca BeautifulSoup para fazer o webscraping dos dados
* Limpeza e transformação dos dados extraídos
3. Carregamento de dados:
* Criação do projeto, dataset e tabela no Google BigQuery
* Carregamento de dados brutos e tratados para o datalake
* Carregamento dos dados tratados para o BigQuery
* Exportação dos dados para uma planilha no [Google Sheets](https://docs.google.com/spreadsheets/d/1siFjaCa92INpVe-cp2kr8vIAiNvs5thCLkI4VxBTjC8/edit?usp=sharing)
4. Análise e visualização:
* Criação de consultas SQL no BigQuery para análise dos dados
* Desenvolvimento de um dashboard no Google Data Studio para visualização dos insights
5. Automatização do processo:
* Criação de um script de automação bash para executar o processo de ETL periodicamente


## Contato

Se tiver alguma duvida, sinta-se à vontade para entrar em contato comigo em: 

<div> 
  <a href = "mailto:nayyarabernardo@gmail.com"><img src="https://img.shields.io/badge/-Gmail-%23333?style=for-the-badge&logo=gmail&logoColor=white" target="_blank"></a>
  <a href="https://www.linkedin.com/in/nayyarabernardo" target="_blank"><img src="https://img.shields.io/badge/-LinkedIn-%230077B5?style=for-the-badge&logo=linkedin&logoColor=white" target="_blank"></a> 
  
</div>
