#!/bin/bash

# Define os caminhos para os scripts
#EXTRACTION_SCRIPT="/home/nay/Documentos/Projetos/SteamDB-Analytics-Pipeline/scripts/extraction.py"
PROCESSING_SCRIPT="/home/nay/Documentos/Projetos/SteamDB-Analytics-Pipeline/scripts/processing.py"
MODELING_SCRIPT="/home/nay/Documentos/Projetos/SteamDB-Analytics-Pipeline/scripts/trusted.py"
LOAD_SCRIPT="/home/nay/Documentos/Projetos/SteamDB-Analytics-Pipeline/scripts/load_to_bigquery.py"

# Define a tarefa cron (aqui, execução diária às 19:00)
CRON_JOB="00 08 * * * python3 $PROCESSING_SCRIPT >> /home/nay/Documentos/Projetos/SteamDB-Analytics-Pipeline/logs/processing_log.txt 2>&1 && \
python3 $MODELING_SCRIPT >> /home/nay/Documentos/Projetos/SteamDB-Analytics-Pipeline/logs/truste.txt 2>&1 \
python3 $LOAD_SCRIPT >> /home/nay/Documentos/Projetos/SteamDB-Analytics-Pipeline/logs/load_log.txt 2>&1"


# Adiciona a tarefa cron
( crontab -l; echo "$CRON_JOB" ) | crontab -

# Mensagem de confirmação
echo "Tarefa cron agendada para executar os scripts todos os dias às 19:00."



