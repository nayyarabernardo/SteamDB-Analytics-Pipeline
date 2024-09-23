#!/bin/bash

# Define o diretório base do projeto
PROJECT_DIR="/home/nay/Documentos/Projetos/SteamDB-Analytics-Pipeline"

# Define os caminhos para os scripts
PROCESSING_SCRIPT="$PROJECT_DIR/scripts/processing.py"
MODELING_SCRIPT="$PROJECT_DIR/scripts/trusted.py"
LOAD_SCRIPT="$PROJECT_DIR/scripts/load.py"

# Caminho para o ambiente virtual existente
VENV_DIR="$PROJECT_DIR/venv"  # Ajuste este caminho se necessário

# Verifica se o ambiente virtual existe
if [ ! -d "$VENV_DIR" ]; then
    echo "Erro: O ambiente virtual não foi encontrado em $VENV_DIR"
    exit 1
fi

# Caminho para o Python do ambiente virtual
PYTHON_PATH="$VENV_DIR/bin/python3"

# Define a tarefa cron (execução diária às 17:00)
CRON_JOB="50 12 * * * $PYTHON_PATH $PROCESSING_SCRIPT >> $PROJECT_DIR/logs/processing_log.txt 2>&1 && $PYTHON_PATH $MODELING_SCRIPT >> $PROJECT_DIR/logs/trusted_log.txt 2>&1 && $PYTHON_PATH $LOAD_SCRIPT >> $PROJECT_DIR/logs/load_log.txt 2>&1"

# Adiciona a tarefa cron
(crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -

# Mensagem de confirmação
echo "Tarefa cron agendada para executar os scripts todos os dias às 17:00 usando o ambiente virtual em $VENV_DIR."