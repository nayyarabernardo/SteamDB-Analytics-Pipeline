import os
import glob
import pandas as pd
import datetime
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_latest_file(directory: str, pattern: str) -> str:
    """
    Encontra o arquivo mais recente em um diretório que corresponde a um padrão específico.
    
    Args:
        directory (str): O diretório onde procurar os arquivos.
        pattern (str): O padrão do nome do arquivo (ex: 'steamdb_sales_processed_*.csv').
    
    Returns:
        str: O caminho completo do arquivo mais recente ou None se nenhum arquivo for encontrado.
    """

    list_of_files = glob.glob(os.path.join(directory, pattern))
    if not list_of_files:
        return None
    return max(list_of_files, key=os.path.getctime)

def generate_filename(stage: str, dataset_name: str, extension: str = 'csv') -> str:
    """
    Gera um nome de arquivo com base no nome do dataset, estágio e data atual.
    
    Args:
        dataset_name (str): Nome do dataset.
        stage (str): Estágio do processamento (ex: 'processed', 'trusted').
        extension (str, optional): Extensão do arquivo. Padrão é 'csv'.
    
    Returns:
        str: Nome do arquivo gerado.
    """

    date = datetime.datetime.now().strftime('%Y%m%d')
    return f"{stage}_{dataset_name}_{date}.{extension}"


"""
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    
    #Aplica transformações ao DataFrame.
    
    #Args:
    #    df (pd.DataFrame): DataFrame a ser transformado.
    
    #Returns:
    #    pd.DataFrame: DataFrame transformado.
    

    df['discount_in_percent'] = df['discount_in_percent'].astype(int)
    df['price_in_brl'] = df['price_in_brl'] / 100
    
    for col in ['end_time_in_seconds', 'start_time_in_seconds', 'release_time_in_seconds']:
        df[col.replace('_in_seconds', '')] = pd.to_datetime(df[col], unit='s')
        df = df.drop(columns=[col])
    
    df['all_time_low'] = pd.to_numeric(df['all_time_low'], errors='coerce')
    
    # Adiciona a coluna 'safra'
    data_extracao = datetime.datetime.now()
    df['safra'] = data_extracao.strftime('%Y%m')

    return df
"""
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza transformações no DataFrame, incluindo a adição da coluna 'safra'.
    
    Args:
        df (pd.DataFrame): O DataFrame original a ser transformado.
    
    Returns:
        pd.DataFrame: O DataFrame transformado.
    """

    # Adiciona a coluna 'safra'
    data_extracao = datetime.datetime.now()
    df['safra'] = data_extracao.strftime('%Y%m')

    return df


def append_to_trusted(new_data: pd.DataFrame, trusted_file: str):
    """
    Adiciona novos dados ao arquivo trusted existente ou cria um novo se não existir.
    
    Args:
        new_data (pd.DataFrame): Novos dados a serem adicionados.
        trusted_file (str): Caminho do arquivo trusted.
    """
    if os.path.exists(trusted_file):
        existing_data = pd.read_csv(trusted_file)
        combined_data = pd.concat([existing_data, new_data], ignore_index=True)
    else:
        combined_data = new_data
    
    combined_data.to_csv(trusted_file, index=False)

def run_etl_pipeline(processed_directory: str, trusted_directory: str):
    """
    Executa o pipeline ETL completo, adicionando novos dados ao arquivo trusted existente.

    Args:
        processed_directory (str): Diretório onde estão os arquivos processed.
        trusted_directory (str): Diretório onde será salvo o arquivo trusted.
    """
    try:
        latest_processed_file = get_latest_file(processed_directory, "processed_steamdb_sales_*.csv")

        if latest_processed_file is None:
            logger.warning("Nenhum arquivo processed encontrado para processar.")
            return

        logger.info(f"Processando o arquivo: {latest_processed_file}")
        
        df = pd.read_csv(latest_processed_file)
        df_transformed = transform_data(df)

        trusted_filename = "trusted_steamdb_sales.csv"
        trusted_filepath = os.path.join(trusted_directory, trusted_filename)
        
        append_to_trusted(df_transformed, trusted_filepath)

        # Obtém o caminho relativo em relação ao diretório raiz
        relative_path = os.path.relpath(trusted_filepath, start='/home/nay/Documentos/Projetos')

        logger.info(f"Transformação concluída. Dados adicionados em '{relative_path}'")

    except Exception as e:
        logger.error(f"Erro durante o processamento: {str(e)}")

if __name__ == "__main__":
    PROCESSED_DIRECTORY = "/home/nay/Documentos/Projetos/SteamDB-Analytics-Pipeline/data/processed/"
    TRUSTED_DIRECTORY = "/home/nay/Documentos/Projetos/SteamDB-Analytics-Pipeline/data/trusted/"

    run_etl_pipeline(PROCESSED_DIRECTORY, TRUSTED_DIRECTORY)