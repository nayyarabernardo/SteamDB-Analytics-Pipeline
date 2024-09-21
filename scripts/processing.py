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
        pattern (str): O padrão do nome do arquivo (ex: 'steamdb_sales_raw_*.csv').
    
    Returns:
        str: O caminho completo do arquivo mais recente ou None se nenhum arquivo for encontrado.
    """

    list_of_files = glob.glob(os.path.join(directory, pattern))
    if not list_of_files:
        return None
    return max(list_of_files, key=os.path.getctime)

def generate_filename(dataset_name: str, stage: str, extension: str = 'csv') -> str:
    """
    Gera um nome de arquivo com base no nome do dataset, estágio e data atual.
    
    Args:
        dataset_name (str): Nome do dataset.
        stage (str): Estágio do processamento (ex: 'raw', 'processed').
        extension (str, optional): Extensão do arquivo. Padrão é 'csv'.
    
    Returns:
        str: Nome do arquivo gerado.
    """

    date = datetime.datetime.now().strftime('%Y%m%d')
    return f"{dataset_name}_{stage}_{date}.{extension}"

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


def run_etl_pipeline(raw_directory: str, processed_directory: str):
    """
    Executa o pipeline ETL completo.

    Args:
        raw_directory (str): Diretório onde estão os arquivos raw.
        processed_directory (str): Diretório onde serão salvos os arquivos processados.
    """
    
    try:
        latest_raw_file = get_latest_file(raw_directory, "steamdb_sales_raw_*.csv")
        
        if latest_raw_file is None:
            logger.warning("Nenhum arquivo raw encontrado para processar.")
            return

        logger.info(f"Processando o arquivo: {latest_raw_file}")
        
        df = pd.read_csv(latest_raw_file)
        df_transformed = transform_data(df)

        
        processed_filename = generate_filename('steamdb_sales', 'processed')
        processed_filepath = os.path.join(processed_directory, processed_filename)
        
        df_transformed.to_csv(processed_filepath, index=False)
        # Obtém o caminho relativo em relação ao diretório raiz
        relative_path = os.path.relpath(processed_filepath, start='/home/nay/Documentos/Projetos')

        logger.info(f"Transformação concluída. Dados salvos em '{relative_path}'")


    except Exception as e:
        logger.error(f"Erro durante o processamento: {str(e)}")

if __name__ == "__main__":
    RAW_DIRECTORY = "/home/nay/Documentos/Projetos/SteamDB-Analytics-Pipeline/data/raw/"
    PROCESSED_DIRECTORY = "/home/nay/Documentos/Projetos/SteamDB-Analytics-Pipeline/data/processed/"

    run_etl_pipeline(RAW_DIRECTORY, PROCESSED_DIRECTORY)