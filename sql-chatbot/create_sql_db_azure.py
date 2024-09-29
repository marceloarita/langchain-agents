#---------------------------------------------------- 1.0 Libraries ----------------------------------------------------
#pip install sqlalchemy pandas pyodbc

import os
import urllib
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.types import (
    Integer, Float, String, Date, DateTime, Boolean
)
from sqlalchemy.exc import SQLAlchemyError

#------------------------------------------------------ 2.0 Setup ------------------------------------------------------

# Load environment variables
load_dotenv('.env')
# Define the environment type (DEV, PROD, etc.)
env_type = "DEV"

# Get the Azure SQL configurations from environment variables
server = os.getenv(f"AZURE_SQL_SERVER_{env_type}")
database = os.getenv(f"AZURE_SQL_DATABASE_{env_type}")
username = os.getenv(f"AZURE_SQL_USERNAME_{env_type}")
password = os.getenv(f"AZURE_SQL_PASSWORD_{env_type}")
driver = os.getenv("SQL_DRIVER")

# Construir a string de conexão ODBC
odbc_str = (
    f'DRIVER={{{driver}}};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password};'
    'Encrypt=yes;'
    'TrustServerCertificate=no;'
    'Connection Timeout=30;'
)

# URL-encode a string ODBC
params = urllib.parse.quote_plus(odbc_str)

# Construir a URL de conexão SQLAlchemy com pyodbc
connection_string = f'mssql+pyodbc:///?odbc_connect={params}'
engine = create_engine(connection_string)

#---------------------------------------------------- 3.0 Functions ----------------------------------------------------

def map_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return Integer
    elif pd.api.types.is_float_dtype(dtype):
        return Float
    elif pd.api.types.is_bool_dtype(dtype):
        return Boolean
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return DateTime
    else:
        return String(255)

def create_table_from_csv(engine, table_name, csv_path, drop_if_exist=None):
    try:
        # Ler o CSV em um DataFrame
        df = pd.read_csv(csv_path, sep=';', encoding='utf-8')
        target_columns = ['data', 'feridos', 'longitude', 'latitude', 'tipo_acid', 'dia_sem', 'hora', 'regiao']
        df = df[target_columns].head(5000)

        # Inferir os tipos de dados do SQLAlchemy
        dtype_mapping = {col: map_dtype(dtype) for col, dtype in df.dtypes.items()}

        # Criar o inspector
        inspector = inspect(engine)

        # Verificar se a tabela já existe
        if inspector.has_table(table_name):
            if drop_if_exist:
                print(f"A tabela '{table_name}' existe e será descartada.")
                with engine.begin() as connection:
                    connection.execute(text(f"DROP TABLE [{table_name}]"))
                # Criar a tabela novamente
                df.head(0).to_sql(table_name, con=engine, index=False, if_exists='fail', dtype=dtype_mapping)
                print(f"Tabela '{table_name}' recriada com sucesso.")
            else:
                print(f"A tabela '{table_name}' já existe no banco de dados '{database}'. Nenhuma ação foi realizada.")
        else:
            # Criar a tabela no banco de dados
            df.head(0).to_sql(table_name, con=engine, index=False, if_exists='fail', dtype=dtype_mapping)
            print(f"Tabela '{table_name}' criada com sucesso.")
    except Exception as e:
        print(f"Erro inesperado: {e}")

# Função para inserir dados na tabela
def insert_data_from_csv(engine, table_name, csv_path):
    try:
        # Ler o CSV em um DataFrame
        df = pd.read_csv(csv_path, sep=';', encoding='utf-8')
        target_columns = ['data', 'feridos', 'longitude', 'latitude', 'tipo_acid', 'dia_sem', 'hora', 'regiao']
        df = df[target_columns].head(5000)
        # Inserir os dados no banco de dados
        df.to_sql(table_name, con=engine, index=False, if_exists='append', chunksize=1000, method=None)
        print(f"Dados inseridos com sucesso na tabela '{table_name}'.")
    except SQLAlchemyError as e:
        print(f"Erro ao inserir dados: {e}")
    except FileNotFoundError:
        print(f"Arquivo {csv_path} não encontrado.")
    except Exception as e:
        print(f"Erro inesperado: {e}")

#------------------------------------------------------- 4.0 Main ------------------------------------------------------

if __name__ == "__main__":
    table_name = "poa_acidentes_transito"
    csv_file = os.path.join('data', 'poa_acidentes_transito_from_201901_to_202409.csv')  # Ajuste conforme necessário

    # Criar a tabela
    create_table_from_csv(engine, table_name, csv_file, drop_if_exist=True)

    # Inserir os dados
    insert_data_from_csv(engine, table_name, csv_file)