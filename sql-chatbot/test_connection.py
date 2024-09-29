import os
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import urllib

# Carregar variáveis de ambiente
load_dotenv('.env')
env_type = "DEV"

# Definir os parâmetros do driver ODBC
driver = 'ODBC Driver 17 for SQL Server'  # Certifique-se de que este driver está instalado
server = os.getenv(f"AZURE_SQL_SERVER_{env_type}")  # Exemplo: 'poa-acidentes-server.database.windows.net'
database = os.getenv(f"AZURE_SQL_DATABASE_{env_type}")
username = os.getenv(f"AZURE_SQL_USERNAME_{env_type}")
password = os.getenv(f"AZURE_SQL_PASSWORD_{env_type}")

# Verificar se todas as variáveis estão definidas
if not all([driver, server, database, username, password]):
    raise ValueError("Por favor, defina as variáveis de ambiente AZURE_SQL_SERVER, AZURE_SQL_DATABASE, AZURE_SQL_USERNAME, AZURE_SQL_PASSWORD e DRIVER no arquivo .env")

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

# Criar o engine
try:
    engine = create_engine(connection_string)
    connection = engine.connect()
    print("Conexão bem-sucedida com o Azure SQL Database!")
    connection.close()
except SQLAlchemyError as e:
    print(f"Erro ao conectar ao banco de dados: {e}")