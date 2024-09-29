
#---------------------------------------------------- 1.0 Libraries ----------------------------------------------------
import os
import re
from dotenv import load_dotenv

from langchain_openai  import AzureChatOpenAI
from langchain.chains.llm import LLMChain
from langchain.prompts import PromptTemplate

import pyodbc
import json
import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

#------------------------------------------------------ 2.0 Setup ------------------------------------------------------

load_dotenv('.env')
# Define o tipo de ambiente (DEV, PROD, etc.)
env_type = "DEV"

# Carregar configurações do Azure OpenAI a partir das variáveis de ambiente
AOAI_ENDPOINT = os.getenv(f"AOAI_ENDPOINT_{env_type}")
AOAI_DEPLOYMENT_NAME = os.getenv(f"AOAI_DEPLOYMENT_NAME_{env_type}")
AOAI_API_KEY = os.getenv(f"AOAI_API_KEY_{env_type}")
AIOAI_API_VERSION = os.getenv(f"AOAI_API_VERSION") 

# Carregar configurações do Azure SQL Server a partir das variáveis de ambiente
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

# Criar o engine do SQLAlchemy
engine = create_engine(connection_string)

target_columns = ['data', 'feridos', 'longitude', 'latitude', 'tipo_acid', 'hora', 'regiao']

data_dictionary_filtered = {key: value for key, value in {
    "data_extracao": "Data e hora de realização da extração de dados do sistema",
    "idacidente": "Número de identificação do acidente",
    "longitude": "Coordenada geográfica (eixo X) do acidente",
    "latitude": "Coordenada geográfica (eixo Y) do acidente",
    "log1": "Nome do Logradouro onde ocorreu o acidente",
    "log2": "Nome do Logradouro que cruza o Logradouro no ponto onde ocorreu o acidente",
    "predial1": "Número do Logradouro onde ocorreu o acidente",
    "tipo_acid": "Informação descritiva do tipo de acidente",
    "queda_arr": "Informação se no acidente houve queda de algum veículo em arroio",
    "data": "Data em que ocorreu o acidente",
    "dia_sem": "Dia da semana em que ocorreu o acidente",
    "hora": "Hora em que ocorreu o acidente",
    "feridos": "Número de feridos no acidente",
    "feridos_gr": "Número de feridos graves no acidente",
    "mortos": "Contagem de vítimas fatais no momento do acidente",
    "morte_post": "Contagem de vítimas fatais posteriores ao acidente e relacionadas ao mesmo. É considerado morte posterior a vítima que veio a óbito até 30 dias após o acidente de trânsito.",
    "fatais": "Somatório das vítimas fatais no momento do acidente e das vítimas posteriores relacionadas ao mesmo",
    "auto": "Número de veículos do tipo automóvel envolvidos no acidente",
    "taxi": "Número de táxis envolvidos no acidente",
    "lotacao": "Número de veículos do tipo lotações envolvidas no acidente",
    "onibus_urb": "Número de ônibus urbanos envolvidos no acidente",
    "onibus_met": "Número de ônibus metropolitanos envolvidos no acidente",
    "onibus_int": "Número de ônibus interurbanos envolvidos no acidente",
    "caminhao": "Número de veículos do tipo caminhão envolvidos no acidente",
    "moto": "Número de motocicletas envolvidas no acidente",
    "carroca": "Número de carroças envolvidas no acidente",
    "bicicleta": "Número de bicicletas envolvidas no acidente",
    "outro": "Número de outros veículos envolvidos no acidente",
    "noite_dia": "Turno em que ocorreu o acidente",
    "regiao": "Zona da cidade onde ocorreu o acidente",
    "cont_vit": "Informação se o acidente possuiu ou não vítimas",
    "ups": "Unidade padrão de severidade: Peso atribuído aos tipos de acidentes de acordo com a gravidade dos danos causados",
    "consorcio": "Consórcio responsável pelo(s) ônibus urbano(s) envolvido(s) no acidente"
}.items() if key in target_columns}

data_dictionary = {
    "tables": {
        "poa_acidentes_transito": {
            "columns": data_dictionary_filtered
        }
    }
}

#---------------------------------------------------- 3.0 Verificação da Conexão -------------------------------------

def verify_connection(engine):
    """Verifica se a conexão com o banco de dados está funcionando."""
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            value = result.fetchone()[0]
            if value == 1:
                print("Conexão bem-sucedida com o Azure SQL Database!")
            else:
                print("Conexão falhou.")
    except SQLAlchemyError as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        raise e


def extrair_json(response: str) -> str:
    """
    Extrai o conteúdo JSON de uma resposta que pode conter blocos de código Markdown.
    
    :param response: Resposta do modelo.
    :return: String contendo apenas o JSON.
    """
    # Expressão regular para capturar conteúdo dentro de blocos de código ```json ... ```
    match = re.search(r'```json\s*([\s\S]*?)\s*```', response)
    if match:
        return match.group(1)
    else:
        # Se não houver blocos de código, retorna a resposta original
        return response.strip()
    

def interpretar_intencao(user_input: str, data_dictionary: dict) -> dict:
    """
    Interpreta a intenção do usuário a partir da entrada em linguagem natural.
    
    :param user_input: Entrada em linguagem natural do usuário.
    :param data_dictionary: Dicionário de dados da base de dados.
    :return: Dicionário contendo a intenção, entidades e ação.
    """
    # Converter o dicionário de dados para string JSON formatada
    data_dict_str = json.dumps(data_dictionary, ensure_ascii=False, indent=4)
    
    response = intent_chain.run({
        "user_input": user_input,
        "data_dictionary": data_dict_str
    })
    
    # Extrair o JSON da resposta
    json_str = extrair_json(response)
    
    try:
        intent_data = json.loads(json_str)
        return intent_data
    except json.JSONDecodeError as e:
        print("Erro ao decodificar a resposta do LLM:", e)
        print("Resposta recebida:", response)
        return {}


def gerar_query_sql(intent_data: dict, data_dictionary: dict) -> str:
    """
    Gera uma query T-SQL válida com base na intenção do usuário e no dicionário de dados.

    :param intent_data: Dicionário contendo a intenção, entidades e ação.
    :param data_dictionary: Dicionário de dados da base de dados.
    :return: String contendo a query SQL gerada.
    """
    intencao = intent_data.get("intencao", "")
    entidades = ", ".join(intent_data.get("entidades", {}).keys())
    acao = intent_data.get("acao", "").upper()
    
    # Converter o dicionário de dados para string JSON formatada
    data_dict_str = json.dumps(data_dictionary, ensure_ascii=False, indent=4)
    
    # Gerar a query usando a cadeia de geração
    query = query_chain.run({
        "intencao": intencao,
        "entidades": entidades,
        "acao": acao,
        "data_dictionary": data_dict_str
    })
    
    # Substituir os placeholders na query com os valores reais
    entidades_valores = intent_data.get("entidades", {})
    query_substituida = substituir_placeholders(query, entidades_valores)
    
    return query_substituida.strip()

def executar_query(query: str, engine) -> pd.DataFrame:
    """
    Executa a query no banco de dados e retorna os resultados em um DataFrame.
    
    :param query: Query SQL a ser executada.
    :param engine: Engine de conexão do SQLAlchemy.
    :return: DataFrame contendo os resultados da query.
    """
    try:
        df = pd.read_sql_query(query, engine)
        return df
    except Exception as e:
        print("Erro ao executar a query:", e)
        print("Query executada:", query)
        return pd.DataFrame()


def substituir_placeholders(query: str, entidades: dict) -> str:
    """
    Substitui os placeholders na query pelos valores correspondentes em entidades.

    :param query: A query SQL com placeholders (e.g., @ano).
    :param entidades: Dicionário contendo os valores para substituição.
    :return: Query SQL com os placeholders substituídos pelos valores reais.
    """
    for chave, valor in entidades.items():
        placeholder = f"@{chave}"
        # Determinar se o valor é numérico ou string para adicionar aspas se necessário
        if isinstance(valor, str):
            valor_str = f"'{valor}'"
        else:
            valor_str = str(valor)
        query = query.replace(placeholder, valor_str)
    return query



#---------------------------------------------------- 4.0 Configuração do Modelo de Linguagem --------------------------
# Verificar a conexão antes de prosseguir
verify_connection(engine)

# Configurar o modelo de chat OpenAI
llm = AzureChatOpenAI(
    temperature=0.7,
    azure_endpoint=AOAI_ENDPOINT,
    deployment_name=AOAI_DEPLOYMENT_NAME,
    openai_api_version=AIOAI_API_VERSION,
    openai_api_key=AOAI_API_KEY,
)

llm_sql = AzureChatOpenAI(
    temperature=0.1,
    azure_endpoint=AOAI_ENDPOINT,
    deployment_name=AOAI_DEPLOYMENT_NAME,
    openai_api_version=AIOAI_API_VERSION,
    openai_api_key=AOAI_API_KEY,
)


#---------------------------------------------------- 5.0 Configuração do SQLDatabase e SQLDatabaseChain ----------------
intent_prompt = PromptTemplate(
    input_variables=["user_input", "data_dictionary"],
    template="""
        Você é um assistente que interpreta a intenção do usuário para construir queries de banco de dados.

        Aqui está o dicionário de dados da base de dados em formato JSON:

        {data_dictionary}

        Dada a seguinte entrada do usuário: "{user_input}",
        identifique a intenção e estruture a saída como JSON com os seguintes campos:

        - **intencao**: A intenção principal do usuário.
        - **entidades**: Os principais elementos ou filtros mencionados.
        - **acao**: A ação a ser realizada (e.g., SELECT, UPDATE).

        **Resposta apenas o JSON, sem blocos de código ou texto adicional.**

        Entrada do usuário: "{user_input}"
        Saída:
        """
        )

query_prompt = PromptTemplate(
    input_variables=["intencao", "entidades", "acao", "data_dictionary"],
    template="""
        Você é um assistente que cria queries SQL baseadas na intenção do usuário e na estrutura do banco de dados fornecida.

        Aqui está o dicionário de dados da base de dados em formato JSON:

        {data_dictionary}

        Com base na intenção, entidades e ação abaixo, gere uma query T-SQL válida que possa ser executada no banco de dados.

        Intenção: {intencao}
        Entidades: {entidades}
        Ação: {acao}

        **Regras:**
        1. Utilize apenas as tabelas e colunas fornecidas no dicionário de dados.
        2. A query deve ser sintaticamente correta em T-SQL.
        3. Não inclua blocos de código ou formatação adicional; retorne apenas a query.

        **Saída:**
        """
        )


# Criar a cadeia (chain) para interpretar a intenção
intent_chain = LLMChain(
    llm=llm,
    prompt=intent_prompt,
    verbose=True
)


# Criar a cadeia (chain) para gerar a query SQL
query_chain = LLMChain(
    llm=llm_sql,
    prompt=query_prompt,
    verbose=True
)


if __name__ == "__main__":
    # Exemplo de input do usuário
    user_input = "Quero saber o número de acidentes de trânsito no ano de 2019."
    # user_input = "Me mostre os últimos 5 acidentes da zona sul"
    user_input = "Me mostre os 5 registros mais recentes de acidentes, ordenados por data decrescente."
    
    # Interpretar a intenção
    intent_data = interpretar_intencao(user_input, data_dictionary)
    
    print("Intenção Interpretada:")
    print(json.dumps(intent_data, indent=4, ensure_ascii=False))
    
    # Gerar a query SQL com base na intenção e no dicionário de dados
    query = gerar_query_sql(intent_data, data_dictionary)
    
    print("\nQuery Gerada:")
    print(query)
    
    # Executar a query no banco de dados
    if query:
        df_resultado = executar_query(query, engine)
        print("\nResultado da Query:")
        print(df_resultado)