
import os
# from langchain.llms import AzureOpenAI
from langchain_community.llms.openai import AzureOpenAI
from openai import AzureOpenAI

from dotenv import load_dotenv
import urllib
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError

# Carregar variáveis de ambiente do arquivo .env
load_dotenv('.env')
# Define the environment type (DEV, PROD, etc.)
env_type = "DEV"

# Load Azure OpenAI configurations from environment variables
AOAI_ENDPOINT = os.getenv(f"AOAI_ENDPOINT_{env_type}")
AOAI_DEPLOYMENT_NAME = os.getenv(f"AOAI_DEPLOYMENT_NAME_{env_type}")
AOAI_API_KEY = os.getenv(f"AOAI_API_KEY_{env_type}")\

# llm = AzureOpenAI(
#     temperature=0.7,  # Controla a criatividade das respostas
#     openai_api_key=AOAI_API_KEY,
#     openai_api_base=AOAI_ENDPOINT,
#     openai_api_type="azure",
#     openai_api_version='2024-06-01',
#     deployment_name=AOAI_DEPLOYMENT_NAME
# )

llm = AzureOpenAI(
    # temperature=0.7,  # Controla a criatividade das respostas
    api_key=AOAI_API_KEY,
    azure_endpoint=AOAI_ENDPOINT,
    # openai_api_type="azure",
    api_version='2024-06-01',
    azure_deployment=AOAI_DEPLOYMENT_NAME
)

def get_chat_response(user_input: str) -> str:
    """
    Recebe uma string de entrada e retorna a resposta gerada pelo modelo de linguagem.

    Parâmetros:
    - user_input (str): A mensagem do usuário.

    Retorna:
    - str: A resposta do modelo.
    """
    try:
        # Defina as mensagens como uma lista de dicionários
        messages = [{"role": "user", "content": user_input}]
        
        # Gere a resposta usando o modelo de linguagem
        response = llm.chat.completions.create(messages=messages, model='gpt-4o-mini')  
        
        # Extrair o conteúdo da resposta
        return response.choices[0].message.content.strip() 
    except Exception as e:
        return f"Erro ao gerar a resposta: {str(e)}"


mensagem_usuario = 'isso é um teste'
messages = [{"role": "user", "content": "Olá, como você está?"}]

response = llm.chat.completions.create(messages=messages, model='gpt-4o-mini')
response['choices'][0]['message']['content'].strip()
response.choices[0].message.content.strip() 


resposta_bot = get_chat_response(mensagem_usuario)
