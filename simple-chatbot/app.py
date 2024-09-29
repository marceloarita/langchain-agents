'''
References:
    1. https://docs.streamlit.io/develop/tutorials/llms/build-conversational-apps#build-a-bot-that-mirrors-your-input

'''

# !pip install python-dotenv streamlit langchain pandas openai
# Import necessary libraries
import os
import streamlit as st
from dotenv import load_dotenv
from openai import AzureOpenAI

# Load environment variables from .env file
load_dotenv('.env')

######################
# Setup environment variables
######################

# Define the environment type (DEV, PROD, etc.)
env_type = "DEV"

# Load Azure OpenAI configurations from environment variables
AOAI_ENDPOINT = os.getenv(f"AOAI_ENDPOINT_{env_type}")
AOAI_DEPLOYMENT_NAME = os.getenv(f"AOAI_DEPLOYMENT_NAME_{env_type}")
AOAI_API_KEY = os.getenv(f"AOAI_API_KEY_{env_type}")

# Initialize the Azure OpenAI client
client = AzureOpenAI(
    api_key=AOAI_API_KEY,
    azure_endpoint=AOAI_ENDPOINT,
    api_version="2024-06-01",
    azure_deployment=AOAI_DEPLOYMENT_NAME
)

######################
# Main application logic
######################

# Set the title of the Streamlit app
st.title("Azure ChatGPT App")

# Set a default model for the OpenAI chat
if "azure_openai_model" not in st.session_state:
    st.session_state["azure_openai_model"] = "gpt-4o-mini"

# Initialize chat history if it doesn't exist
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Accept user input for the chat
if prompt := st.chat_input("Digite algo"):
    # Append user message to the session state
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    # Display the user message in the chat interface
    with st.chat_message("user"):
        st.markdown(prompt)

    # Get a response from the Azure OpenAI model
    with st.chat_message("assistant"):
        stream = client.chat.completions.create(
            model=st.session_state["azure_openai_model"],
            messages=[
                {"role": m["role"], "content": m["content"]}
                for m in st.session_state.messages
            ],
            stream=True,
        )
        
        # Display the streamed response in the chat
        response = st.write_stream(stream)
        
    # Append the assistant's response to the session state
    st.session_state.messages.append({"role": "assistant", "content": response})