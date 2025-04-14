from openai import AsyncAzureOpenAI, AzureOpenAI
from dotenv import load_dotenv
import os
import asyncio
from concurrent.futures import ThreadPoolExecutor
import random
from fastapi import HTTPException
import aiohttp
import os
from dotenv import load_dotenv
from typing import Optional
import time
import asyncio
import httpx
import tiktoken
executor = ThreadPoolExecutor()

load_dotenv()
azure_openai_api_key = os.getenv('AZURE_OPENAI_API_KEY')
azure_openai_endpoint = os.getenv('AZURE_OPENAI_ENDPOINT')


llm_endpoint = os.getenv("LLM_CALL_API")

def get_tokens_length(string):
    """_token length for model 4o_mini_

    Args:
        string (_string_): _input as a string_
    """
    encoder = tiktoken.encoding_for_model("gpt-4o-mini")
    length = len(encoder.encode(string))
    return length


llm_client = httpx.AsyncClient(
    limits=httpx.Limits(
        max_connections=20,
        max_keepalive_connections=20
    ),
    timeout=600
)

llm_semaphore = asyncio.Semaphore(60)


async def llm_service(prompt: str, max_tokens: int = 2000):
    # async with llm_semaphore:
    try:
        response = await llm_client.post(llm_endpoint, json={"prompt": prompt, "max_tokens": max_tokens})
        return response.json()
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Error in llm_service: {str(ex)}")      

    
async def call_llm_azure_gpt4o(prompt):
    AZURE_OPENAI_API_KEY = azure_openai_api_key
    AZURE_OPENAI_ENDPOINT = azure_openai_endpoint

    client = AzureOpenAI(
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_key=AZURE_OPENAI_API_KEY,
        api_version='2024-08-01-preview'
    )

    def make_llm_call():
        try:
            response = client.chat.completions.create(
                model='gpt-4o',
                response_format={ "type": "json_object" },
                messages=[
                    {"role": "system", "content": "You are a helpful assistant. Answer adhering to the conditions in the prompt in a JSON format"},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=4096,
                timeout=600
            )
            return (response.choices[0].message.content)
        except Exception as ex:
            print('4o api failed',ex)

    value = await asyncio.get_event_loop().run_in_executor(executor, make_llm_call)
    return value

async def call_llm_azure_gpt4(prompt):
    AZURE_OPENAI_API_KEY = azure_openai_api_key
    AZURE_OPENAI_ENDPOINT = azure_openai_endpoint
    client = AzureOpenAI(
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_key=AZURE_OPENAI_API_KEY,
        api_version='2024-08-01-preview'
    )
    def make_llm_call():
        try:
            response = client.chat.completions.create(
                model='gpt-4',
                response_format={ "type": "json_object" },
                messages=[
                    {"role": "system", "content": "You are a helpful assistant. Answer adhering to the conditions in the prompt in a JSON format"},
                    {"role": "user", "content": prompt},
                ],
                timeout=600
            )
            return (response.choices[0].message.content)
        except:
            print('gpt api failed')
    value = await asyncio.get_event_loop().run_in_executor(executor, make_llm_call)
    return value
