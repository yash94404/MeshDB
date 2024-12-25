from dotenv import load_dotenv
import openai
import asyncio
import os 

load_dotenv()

openai.api_key = os.getenv('OPENAI_API_KEY')

print(openai.api_key)

async def main():
    response = await openai.ChatCompletion.acreate(
        model="gpt-3.5-turbo-16k",
        messages=[
            {"role": "user", "content": "Hello, GPT-3.5!"}
        ]
    )
    print(response)

asyncio.run(main())
