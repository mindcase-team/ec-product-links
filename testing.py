from fastapi import FastAPI, Response, status, HTTPException, Request, BackgroundTasks
from pydantic import BaseModel
from typing import Optional
from bs4 import BeautifulSoup
import xmltodict
import requests
import re
import json
from urllib.parse import urljoin, urlparse
import httpx
import asyncio
from playwright.async_api import async_playwright, Error as PlawrightError
from playwright.sync_api import sync_playwright
import os
from starlette.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import anyio
import random
from supabase import create_client, Client  
import time
import gc
from caching import *
from job_scrape.unstop import *
# from test_files.pdf_prompts import *
import csv
from io import StringIO
from datetime import datetime
load_dotenv()
##variables to use throughout the code here
pw = None
logs = asyncio.Queue()
base_env_var = "BROWSER_PLAYWRIGHT_ENDPOINT"

endpoints = [
    os.environ.get(base_env_var if i == 0 else f"{base_env_var}_{i+1}")
    for i in range(3)
]
workers = 4
distributed_lists = [endpoints[i::workers] for i in range(workers)]
print("endpoints",len(endpoints))

async def startup():
    limiter = anyio.to_thread.current_default_thread_limiter()
    limiter.total_tokens = 1000
    global pw
    pw = await async_playwright().start()  
    # Start the background tasks
    # asyncio.create_task(send_logs_to_service())
    # asyncio.create_task(scheduled_scraping())

async def shutdown():
    global pw
    await pw.stop()
    

origins = [
    "http://localhost:3000",
    "https://localhost:3000",
    "http://localhost",
    "http://localhost:8080",
    "https://mc-react-app-production.up.railway.app",
    "https://mc-react-app-staging.up.railway.app",
    "https://mindcase-addin-stage.vercel.app"
]

app = FastAPI(on_startup=[startup],on_shutdown=[shutdown])

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "PUT", "POST", "DELETE", "OPTIONS", "PATCH"],
    allow_headers=["Content-Type"],
)

headers = {
    "method": "GET",
    "scheme": "https",
    "accept": "*/*",
    "accept-encoding": "*",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "cache-control": "max-age=0",
    "cookie": "ajs_anonymous_id=932757ca-fa8f-4444-b5a0-8bcc7e8373b6; logged_in=true;cf_clearance=3ivoFiMORVtYXtN7bBQE1Sqsz8HJk4oTBts7WXlSzFg-1729998438-1.2.1.1-MJVL0oKq05rbXjxRAIjhKCG4M_S1lVdPL00Emfc0YU6Kwx5R3NOqR9op.EY1c.mdgZQ5WAwZ7Mf2nQ_TJ71rIc.bl15n4qc1DC86oh.GrCqLfXuo44JrDdVpeMB7BUCpzItIBoMyA7q1eFR3H1iJXZ5LIiX..edxa7D2sLU2G.GJLGnPG6fNVc5Fxw560iZ9aEApxMb0heCpSirZemmzE4y9xYnJup.50KMDjrXgmx6LaVNydS9CakgoBOMzNhKRb2cln1rzLSRWvNsko3Sqr6n8vKn9WbZ4GlMpb3AYzfPkYUwY1zXpLDRO98Gn5T4s13XhmegyDwiCeJMmEmKDuphXqJDAdwGgdevovKC1EUQ.kN.caLIMWy9agGPw.kgaCPQFXXHrYXYq311tI.0yRg;datadome=3sJFw_WHv3IEA0RzxsrxihghSLhiLYxcvoKfbkQY7~EUWhWH5WtTvDWpCTjMqET9TVdH4VC2zS6Fchg3zjoZByjMC3SlT9tI6CwicxBZAUgGHIU3jjBGNOsx5Y7z2Csm;_cfuvid=TuwMpdHp6XJL2GW3mn3LYaBJ9_Bab8u0umPG3VtMqus-1728924345721-0.0.1.1-604800000; __cf_bm=C9CqQ_7woKw8EwWQdGFKbdll9gj5JTa9Y2Di7JTbtoY-1728940938-1.0.1.1-wF_m4WWX0EGSQAIu8mFp6bLl6bbchA38cJH5jBci7eg4ysu02hEEHs_jRrHqtwQ782hlaAHrzerZariX0DoJQw; amp_14ff67=D8-5V7gXtd4845d1G4RnsL...1ia6ei5uh.1ia6ei5uh.0.0.0",
    "sec-ch-ua": "\"Google Chrome\";v=\"129\", \"Not=A?Brand\";v=\"8\", \"Chromium\";v=\"129\"",
    "sec-ch-ua-arch": "\"arm\"",
    "sec-ch-ua-bitness": "\"64\"",
    "sec-ch-ua-full-version": "\"129.0.6668.91\"",
    "sec-ch-ua-full-version-list": "\"Google Chrome\";v=\"129.0.6668.91\", \"Not=A?Brand\";v=\"8.0.0.0\", \"Chromium\";v=\"129.0.6668.91\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-model": "\"\"",
    "sec-ch-ua-platform": "\"macOS\"",
    "sec-ch-ua-platform-version": "\"15.0.0\"",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
}

user_agents = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.2227.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.2228.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.3497.92 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
]

class ScrapeRequest(BaseModel):
    url: str
    page: Optional[int] = None

async def scroll_page(page):
    # last_height = await page.evaluate("document.body.scrollHeight")
    last_height = await page.evaluate("document.documentElement.scrollHeight")
    print("last_height",last_height)

    while True:

        # await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.evaluate("""
            window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });
        """)

        await asyncio.sleep(2)
        # await page.wait_for_load_state("networkidle")
        new_height = await page.evaluate("document.body.scrollHeight")
        # new_height = await page.evaluate("document.documentElement.scrollHeight")
        print("new_height",new_height)

        if new_height == last_height:
            break
        last_height = new_height
    return []

def sync_scroll_page(page, max_scrolls=10, wait_time=2):
    last_height = page.evaluate("() => document.body.scrollHeight")
    print(f"Initial height: {last_height}")

    for _ in range(max_scrolls):
        page.evaluate(f"() => window.scrollTo(0, {last_height})")
        time.sleep(wait_time)

        new_height = page.evaluate("() => document.body.scrollHeight")
        if new_height == last_height:
            print("No more content to load.")
            break
        last_height = new_height

    print("Scrolling complete.")

        
        
active_requests = {"total":0,"context":0,"page":0}
concurrency = asyncio.Semaphore(60)
rnd = 0

async def playwright_scrape(url):
    global active_requests, rnd
    active_requests["total"] += 1
    if rnd>2:
        rnd = 0
    endpoint = endpoints[rnd]
    user_agent = random.choice(user_agents)
    parsed_url = urlparse(url)
    origin = f"{parsed_url.scheme}://{parsed_url.netloc}/"
    new_headers = {
        "Origin": origin,
        "Referer": origin,
        "User-Agent": user_agent,
        "sec-ch-ua": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"macOS\"",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
    }
    lower_headers = {k.lower(): v for k, v in new_headers.items()}
    global pw
    start = time.time()
    try:
        async with concurrency:

            try:
                # print(endpoint)
                # browser = await pw.chromium.connect(endpoint, timeout=500_000)
                browser = await pw.chromium.launch(headless=False)
            except Exception as ex:
                raise HTTPException(status_code=500, detail=f" error connecting to browserless service {ex}")
            try:
                context = await browser.new_context(java_script_enabled=True,user_agent=user_agent, extra_http_headers=lower_headers)
            except Exception as ex:
                raise HTTPException(status_code=500, detail=f" error creating context {ex}")

            active_requests["context"] += 1
            if "linkedin" in url:
                with open("cookies/google.json", "r") as f:
                    cookies = json.load(f)
                await context.add_cookies(cookies)
            try:
                page = await context.new_page()
                await page.evaluate("navigator.webdriver = undefined")

                active_requests["page"] += 1
                response = await page.goto(url,timeout=100000)
                await asyncio.sleep(2)
                resp_stat = response.status
                list = await scroll_page(page)

                content = await page.content()
                # cookies = await context.cookies()
                # with open("cookies/blinkit.json", "w") as f:
                #     json.dump(cookies, f)

                
            except Exception as ex:
                raise HTTPException(status_code=500, detail=f" error in page actions {ex}")

            await context.close()
            active_requests["context"] -= 1
            await browser.close()
        active_requests["total"] -= 1
        active_requests["page"] -= 1
        
        with open("output.txt", "w",encoding = 'utf-8') as file:
            file.write(content)
        return content, resp_stat,list

    except Exception as ex:
        print(f"Error in playwright function for URL {url} -- {str(ex)}")
        active_requests["total"] -= 1
        active_requests["page"] -= 1
        raise HTTPException(status_code=500, detail=f"Error in playwright function {str(ex)}")

    finally:
        print("current active requests -", active_requests)
        print("Time taken -", url, time.time() - start)
        rnd+=1
        if 'page' in locals() and not page.is_closed():
            await page.close()
        if 'context' in locals():
            await context.close()
        if 'browser' in locals() and browser.is_connected():
            await browser.close()


async def handle_dialog(dialog):
    print(f"Dialog message: {dialog.message}")
    await dialog.dismiss()
    # Optionally: await dialog.accept("Optional input")
    
# Popup handler
async def handle_popup(popup):
    print(f"Popup URL: {popup.url}")
    await popup.close()

class HttpClientManager:
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=5.0)
    
    async def close(self):
        await self.client.aclose()

http_client_manager = HttpClientManager()


play_count = {"total":0,"success":0,"failure":0}

@app.post("/scrape")
async def scrape_url(request: ScrapeRequest):
    url = request.url

    play_count["total"] += 1
    # print(f"request recieved for url --{request.url}")
    try:
        cached_result = await get_cached_data(url)
        if cached_result is not None:
            print("cache used")
            play_count["success"] += 1
            return cached_result
        # print("cache not used")
    except Exception as ex:
        print(f"error in caching {str(ex)}")
    
    
    response = None
    
    if url is None or url.isspace() or url == "":
        print(f"Invalid URL: {request.url}")
        raise HTTPException(status_code=400, detail=f"Invalid URL: {request.url}")

    try:
        response_text,code,list = await playwright_scrape(request.url)
        soup = BeautifulSoup(response_text, 'lxml')
        page_text = soup.get_text()
        
        page_text = re.sub(r'\s+', ' ', page_text).strip()

        urls = [urljoin(request.url, a['href'])
                for a in soup.find_all('a', href=True)]
        for meta in soup.find_all("meta", {"itemprop": "url"}):
            urls.append(meta["content"])
        print(f"length of text on url{request.url} ",len(page_text))
        gc.collect()
        del soup
        del response_text
        play_count["success"] += 1
        with open("urls.txt", "w",encoding = 'utf-8') as file:
            for url in urls:
                file.write(url + "\n")
        # log = {
        #     "url": request.url,
        #     "output": page_text.replace('\u0000', '')[:100],
        #     "status_code": code,
        #     "character_length": len(page_text),
        #     "method_used":"playwright"
        # }

        answer = {
            "text": page_text,
            "urls": urls
        }
        if len(page_text) > 1000:
            try:
                await set_cached_data(request.url, answer)
            except Exception as ex:
                print(f"error in caching {str(ex)}")
        return answer
    except Exception as ex:
        play_count["failure"] += 1
        error = f"error in scrape api in else statement for url {request.url} -- {str(ex)}"
        print(error)
        log = {
            "url": request.url,
            "output": error,
            "status_code": 500,
            "character_length": 0,
            "method_used":"playwright"
        }
        await logs.put(log)
        raise HTTPException(status_code=500, detail=f"error scraping with playwright{str(ex)}")

