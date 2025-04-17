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
from llmcall import *
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
    
# def sync_scroll_page(page, max_scrolls=10, wait_time=2):
#     last_height = page.evaluate("() => document.body.scrollHeight")
#     print(f"Initial height: {last_height}")

#     for _ in range(max_scrolls):
#         page.evaluate(f"() => window.scrollTo(0, {last_height})")
#         time.sleep(wait_time)

#         new_height = page.evaluate("() => document.body.scrollHeight")
#         if new_height == last_height:
#             print("No more content to load.")
#             break
#         last_height = new_height

#     print("Scrolling complete.")

async def get_next_page(page,url):
    output_format = {
        "text" : "The text of the button that takes to the next page" ###give only the text and no extra explanation or other words.
    }
    try:
        content = await page.content()
        soup = BeautifulSoup(content, 'lxml')
        # Remove header and footer
        for tag in soup(["header", "footer"]):
            tag.decompose()  # completely removes the tag and its contents

        # Now extract the text
        text = soup.get_text(separator="\n", strip=True)
        # text = soup.get_text()
        prompt = f"""
            I will give you text content of a webpage.
            This webpage has pagination for loading more products.The current webpage is {url}.
            Identify the text of the button that needs to be clicked that will take me to the next page.
            The pagination numbers and buttons come usually (but not necessarily) just after the product listings.
            If there is a button that explicitly says next or load next page or load more products, etc. then give the text for that button.
            Or else if there are only numbered buttons give the next page number that comes after the current page based on the current url.
            The text content of the page is {text}
            Output in this json format - {output_format}
        """
        response = await llm_service(prompt)
        response = json.loads(response)
        print(response)
        return response["text"]
    except Exception as ex:
        print(f"error in pagination_text {str(ex)}")
        return "2"

async def pagination_type(page,url):
    
    output_format = {
        "pagination":True
    }
    try:
        content = await page.content()
        soup = BeautifulSoup(content, 'lxml')
        # Remove header and footer
        for tag in soup(["header", "footer"]):
            tag.decompose()  # completely removes the tag and its contents

        # Now extract the text
        text = soup.get_text(separator="\n", strip=True)
        # text = soup.get_text()
        prompt = f"""
            from this text content of a webpage, identify whether the webpage has pagination buttons with subpages for products or not.
            If the products are loaded by scrolling and not by a button that needs to be clicked that does not constitute as pagination.
            Pagination is when there is a button that when clicked loads more products.It can be with next button or numbered page buttons.
            The numbers come usually (but not necessarily) just after the product listings.If there are numbers similar to page numbers, or if there is a button that takes to the next page return True.
            output in this json format: {output_format}.
            The text content is below - {text}
            """
        response = await llm_service(prompt)
        response = json.loads(response)
        print(response)
        return response["pagination"]
    except Exception as ex:
        print(f"error in pagination_type {str(ex)}")
        return False

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

async def pagination_scroll(page,url,llm=None):
    # last_height = await page.evaluate("document.body.scrollHeight")
    
    last_height = await page.evaluate("document.documentElement.scrollHeight")
    print("last_height",last_height)
    all_urls = set()
    while True:
        prev_length = len(all_urls)
        content = await page.content()
        soup = BeautifulSoup(content, 'lxml')
        urls = [urljoin(url, a['href'])
                for a in soup.find_all('a', href=True)]
        for meta in soup.find_all("meta", {"itemprop": "url"}):
            urls.append(meta["content"])
        all_urls.update(urls)
        if llm ==False:
            has_next = await page.query_selector("a[rel='next'], .pagination-next, .next, .page-next")
            if has_next:
                print("Clicking next button...")
                await has_next.click()
                await page.wait_for_load_state("networkidle")  # Wait until the page finishes loading
            else:
                print("No clickable 'next' button found.")
        elif llm == True:
            # await page.evaluate("""
            #     (text) => {
            #         const xpath = `//*[normalize-space(text())='${text}']`;
            #         const result = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
            #         const element = result.singleNodeValue;
            #         if (element) {
            #             element.scrollIntoView({ behavior: 'smooth', block: 'center' });
            #         } else {
            #             console.warn(`No element found with text: ${text}`);
            #         }
            #     }
            # """, selector)

            try:
                # Get the current URL the page is on
                current_url = await page.evaluate("window.location.href")
                print(f"Current URL: {current_url}")
                selector = await get_next_page(page,current_url)
                button = await page.wait_for_selector(f"text={selector}", timeout=3000)
            except Exception as ex:
                print(f"error in waiting for button {str(ex)}")
                break
            if button:
                print(f"Clicking button: {selector}")
                await button.click()
                await page.wait_for_timeout(2000)  # wait for products to load
            else:
                print("Button not found anymore.")
                break
        
        # await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.evaluate("""
            window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });
        """)

        await asyncio.sleep(2)
        # await page.wait_for_load_state("networkidle")
        new_height = await page.evaluate("document.body.scrollHeight")
        # new_height = await page.evaluate("document.documentElement.scrollHeight")
        print("new_height",new_height)

        if len(all_urls) == prev_length and new_height == last_height:
            break
        last_height = new_height
    all_urls = list(all_urls)
    with open("all_urls.txt", "w",encoding = 'utf-8') as file:
        for url in all_urls:
            file.write(url + "\n")
    print("all_urls",len(all_urls))
    return all_urls
        
        
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
                with open("output.html", "w",encoding = 'utf-8') as file:
                    file.write(await page.content())
                has_next = await page.query_selector("a[rel='next'], .pagination-next, .next, .page-next")
                has_pagination_numbers = await page.query_selector(".pagination, .page-numbers, ul.pagination")
                print("has_next",has_next)
                print("has_pagination_numbers",has_pagination_numbers)
                # next_button = page.locator("button:has-text('next page')")
                buttons = await page.query_selector_all("button, input[type='button'], input[type='submit'], a[role='button'], a.button")

                clickable_texts = []

                for button in buttons:
                    # Check if button is visible and enabled
                    if await button.is_visible():
                        text = await button.inner_text()
                        clickable_texts.append(text.strip())

                print("Clickable button texts:", clickable_texts)
                # if await next_button.is_visible():
                #     print("found button")
                #     await next_button.click()
                if has_next or has_pagination_numbers:
                    all_urls = await pagination_scroll(page,url,llm=False)
                    print("Pagination detected.")
                else:
                    print("falling back to llm")
                    pagination = await pagination_type(page,url)
                    if pagination:
                        all_urls = await pagination_scroll(page,url,llm=True)
                    else:
                        all_urls =[]
                        print("No pagination found.")

                
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
        
        # with open("output.txt", "w",encoding = 'utf-8') as file:
        #     file.write(content)
        return content, resp_stat,all_urls

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

async def get_product_urls(urls):
    total_tokens = 0
    limited_urls = []
    start = time.time()
    for url in urls:
        url_tokens = get_tokens_length(url)
        if total_tokens + url_tokens <= 10000:
            limited_urls.append(url)
            total_tokens += url_tokens
        else:
            break
    print("time taken to get limited urls",time.time() - start)
    output_format = {
        "url_format": "The format of the product urls" # Give just the format no other string or explanation
    }
    prompt = f"""
    From this list of urls - {limited_urls}
    give the format of the product urls. Give only that common starting part amongst the urls.
    So that it can be used later along with product names or ids to form product urls.
    Output in this json format - {output_format}
    """
    response = await llm_service(prompt)
    response = json.loads(response)
    print(response)
    return response["url_format"]


play_count = {"total":0,"success":0,"failure":0}

@app.post("/scrape")
async def scrape_url(request: ScrapeRequest):
    url = request.url

    play_count["total"] += 1
    # print(f"request recieved for url --{request.url}")
    
    response = None
    
    if url is None or url.isspace() or url == "":
        print(f"Invalid URL: {request.url}")
        raise HTTPException(status_code=400, detail=f"Invalid URL: {request.url}")

    try:
        response_text,code,list = await playwright_scrape(request.url)
        product_url_format = await get_product_urls(list)
        filtered_urls = [url.strip() for url in list if url.strip().startswith(product_url_format)]
        print(len(filtered_urls))
        
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
            "urls": filtered_urls
        }
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

