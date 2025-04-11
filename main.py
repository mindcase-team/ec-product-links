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
supabase_url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_API_KEY")
supabase: Client = create_client(supabase_url, key)

storage_key = os.getenv("SUPABASE_STORAGE_KEY")
storage : Client = create_client(supabase_url, storage_key)


async def send_logs_to_service():
    logs_to_send = []

    while True:
        try:
            while not logs.empty():
                log = await logs.get()
                logs_to_send.append(log)
            # cpu_percent = psutil.cpu_percent(interval=None)
            # memory_info = psutil.virtual_memory()
            # resorce = {
            #     "cpu_percent": cpu_percent,
            #     "memory_percent": memory_info.percent,
            #     "total_memory_bytes": memory_info.total,
            #     "available_memory_bytes": memory_info.available
            # }
            # print (resorce)
            # Send transformed logs if any
            if len(logs_to_send)>0:
                print(f"playwright - {play_count}")
                try:
                    # Insert multiple rows in a single call by passing data_buffer as a list of dictionaries
                    response = supabase.table("scraper_logs").insert(logs_to_send).execute()
                except Exception as ex:
                    print(f"Error in batch insert to Supabase: {ex}")
            else:
                continue

        except Exception as ex:
            print(f"Error processing logs: {ex}")
        finally:
            await asyncio.sleep(30)  # Adjust sleep interval as needed



async def scheduled_scraping():
    while True:
        try:
            print("Starting scheduled scraping...")
            
            # Run Internshala scraping
            try:
                await scrape_internshala()
                print("Internshala scraping completed successfully")
            except Exception as e:
                print(f"Error in Internshala scraping: {str(e)}")
            
            # Run Unstop scraping
            try:
                await unstop_api()
                print("Unstop scraping completed successfully")
            except Exception as e:
                print(f"Error in Unstop scraping: {str(e)}")
                
            print("Scheduled scraping completed. Waiting for next run...")
            
            # Wait for 24 hours
            await asyncio.sleep(24 * 60 * 60)  # 24 hours in seconds
            
        except Exception as e:
            print(f"Error in scheduled scraping: {str(e)}")
            # If there's an error, wait for 1 hour before retrying
            await asyncio.sleep(60 * 60)

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
    

async def scroll_job_pane(page):
    try:
        # Wait for the job listing container to be present
        job_container = await page.wait_for_selector('.user_list')
        if not job_container:
            print("Could not find job listing container")
            return

        # Get initial scroll height
        last_height = await job_container.evaluate("element => element.scrollHeight")
        print("last_height",last_height)
        loop_count = 0
        while loop_count < 10:
            # Scroll to bottom of container
            await job_container.evaluate("""
                element => element.scrollTo({
                    top: element.scrollHeight,
                    behavior: 'smooth'
                })
            """)

            # Wait for any dynamic content to load
            await page.wait_for_timeout(2000)

            # Get new scroll height
            new_height = await job_container.evaluate("element => element.scrollHeight")
            print("new_height",new_height)
            # Break if no more content loaded
            if new_height == last_height:
                return []
                break
                
            last_height = new_height
            print(f"Scrolled to height: {new_height}")
            loop_count += 1

    except Exception as ex:
        print(f"Error scrolling job pane: {ex}")

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

# async def get_endpoint(worker_id):
#     try:
#         global rnd
#         if rnd>4:
#             rnd = 0
#         worker_id = worker_id - 4
        
#         endpoint = distributed_lists[worker_id][0]
#         rnd += 1
#         return endpoint
#     except Exception as ex:
#         print(f"Error in get_endpoint {str(ex)}")
    
# def get_worker_index():
#     """Determine worker index based on the number of workers."""
#     try:
#         # Use the process's identifier to create an index
#         worker_count = 4  # Must match the --workers 4 in your CMD
#         process_id = multiprocessing.current_process().pid
#         return process_id % worker_count  # Ensures it returns 0, 1, 2, or 3
#     except:
#         return 0  # Default in case of failure

async def save_mutual_fund_data(data, amc_name):
    """Save mutual fund data to both JSON and CSV formats"""
    # Create output directory if it doesn't exist
    os.makedirs("mutual_fund_data", exist_ok=True)
    
    # Save as JSON
    json_filename = f"mutual_fund_data/{amc_name}_funds.json"
    with open(json_filename, "w", encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        
    # Save as CSV
    csv_filename = f"mutual_fund_data/{amc_name}_funds.csv"
    if data and len(data) > 0:
        fieldnames = list(data[0].keys())
        with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
    
    print(f"Saved {len(data)} funds to {json_filename} and {csv_filename}")
    return json_filename, csv_filename

async def extract_mutual_fund_data(page, url):
    """Extract mutual fund data from the current page and handle pagination"""
    all_funds = []
    page_no = 0
    
    # Extract AMC name from URL
    amc_name = url.split('/')[-1].replace('.aspx', '')
    
    while True:
        page_no += 1
        print(f"Processing page {page_no} for {amc_name}")
        
        # Wait for table to be visible
        await page.wait_for_selector('.mds-data-table__cell', timeout=10000)
        
        # Extract current page data
        content = await page.content()
        soup = BeautifulSoup(content, 'lxml')
        
        # Find all table rows
        rows = soup.find_all('tr', class_='mds-data-table__row')
        
        for row in rows:
            fund_data = {}
            cells = row.find_all('td', class_='mds-data-table__cell')
            
            if len(cells) >= 4:  # Ensure we have enough cells
                # Extract fund name and URL
                fund_link = cells[0].find('a')
                if fund_link:
                    fund_data['name'] = fund_link.get_text(strip=True)
                    fund_data['url'] = urljoin("https://www.morningstar.in", fund_link.get('href'))
                
                # Extract Structure
                structure_cell = cells[1].find('span', class_='tiles-label')
                if structure_cell:
                    fund_data['structure'] = cells[1].get_text(strip=True).replace('Structure : ', '')
                
                # Extract Latest NAV
                nav_cell = cells[2].find('span', class_='tiles-label')
                if nav_cell:
                    fund_data['latest_nav'] = cells[2].get_text(strip=True).replace('Latest NAV : ', '')
                
                # Extract NAV Date
                date_cell = cells[3].find('span', class_='tiles-label')
                if date_cell:
                    fund_data['nav_date'] = cells[3].get_text(strip=True).replace('NAV Date : ', '')
                
                # Add metadata
                fund_data['amc_name'] = amc_name
                fund_data['extracted_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                if fund_data:
                    all_funds.append(fund_data)
                    
        print(f"Found {len(all_funds)} funds so far for {amc_name}")
        
        try:
            # First check if next button exists
            next_button = await page.query_selector('a:text("Next >")')
            if not next_button:
                print("No next button found - reached last page")
                break
                
            # Check if the next button is visible and enabled
            is_visible = await next_button.is_visible()
            if not is_visible:
                print("Next button exists but is not visible")
                break
                
            # Check if the button is enabled (not disabled)
            is_disabled = await next_button.evaluate('button => button.disabled || button.classList.contains("disabled")')
            if is_disabled:
                print("Next button is disabled")
                break
                
            # Get button position
            box = await next_button.bounding_box()
            if not box:
                print("Next button is not in viewport")
                break
                
            # Click the next button
            await next_button.click()
            print(f"Clicked next button - moving to page {page_no + 1}")
            
            # Wait for navigation and table update
            await page.wait_for_timeout(2000)  # Short pause
            
            # Wait for table to update - look for loading state or new content
            try:
                await page.wait_for_selector('.mds-data-table__cell', timeout=10000)
                
                # Verify page changed by checking some content changed
                new_content = await page.content()
                if new_content == content:
                    print("Page content didn't change after clicking next")
                    break
                    
            except Exception as e:
                print(f"Timeout waiting for table update: {e}")
                break
                
        except Exception as e:
            print(f"Error during pagination: {e}")
            break
    
    # Save the data
    json_file, csv_file = await save_mutual_fund_data(all_funds, amc_name)
    print(f"Completed extracting data for {amc_name}. Total funds: {len(all_funds)}")
    
    return all_funds

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
                if "morningstar" in url.lower():
                    mutual_fund_data = await extract_mutual_fund_data(page, url)
                    page_text = json.dumps(mutual_fund_data, indent=2)
                    urls = [fund['url'] for fund in mutual_fund_data if 'url' in fund]
                    list = []
                else:
                    if "unstop" in url:
                        list = await scroll_job_pane(page)
                    else:
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
        
        # with open("output.txt", "w",encoding = 'utf-8') as file:
        #     file.write(content)
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
    
    # if url.endswith(".pdf"):
    #     pdf_number = await get_pdf_number(url)
    #     try:
    #         await set_cached_data(url, pdf_number)
    #     except Exception as ex:
    #         print(f"error in caching {str(ex)}")
    #     return {
    #         "text": pdf_number,
    #         "urls": []
    #     }
    
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
    # url_regex = re.compile(r"^https?://(?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9]{1,6}\b(?:[-a-zA-Z0-9@:%_\+.~#?&//=]*)$")
    # if not url_regex.match(request.url):
    #     print(f"Invalid URL: {request.url}")
    #     raise HTTPException(status_code=400, detail=f"Invalid URL: {request.url}")
    
    
    # if "internshala.com" in url:
    #     answer = await scrape_internshala(request)
    #     return answer
    # if "wellfound" in url:
    #     with open("wellfound_all_pages.json", "r") as f:
    #         all_results = json.load(f)
    #         all_results = str(all_results)
    #     answer = {"text": all_results, "urls": []}
    #     return answer
    # if "indiamart" in url and request.page is not None:
    #     answer = await get_excavator_listings(request.page)
    #     return answer
    

    
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


internshala_cats = {
    "AI":"artificial-intelligence-ai-jobs/",
    "big data":"big-data-jobs/",
    "software development":"software-development-jobs/",
    "Product management":"product-jobs/",
    "Data science":"data-science-jobs/",
    "Digital marketing":"digital-marketing-jobs/",
    "IoT":  "internet-of-things-iot-jobs/",
    "SEO":"search-engine-optimization-seo-jobs/"
}

@app.post("/scrape_internshala")
async def scrape_internshala():
    """
    {
    "AI":"artificial-intelligence-ai-jobs/",
    "big data":"big-data-jobs/",
    "software development":"software-development-jobs/",
    "Product management":"product-jobs/",
    "Data science":"data-science-jobs/",
    "Digital marketing":"digital-marketing-jobs/",
    "IoT":  "internet-of-things-iot-jobs/",
    "SEO":"search-engine-optimization-seo-jobs/"
    }
    https://internshala.com/jobs/artificial-intelligence-ai,big-data,data-science,digital-marketing,internet-of-things-iot,product,search-engine-optimization-seo,software-development-jobs/
    """
    url = "https://internshala.com/jobs/artificial-intelligence-ai,big-data,data-science,digital-marketing,internet-of-things-iot,product,search-engine-optimization-seo,software-development-jobs/"

    if url is None or url.isspace() or url == "":
        print(f"Invalid URL: {url}")
        raise HTTPException(status_code=400, detail=f"Invalid URL: {url}")

    try:
        job_listings = []
        page_count = 0
        
        while True:
            print(f"scraping page {page_count +1}")
            response_text, code, list = await playwright_scrape(url)
            soup = BeautifulSoup(response_text, 'lxml')
            
            for job in soup.find_all("div", class_="individual_internship"):
                job_data = {}
                job_data["page"] = page_count
                
                # Extract job ID from the div's attributes
                job_id = job.get('internshipid')
                if job_id:
                    job_data["job_id"] = int(job_id)
                
                # Get job title
                title_elem = job.find("a", class_="job-title-href")
                if title_elem:
                    job_data["title"] = title_elem.text.strip()
                
                # Get company name
                company_elem = job.find("p", class_="company-name")
                if company_elem:
                    job_data["company"] = company_elem.text.strip()
                
                # Get job details
                details = job.find("div", class_="detail-row-1")
                if details:
                    # Location
                    location = details.find("p", class_="locations")
                    if location:
                        job_data["location"] = location.text.strip()
                    
                    # Experience
                    experience = details.find("div", class_="row-1-item")
                    if experience and experience.find("i", class_="ic-16-briefcase"):
                        job_data["experience"] = experience.text.strip()
                    
                    # Salary
                    salary = details.find("span", class_="desktop")
                    if salary:
                        job_data["salary"] = salary.text.strip()
                # Get posting time
                status = job.find("div", class_="status-success") or job.find("div", class_="status-inactive") or job.find("div", class_="status-info")
                if status:
                    job_data["posted"] = status.text.strip()
                # Get job URL if available
                if title_elem and title_elem.get('href'):
                    job_data["url"] = urljoin(url, title_elem['href'])
                job_listings.append(job_data)
            print(f"scraped {len(job_listings)} jobs")
            # Get pagination URL (next page)
            base_url = "https://internshala.com/"
            next_page_url = None
            next_page_link = soup.find("link", rel="next")
            if next_page_link and next_page_link.get('href'):
                next_page_url = urljoin(base_url, next_page_link['href'])
                url = next_page_url
                print(f"next page url {next_page_url}")
                page_count += 1
            else:
                break

        # Remove duplicate jobs based on job_id
        seen_job_ids = set()
        unique_job_listings = []
        for job in job_listings:
            if job.get('job_id') is None:
                continue
            if job['job_id'] not in seen_job_ids:
                seen_job_ids.add(job['job_id'])
                unique_job_listings.append(job)
        
        job_listings = unique_job_listings
        print(f"Found {len(job_listings)} unique jobs after removing duplicates")

        # Create CSV in memory
        csv_buffer = StringIO()
        fieldnames = ["job_id", "title", "company", "location", "experience", "salary", "posted", "url", "page"]
        writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
        writer.writeheader()
        for job in job_listings:
            writer.writerow(job)

        # Prepare CSV for upload
        csv_filename = f"internshala_jobs_{datetime.now().strftime('%Y-%m-%d')}.csv"
        csv_bytes = csv_buffer.getvalue().encode('utf-8')
        
        # Get existing job IDs from internshala_jobs table
        page_size = 1000
        offset = 0
        existing_job_ids = set()

        while True:
            batch = supabase.table('internshala_jobs').select('job_id').limit(page_size).offset(offset).execute()
            if not batch.data:  # No more results
                break
            existing_job_ids.update(job['job_id'] for job in batch.data)
            offset += page_size
        
        print(f"Found {len(existing_job_ids)} existing jobs")

        # Filter new jobs that don't exist in the table
        new_jobs = [job for job in job_listings if job['job_id'] not in existing_job_ids]
        # Insert new jobs into internshala_jobs table
        if new_jobs:
            response = supabase.table('internshala_jobs').insert(new_jobs).execute()
        storage_client = get_supabase_client()
        # Upload CSV to storage
        response = (
            storage_client.storage
            .from_("job-postings")
            .upload(
                path=csv_filename,
                file=csv_bytes,
                file_options={"cache-control": "3600", "upsert": "false"}
            )
        )

        print(f"Uploaded {len(job_listings)} jobs to Supabase storage")
        
        return {
            "text": job_listings,
            "total_jobs": len(job_listings),
            "storage_path": f"job-postings/{csv_filename}"
        }

    except Exception as ex:
        error = f"Error scraping Internshala for url {url} -- {str(ex)}"
        print(error)
        log = {
            "url": url,
            "output": error,
            "status_code": 500,
            "character_length": 0,
            "method_used": "playwright"
        }
        await logs.put(log)
        raise HTTPException(status_code=500, detail=f"Error scraping Internshala: {str(ex)}")

@app.post("/scrape_unstop")
async def unstop_api():
    """
    Get Unstop jobs sorted by start date (most recent first)
    """
    response = await extract_jobs()
    return response



# @app.post("/indiamart_excavator_listings")
# async def get_excavator_listings(page_no:int):
#     """
#     Get JCB excavator rental listings from IndiaMART for a specific page.
    
#     Args:
#         page_no (int): Page number to fetch, each page has 19 items
        
#     Returns:
#         Dict: Results for the requested page
#     """
#     try:
#         if page_no < 1:
#             raise HTTPException(status_code=400, detail="page_no must be greater than 0")
            
#         start_index = (page_no - 1) * 19 + 1
#         end_index = page_no * 19
            
#         try:
#             result = get_indiamart_excavator_listings(start_index, end_index, page_no)
#             if not result:
#                 raise HTTPException(status_code=404, detail="No results found")
                
#             return {
#                 "text": result
#             }
                
#         except Exception as ex:
#             print(f"Error processing page {page_no}: {str(ex)}")
#             raise HTTPException(status_code=500, detail=f"Error processing page {page_no}: {str(ex)}")

#     except Exception as ex:
#         raise HTTPException(status_code=500, detail=f"Error processing excavator listings: {str(ex)}")

# async def extract_linkedin_content(html):
#     soup = BeautifulSoup(html, 'html.parser')
    
#     # Find the main content section (after "Skip to main content")
#     main_content = []
    
#     # Extract all text content while skipping script, style and metadata tags
#     for text in soup.stripped_strings:
#         # Skip metadata-like content
#         if any(skip in text.lower() for skip in ['<!doctype', '<script', '<style', 'tracking', 'cookie']):
#             continue
            
#         # Skip empty strings and single characters
#         if len(text.strip()) <= 1:
#             continue
            
#         main_content.append(text.strip())
    
#     # Join all text with newlines
#     return '\n'.join(main_content)


base_url = "https://www.morningstar.in"

mutual_fund_links = [
    # f"{base_url}/funds/adityabirla.aspx",
    # f"{base_url}/funds/axis.aspx",         
    # f"{base_url}/funds/bajaj.aspx",
    # f"{base_url}/funds/bandhan.aspx",
    # f"{base_url}/funds/barodabnp.aspx",
    # f"{base_url}/funds/boi.aspx",
    # f"{base_url}/funds/canara.aspx",
    # f"{base_url}/funds/dsp.aspx",
    # f"{base_url}/funds/edelweiss.aspx",
    # f"{base_url}/funds/franklintempleton.aspx",
    # f"{base_url}/funds/groww.aspx",
    # f"{base_url}/funds/hdfc.aspx",
    # f"{base_url}/funds/helios.aspx",
    # f"{base_url}/funds/hsbc.aspx",
    # f"{base_url}/funds/icici.aspx",                    
    # f"{base_url}/funds/iifl.aspx",                     
    # f"{base_url}/funds/invesco.aspx",
    # f"{base_url}/funds/iti.aspx",
    # f"{base_url}/funds/jm.aspx",
    # f"{base_url}/funds/kotak.aspx",
    # f"{base_url}/funds/lic.aspx",
    # f"{base_url}/funds/mahindramanulife.aspx",
    # f"{base_url}/funds/mirae.aspx",
    # f"{base_url}/funds/motilal.aspx",
    # f"{base_url}/funds/navi.aspx",
    # f"{base_url}/funds/nippon.aspx",
    # f"{base_url}/funds/nj.aspx",
    # f"{base_url}/funds/oldbridge.aspx",
    # f"{base_url}/funds/pgim.aspx",                  
    # f"{base_url}/funds/ppfas.aspx",
    # f"{base_url}/funds/quant.aspx",
    # f"{base_url}/funds/quantum.aspx",
    # f"{base_url}/funds/samco.aspx",
    # f"{base_url}/funds/sbi.aspx",                  #??????
    # f"{base_url}/funds/shriram.aspx",
    # f"{base_url}/funds/sundaram.aspx",
    # f"{base_url}/funds/tata.aspx",                
    # f"{base_url}/funds/taurus.aspx",
    # f"{base_url}/funds/trust.aspx",
    # f"{base_url}/funds/union.aspx",
    # f"{base_url}/funds/uti.aspx",
    # f"{base_url}/funds/whiteoak.aspx",
    # f"{base_url}/funds/zerodha.aspx"                   
]

@app.post("/scrape_mutual_funds")
async def mutual_funds_lists():
    """Scrape mutual fund data from all AMC URLs"""
    failed_urls = []
    
    for url in mutual_fund_links:
        try:
            print(f"\nScraping {url}")
            content, status, fund_list = await playwright_scrape(url)
            
            if status == 200:
                print(f"Successfully scraped {url}")

            else:
                print(f"Failed to scrape {url} with status {status}")
                failed_urls.append(url)

                
        except Exception as e:
            print(f"Error scraping {url}: {str(e)}")
            failed_urls.append(url)

            
    # Save results summary

    print(f"Successfully scraped {len(mutual_fund_links) - len(failed_urls)} URLs")
    if failed_urls:
        print(f"Failed to scrape {len(failed_urls)} URLs:")
        for url in failed_urls:
            print(f"- {url}")
            
    return {"message":"endpoint run successfully"}

