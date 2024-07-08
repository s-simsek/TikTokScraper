import concurrent.futures
import multiprocessing
import re
import time

import utils
from bs4 import BeautifulSoup
from config import ScrapperConfig
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

ua = UserAgent()
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--window-size=1920,1080")

def url_verificaiton(url:str) -> bool:   
    """Verifies the URL based on URL structure

    Parameters
    ----------
    url : str

    Returns
    -------
    bool
        True if the url is verifies, False otherwise
    """
    pattern = re.compile(r'^https://www\.tiktok\.com/@[^/]+/(video|photo)/\d+$')
    return bool(pattern.match(url)) 

def fetch_video_urls(url: str, existing_urls: list, shared_video_urls, lock, stop_signal):
    """
    Fetches video URLs from a given hashtag URL using Selenium to scroll and load more videos.

    Parameters
    ----------
    url : str
        The hashtag URL to scrape videos from.
    existing_urls : list
        List of existing video URLs to check against to avoid duplicates.
    shared_video_urls : multiprocessing.Manager().list
        A shared list to store the fetched video URLs.
    lock : multiprocessing.Manager().Lock
        A lock to synchronize access to the shared list.
    stop_signal : multiprocessing.Manager().Value
        A signal to indicate when to stop the scraping process.
    """
    chrome_options.add_argument(f"--user-agent={ua.random}")
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    video_urls = set(existing_urls)
    attempt = 0
    
    while len(shared_video_urls) < ScrapperConfig.URL_SCRAP_COUNT:
        if stop_signal.value:
            break
        # Scroll to load more videos
        for _ in range(4):
            driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.END)
            time.sleep(0.5)  
            
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        videos = soup.find_all('div', {'class': ScrapperConfig.VIDEO_TAG}) 
        
        # video URLs scrapped
        video_urls_collected = set([video.find('a', href=True)['href'] for video in videos])
        
        # remove already existing video URLs
        new_videos = video_urls_collected.difference(video_urls)
        
        # add new URLs to already existing URLS
        video_urls |= new_videos
        new_videos = [i for i in new_videos if url_verificaiton(i)]
        
        # make sure that anohter process didn't append the same URL 
        with lock:
            for video_id in new_videos:
                if video_id not in shared_video_urls:
                    shared_video_urls.append(video_id)
                    
            # save the URLs to database
            utils.write('data/fetched_urls.json', list(shared_video_urls))
        time.sleep(2)
        attempt += 1
    driver.quit()

def scrap_parallel(hashtag_urls, existing_urls:list, stop_signal):
    """
    Scrapes video URLs in parallel using multiple processes.

    Parameters
    ----------
    hashtag_urls : list of str
        List of hashtag URLs to scrape videos from.
    existing_urls : list of str
        List of existing video URLs to check against to avoid duplicates.
    stop_signal : multiprocessing.Manager().Value
        A signal to indicate when to stop the scraping process.

    Returns
    -------
    list
        A list of video URLs scraped from the provided hashtag URLs, excluding duplicates.
    """
    manager = multiprocessing.Manager()
    shared_video_urls = manager.list()
    lock = manager.Lock()
    cpu_count = min(multiprocessing.cpu_count(), len(ScrapperConfig.HASHTAGS))
    with concurrent.futures.ProcessPoolExecutor(max_workers=cpu_count) as executor:
        futures = [executor.submit(fetch_video_urls, url, existing_urls, shared_video_urls, lock, stop_signal) for url in hashtag_urls]

        for future in concurrent.futures.as_completed(futures):
            future.result()  

def url_scrapper(existing_urls: list):
    """
    Initiates the URL scraping process for the given list of existing URLs.

    Parameters
    ----------
    existing_urls : list
        List of existing video URLs to check against to avoid duplicates.
    """
    hashtag_urls = [ScrapperConfig.URL + hashtag for hashtag in ScrapperConfig.HASHTAGS]
    stop_signal = multiprocessing.Manager().Value('b', False)
    process = multiprocessing.Process(target=scrap_parallel, args=(hashtag_urls, existing_urls, stop_signal))
    process.start()
    process.join(timeout=ScrapperConfig.URL_SCRAPPER_TIMEOUT)
    if process.is_alive():
        stop_signal.value = True
        process.terminate()
        process.join()
        
if __name__ == '__main__':
    existing_urls = utils.read('urls.json')[:10]
    url_scrapper(existing_urls)
    