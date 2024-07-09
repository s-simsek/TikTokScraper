"""Configuration Settings for the App"""

import multiprocessing

from fake_useragent import UserAgent

cpu_count = multiprocessing.cpu_count()
ua = UserAgent()

class ScraperConfig:
    # base url
    URL = "https://www.tiktok.com/tag/"
    
    # hashtags to scrap
    HASHTAGS = ['fashion', 'dress', 'trend', 'fashiontiktok', 'outfit', 'stylish'] 
    
    # tiktok video class tag
    VIDEO_TAG = 'css-x6y88p-DivItemContainerV2 e19c29qe8'
    
    # number of urls to scrap at single run
    URL_SCRAP_COUNT = 100
    
    # user agent header
    HEADERS = {'User-Agent': ua.random}
    
    # number of comments to scrap at single run
    COMMENT_COUNT = 50
    
    # metadata scrapper method: parallel or async
    ASYNC_METADATA = True
    
    # number of cores to use for parallel processes
    CPU_COUNT = max(cpu_count - 4, 1)
    
    # comment scrapper method: parallel or async
    ASYNC_COMMENTS = True 
    
    # total urls to scrap
    TOTAL_SCRAP_COUNT = 600 
    
    # url scrapper timeout in seconds
    URL_SCRAPER_TIMEOUT = 60
    
    # metadata scrapper timeout in seconds
    METADATA_SCRAPER_TIMEOUT = 40 
    
    # comment scrapper timeout in seconds
    COMMENT_SCRAPER_TIMEOUT = 40 
    
    # sleep between methods in seconds
    METHOD_BREAK = 10
    
    # sleep between runs in seconds
    RUN_BREAK = 20
    
    # success rate threshold
    SUCCESS_RATE_THRESHOLD = 60
    
    # left over run count
    LEFT_OVER_RUN_COUNT = 3
    
    
    
    