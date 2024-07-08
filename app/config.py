"""Configuration Settings for the App"""

from fake_useragent import UserAgent

ua = UserAgent()

class ScrapperConfig:
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
    ASYNC_METADATA = False
    
    # comment scrapper method: parallel or async
    ASYNC_COMMENTS = False 
    
    # total urls to scrap
    TOTAL_SCRAP_COUNT = 200 
    
    # url scrapper timeout in seconds
    URL_SCRAPPER_TIMEOUT = 40
    
    # metadata scrapper timeout in seconds
    METADATA_SCRAPPER_TIMEOUT = 40 
    
    # comment scrapper timeout in seconds
    COMMENT_SCRAPPER_TIMEOUT = 40 
    
    # sleep between methods in seconds
    METHOD_BREAK = 15 
    
    # sleep between runs in seconds
    RUN_BREAK = 60
    
    # success rate threshold
    SUCCESS_RATE_THRESHOLD = 60
    
    # left over run count
    LEFT_OVER_RUN_COUNT = 3
    
    
    
    