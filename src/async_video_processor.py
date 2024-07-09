import asyncio
import datetime
import json
import re

import aiohttp
from fake_useragent import UserAgent
from lxml import html

import utils
from config import ScraperConfig


class AsyncVideoProcessor:
    
    async def _process_url(self, 
                           session: aiohttp.ClientSession, 
                           url:str, 
                           shared_dict:dict, 
                           lock:asyncio.Lock, 
                           path:str):
        """
        Asynchronously processes a URL to fetch data and updates the shared dictionary.

        Parameters
        ----------
        session : aiohttp.ClientSession
            The aiohttp session to use for the request.
        url : str
            The URL to be processed.
        shared_dict : dict
            The shared dictionary to store the fetched data.
        lock : asyncio.Lock
            The lock to synchronize access to the shared dictionary.
        path : str
            The path where the shared dictionary is saved.
        """
        fetched_data = await self._fetch_data(session, url)
        if fetched_data:
            async with lock:
                shared_dict[url] = fetched_data               
                utils.write(path, shared_dict)

    async def _async_scraper(self, url_list:list, path:str):
        """
        Asynchronously scrapes data from a list of URLs and saves the results to the specified path.

        Parameters
        ----------
        url_list : list
            A list of URLs to be scraped.
        path : str
            The path where the scraped data will be saved.
        """
        shared_dict = {}
        lock = asyncio.Lock()
        async with aiohttp.ClientSession() as session:
            tasks = [self._process_url(session, url, shared_dict, lock, path) for url in url_list]
            await asyncio.gather(*tasks)
    
    async def _fetch_data(self,):
        """Placeholder for fetching data from a URL"""
        pass
    
class AsyncProcessMetaData(AsyncVideoProcessor): 
    
    def __init__(self, url_list) -> None:
        self.url_list = url_list
    
    async def _fetch_data(self, 
                          session: aiohttp.ClientSession, 
                          url:str, 
                          max_retries:int=5):
        """
        Asynchronous metadata scraping for a single URL.
        If scraping fails, retries up to max_retries times.

        Parameters
        ----------
        session : aiohttp.ClientSession
            The aiohttp session to use for the request.
        url : str
            The URL to fetch.
        max_retries : int, optional
            The number of fetch attempts, by default 5.

        Returns
        -------
        dict
            The video metadata information or None if fetching is not successful.
        """
        attempt = 0
        while attempt < max_retries:
            try:
                async with session.get(url, headers=ScraperConfig.HEADERS, timeout=10) as response:
                    if response.status == 200:
                        text = await response.text()
                        tree = html.fromstring(text)
                        json_content = tree.xpath('//script[@id="__UNIVERSAL_DATA_FOR_REHYDRATION__"]/text()')
                        if json_content:
                            video_data = json.loads(json_content[0])["__DEFAULT_SCOPE__"]["webapp.video-detail"]["itemInfo"]["itemStruct"]
                            video_info = {
                                'Account': video_data['author']['uniqueId'],
                                'Views': video_data['stats']['playCount'],
                                'Likes': video_data['stats']['diggCount'],
                                'Saved': video_data['stats']['collectCount'],
                                'Comment Count': video_data['stats']['commentCount'],
                                'Share Count': video_data['stats']['shareCount'],
                                'Caption': re.sub(r'#\w+', '', video_data['desc']).strip().replace(',', ''),
                                'Hashtags': ' '.join(re.findall(r'#\w+', video_data['desc'])),
                                'Date posted': datetime.datetime.fromtimestamp(int(video_data['createTime'])).strftime("%m/%d/%Y"),
                                'Date Collected': datetime.datetime.today().strftime("%m/%d/%Y")
                            }
                            return video_info if video_info else None
            except (aiohttp.ClientError, json.JSONDecodeError, KeyError) as e:
                # print(e)
                pass
            attempt += 1
            await asyncio.sleep(1)
        return None
    
    def get_metadata(self):
        """Retrieves metadata for the URLs in the url_list. until timeout"""
        try:
            asyncio.run(asyncio.wait_for(self._async_scraper(self.url_list, 'data/fetched_metadata.json'), 
                                         timeout=ScraperConfig.METADATA_SCRAPER_TIMEOUT))
        except:
            pass
    
class AsyncProcessComments(AsyncVideoProcessor):
    """_summary_

    Parameters
    ----------
    AsyncVideoProcessor : _type_
        _description_
    """
    
    def __init__(self, url_list) -> None:
        self.url_list = url_list
    
    async def _fetch(self, session: aiohttp.ClientSession, url: str) -> dict:
        """
        Fetches JSON data from a given URL.

        Parameters
        ----------
        session : aiohttp.ClientSession
            The aiohttp session to use for the request.
        url : str
            The URL to fetch JSON data from.

        Returns
        -------
        dict
            The fetched JSON data.
        """
        async with session.get(url, headers=ScraperConfig.HEADERS) as response:
            try: 
                return await response.json()
            except: 
                return None  
            
    async def _fetch_data(self, session: aiohttp.ClientSession, url :str) -> list:
        """
        Fetch comments data for a given video URL.

        Parameters
        ----------
        session : aiohttp.ClientSession
            The aiohttp session to use for the request.
        url : str
            The URL of the video to scrape comments for.

        Returns
        -------
        list
            A list of comments for the given video URL.
        """
        video_id = url.split('/')[-1]
        pattern = r'comment:\s*(.*)'

        post_comments = []
        cursor_index = 0

        while len(post_comments) < ScraperConfig.COMMENT_COUNT:
            comment_url = f'https://www.tiktok.com/api/comment/list/?aweme_id={video_id}&count=50&cursor={cursor_index}'
            comment_data = await self._fetch(session, comment_url)
            try: 
                if comment_data:
                    comment_data = comment_data.get('comments', [])
                    if not comment_data:
                        break
                    temp_comments = [re.search(pattern, comment['share_info']['desc']).group(1) for comment in comment_data]
                    post_comments.extend(temp_comments)
                else:
                    break
            except:
                continue
            cursor_index += 50 
        return post_comments
    
    def get_comments(self):
        """Retrieves comments for the URLs in the url_list."""
        try:
            asyncio.run(asyncio.wait_for(self._async_scraper(self.url_list, 'data/fetched_comments.json'), 
                                         timeout=ScraperConfig.COMMENT_SCRAPER_TIMEOUT))
        except:
            pass
    

if __name__ == '__main__':
    
    with open('urls.json', 'r') as file:
        urls = json.load(file)
    AsyncProcessMetaData(urls[:10]).get_metadata()
    AsyncProcessComments(urls[:10]).get_comments()
    try:
        comments = utils.read('data/fetched_comments.json')
        metadata = utils.read('data/fetched_metadata.json')
        print(f'metadata: {len(metadata)}')
        print(f'comments: {len(comments)}')
    except Exception as e:
        print(e)
        
    