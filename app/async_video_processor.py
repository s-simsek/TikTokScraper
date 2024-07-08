import asyncio
import datetime
import json
import re

import aiohttp
import utils
from config import ScrapperConfig
from fake_useragent import UserAgent
from langdetect import DetectorFactory, detect
from langdetect.lang_detect_exception import LangDetectException
from lxml import html


class AsyncVideoProcessor:
    
    async def _process_url(self, session, url:str, shared_dict:dict, lock, path:str):
        fetched_data = await self._fetch_data(session, url)
        if fetched_data:
            async with lock:
                shared_dict[url] = fetched_data               
                utils.write(path, shared_dict)

    async def _async_scrapper(self, url_list:list, path:str) -> dict:
        shared_dict = {}
        lock = asyncio.Lock()
        async with aiohttp.ClientSession() as session:
            tasks = [self._process_url(session, url, shared_dict, lock, path) for url in url_list]
            await asyncio.gather(*tasks)
    
    async def _fetch_data(self):
        pass
    
class AsyncProcessMetaData(AsyncVideoProcessor): 
    
    def __init__(self, url_list) -> None:
        self.url_list = url_list
    
    async def _fetch_data(self, session, url, max_retries=5):
        """_summary_

        Parameters
        ----------
        session : _type_
            _description_
        url : _type_
            _description_
        max_retries : int, optional
            _description_, by default 5

        Returns
        -------
        _type_
            _description_
        """
        attempt = 0
        while attempt < max_retries:
            try:
                async with session.get(url, headers=ScrapperConfig.HEADERS, timeout=10) as response:
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
    
    def get_metadata(self) -> dict:
        try:
            asyncio.run(asyncio.wait_for(self._async_scrapper(self.url_list, 'data/fetched_metadata.json'), 
                                         timeout=ScrapperConfig.METADATA_SCRAPPER_TIMEOUT))
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
    
    async def _fetch(self, session, url):
        async with session.get(url, headers=ScrapperConfig.HEADERS) as response:
            try: 
                return await response.json()
            except: 
                return None  
            
    async def _fetch_data(self, session, url):
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

        while len(post_comments) < ScrapperConfig.COMMENT_COUNT:
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
    
    @utils.time_it
    def get_comments(self) -> dict:
        """_summary_

        Returns
        -------
        dict
            _description_
        """
        try:
            asyncio.run(asyncio.wait_for(self._async_scrapper(self.url_list, 'data/fetched_comments.json'), 
                                         timeout=ScrapperConfig.COMMENT_SCRAPPER_TIMEOUT))
        except:
            pass
    

if __name__ == '__main__':
    
    with open('urls.json', 'r') as file:
        urls = json.load(file)
    AsyncProcessMetaData(urls).get_metadata()
    # AsyncProcessComments(urls[:20]).get_comments()
    try:
        comments = utils.read('data/fetched_metadata.json')
        print(len(comments))
    except:
        print('empty')
        
    