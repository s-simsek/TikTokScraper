import concurrent.futures
import datetime
import json
import multiprocessing
import re
import time

import requests
from bs4 import BeautifulSoup

import utils
from config import ScraperConfig


class VideoBatchProcessor:
    
    def _process_url(self, url:str, shared_dict:dict, lock, path:str):
        """
        Processes a single URL to fetch data and update the shared dictionary.

        Parameters
        ----------
        url : str
            The URL to be processed.
        shared_dict : dict
            The shared dictionary to store the fetched data.
        lock : Lock
            The lock to synchronize access to the shared dictionary.
        path : str
            The path where the shared dictionary is saved.
        """
        fetched_data = self._fetch_data(url)
        if fetched_data:
            with lock:
                shared_dict[url] = fetched_data
                utils.write(path, dict(shared_dict))
              
    def _parallel_process(self, url_list: list, path: str):
        """
        Processes a list of URLs in parallel using multiple processes.

        Parameters
        ----------
        url_list : list
            A list of URLs to be processed.
        path : str
            The path where the processed data will be saved.
        """
        manager = multiprocessing.Manager()
        shared_dict = manager.dict()
        lock = manager.Lock()

        with concurrent.futures.ProcessPoolExecutor(max_workers=ScraperConfig.CPU_COUNT) as executor:
            futures = {executor.submit(self._process_url, url, shared_dict, lock, path): url for url in url_list}
            start_time = time.time()
            while futures:
                # Check for completed futures
                done, not_done = concurrent.futures.wait(futures, timeout=0.1, return_when=concurrent.futures.FIRST_COMPLETED)
                # Remove completed futures from the set
                for future in done:
                    try:
                        future.result()
                    except Exception as e:
                        # print(f"Exception occurred: {e}")
                        pass
                    finally:
                        futures.pop(future)
                # Check if the overall timeout has been reached
                elapsed_time = time.time() - start_time
                if elapsed_time >= ScraperConfig.COMMENT_SCRAPER_TIMEOUT:
                    for future in not_done:
                        future.cancel()
                    break

    def _fetch_data(self):
        """Placeholder for fetching data from a URL"""
        pass

class ProcessMetaData(VideoBatchProcessor):
    
    def __init__(self, url_list):
        self.url_list = url_list 
    
    def _fetch_data(self, url:str, max_retries:int=3) -> dict:
        """
        Fetches metadata for a single URL, with retries on failure.

        Parameters
        ----------
        url : str
            The URL to fetch data from.
        max_retries : int, optional
            The number of fetch attempts, by default 3.

        Returns
        -------
        dict
            The fetched metadata information or None if unsuccessful.
        """
        attempt = 0
        while attempt < max_retries:
            try:
                response = requests.get(url, headers=ScraperConfig.HEADERS, timeout=10)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    json_content = soup.find(id="__UNIVERSAL_DATA_FOR_REHYDRATION__").string
                    if json_content:
                        video_data = json.loads(json_content)["__DEFAULT_SCOPE__"]["webapp.video-detail"]["itemInfo"]["itemStruct"]
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
            except Exception as e:
                # print(e)
                pass
            attempt += 1
            time.sleep(1)  
        return None
    
    def get_metadata(self):
        """Retrieves metadata for the URLs in the url_list"""
        self._parallel_process(self.url_list, 'data/fetched_metadata.json')
    
class ProcessComments(VideoBatchProcessor):
    
    def __init__(self, url_list):
        self.url_list = url_list
        
    def _fetch_data(self, url:str) -> list:
        """
        Fetches comments for a given video URL.

        Parameters
        ----------
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
            response = requests.get(comment_url, headers=ScraperConfig.HEADERS)
            if response.status_code == 200: 
                comment_data = response.json()['comments']
                if not comment_data:
                    break
                temp_comments = [re.search(pattern, comment['share_info']['desc']).group(1) for comment in comment_data]
                post_comments.extend(temp_comments)
            cursor_index += 50
        return post_comments
    
    def get_comments(self):
        """Retrieves comments for the URLs in the url_list"""
        self._parallel_process(self.url_list, 'data/fetched_comments.json')

        
if __name__ == '__main__':
    with open('urls.json', 'r') as file:
        urls = json.load(file)
    ProcessMetaData(urls[:10]).get_metadata()
    ProcessComments(urls[:10]).get_comments()
    try:
        comments = utils.read('data/fetched_comments.json')
        metadata = utils.read('data/fetched_metadata.json')
        print(f'metadata: {len(metadata)}')
        print(f'comments: {len(comments)}')
    except Exception as e:
        print(e)
    


    
    
    

        