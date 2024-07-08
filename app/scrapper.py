import concurrent.futures
import json
import multiprocessing
import os
import time
from datetime import datetime

import requests
import utils
from async_video_processor import AsyncProcessComments, AsyncProcessMetaData
from config import ScrapperConfig
from parallel_video_processor import ProcessComments, ProcessMetaData
from url_processor import url_scrapper


class Scrapper:
    
    def __init__(self) -> None:
        self.name = f'runs/{utils.record_now()}.txt' 
        self.async_metadata = ScrapperConfig.ASYNC_METADATA
        self.async_comments = ScrapperConfig.ASYNC_COMMENTS
        self.initiate_scrapper()

    def initiate_scrapper(self):
        """Initiates the database"""
        with open(self.name, 'w') as file:
            file.write(f'Scrapper Run, Date: {datetime.now().strftime("%m/%d/%Y")}\n')
        
        # Creating Database
        with open('data/fetched_urls.json', 'w') as json_file:
            json.dump([], json_file, indent=4)
            
        with open('data/fetched_comments.json', 'w') as json_file:
            json.dump({}, json_file, indent=4)
            
        with open('data/fetched_metadata.json', 'w') as json_file:
            json.dump({}, json_file, indent=4)
            
        with open('data/fetched_full_data.json', 'w') as json_file:
            json.dump({}, json_file, indent=4)
        
        file_path = 'data/database.json'
        if not os.path.exists(file_path):
            with open('data/database.json', 'w') as json_file:
                json.dump({}, json_file, indent=4)
    
    def scrap_urls(self):
        # run scrapper
        existing_urls = list(utils.read('data/database.json').keys())
        start_time = time.time()
        url_scrapper(existing_urls)
        end_time = time.time()
        difference = f"{end_time - start_time:.2f}"
        
        self.url_list = utils.read('data/fetched_urls.json')
        with open(self.name, 'a') as file:
            file.write(f'{len(self.url_list)} URLs collected in {difference} seconds\n')
            
    def scrap_metadata(self, url_list):
        start_time = time.time()
        if self.async_metadata:
            scrapper = AsyncProcessMetaData(url_list)
            method = 'Async Metadata'
        else:
            scrapper = ProcessMetaData(url_list)
            method = 'Parallel Metadata'
        scrapper.get_metadata()
        end_time = time.time()
        difference = f"{end_time - start_time:.2f}"
        processed_url_count = len(utils.read('data/fetched_metadata.json'))
        success_rate = int(processed_url_count/len(url_list)*100)
        
        with open(self.name, 'a') as file:
            file.write(f'{method} processed {processed_url_count} URLs in {difference} seconds, ')
            file.write(f'Success Rate: {int(success_rate)}%\n')
        
        # if success rate is < threshold, change the scrapper
        if success_rate < ScrapperConfig.SUCCESS_RATE_THRESHOLD:
            self.async_metadata = not self.async_metadata
            
    def scrap_comments(self, url_list):
        start_time = time.time()
        if self.async_comments:
            scrapper = AsyncProcessComments(url_list)
            method = 'Async Comments'
        else:
            scrapper = ProcessComments(url_list)
            method = 'Parallel Comments'
        scrapper.get_comments()
        end_time = time.time()
        difference = f"{end_time - start_time:.2f}"
        processed_url_count = len(utils.read('data/fetched_comments.json'))
        success_rate = int(processed_url_count/len(url_list)*100)
        
        with open(self.name, 'a') as file:
            file.write(f'{method} processed {processed_url_count} URLs in {difference} seconds, ')
            file.write(f'Success Rate: {int(success_rate)}%\n')
            
        # if success rate is < threshold, change the scrapper
        if success_rate < ScrapperConfig.SUCCESS_RATE_THRESHOLD:
            self.async_comments = not self.async_comments
        
    def update_database(self, full_data:dict):
        with open(self.name, 'a') as file:
            file.write(f'{len(full_data)} new URLs processed\n')
        if full_data:
            with open('data/database.json', 'r') as file:
                database = json.load(file)
            new_data = set(full_data.keys()).difference(set(database.keys()))
            database = database | full_data
            utils.write('data/database.json', database)
            print(f'{len(new_data)} new data added')
            
            current_run = utils.read('data/fetched_full_data.json')
            current_run = current_run | full_data
            utils.write('data/fetched_full_data.json', current_run)
        metadata = utils.read('data/fetched_metadata.json')
        comments = utils.read('data/fetched_comments.json')
        urls = utils.read('data/fetched_urls.json')
        with open(self.name, 'a') as file:
            file.write(f'Left overs -> {len(urls)} URLs, {len(metadata)} Metadata, {len(comments)} Comments\n')
        
    def merge_results(self, clear=False):
        metadata = utils.read('data/fetched_metadata.json')
        comments = utils.read('data/fetched_comments.json')
        urls = utils.read('data/fetched_urls.json')
        complete = set(metadata.keys()) & set(comments.keys())
        full_data = {}
        for url in complete: 
            metadata[url]['Comments'] = comments[url]
            full_data[url] = metadata[url]
            if not clear:
                del metadata[url]
                del comments[url]
        union = set(metadata.keys()) | set(comments.keys())
        unprocessed_urls = list(set(urls).difference(union))
        if clear: 
            # clean up metadata and comment database 
            utils.write('data/fetched_metadata.json', {})
            utils.write('data/fetched_comments.json', {})
            utils.write('data/fetched_urls.json', [])
            self.url_list = []
        else: 
            # update metadata and comments 
            utils.write('data/fetched_metadata.json', metadata)
            utils.write('data/fetched_comments.json', comments)
            utils.write('data/fetched_urls.json', unprocessed_urls)
            self.url_list = unprocessed_urls
        return full_data
    
    def update_missing_data(self): 
        metadata = utils.read('data/fetched_metadata.json')
        comments = utils.read('data/fetched_comments.json')
        self.missing_comment_urls = list(set(metadata.keys()).difference(set(comments.keys())))
        self.missing_metadata_urls = list(set(comments.keys()).difference(set(metadata.keys())))
    
    def full_run(self):
        print('initiating url collection')
        self.scrap_urls()
        time.sleep(ScrapperConfig.METHOD_BREAK)
        if self.url_list:
            print('initiating metada scrapping')
            self.scrap_metadata(self.url_list)
            time.sleep(ScrapperConfig.METHOD_BREAK)
            
            print('initiating comment Scrapping')
            self.scrap_comments(self.url_list)
            time.sleep(ScrapperConfig.METHOD_BREAK)
            
            full_data = self.merge_results() 
            self.update_database(full_data)
        else:
            with open(self.name, 'a') as file:
                file.write(f'No new URLs acquired\n')
            
    def left_over_run(self, clear=False):
        print(f'Initiating Left Over Run')
        with open(self.name, 'a') as file:
                file.write(f'{'-'*5}Initiating Left Over Run{'-'*5}\n')
        self.update_missing_data()
        if self.missing_comment_urls + self.url_list:
            print('initiating comment Scrapping')
            self.scrap_comments(list(set(self.missing_comment_urls + self.url_list)))
            time.sleep(ScrapperConfig.METHOD_BREAK)
            
        if self.missing_metadata_urls + self.url_list: 
            print('initiating metadata scrapping')
            self.scrap_metadata(list(set(self.missing_metadata_urls + self.url_list)))
            time.sleep(ScrapperConfig.METHOD_BREAK)
        
        full_data = self.merge_results(clear)
        self.update_database(full_data)
        
    def scrap(self):
        start_time = time.time()
        all_data = utils.read('data/fetched_full_data.json')
        
        def perform_left_over_run(clear=False):
            nonlocal all_data
            self.left_over_run(clear=clear)
            all_data = utils.read('data/fetched_full_data.json')
            if len(all_data) >= ScrapperConfig.TOTAL_SCRAP_COUNT:
                return True
            time.sleep(ScrapperConfig.RUN_BREAK)
            return False
          
        outer_break = False  
        while len(all_data) < ScrapperConfig.TOTAL_SCRAP_COUNT and not outer_break:
            
            print(f'Initiating Full Run')
            with open(self.name, 'a') as file:
                file.write(f'{'-'*5}Initiating Full Run{'-'*5}\n')
            self.full_run()
            all_data = utils.read('data/fetched_full_data.json')
            
            # if enough data is collected after full run, break
            if len(all_data) >= ScrapperConfig.TOTAL_SCRAP_COUNT:
                break
            time.sleep(ScrapperConfig.RUN_BREAK)
            
            # start left over run 
            for i in range(ScrapperConfig.LEFT_OVER_RUN_COUNT):
                # on the last iteration, clear database
                clear_database = i == ScrapperConfig.LEFT_OVER_RUN_COUNT - 1
                # if enough data is collected, break the inner and outer loop
                if perform_left_over_run(clear=clear_database):
                    outer_break = True
                    break
                
            all_data = utils.read('data/fetched_full_data.json')
        end_time = time.time()
        difference = f"{end_time - start_time:.2f}"
        with open(self.name, 'a') as file:
                file.write(f'In total {len(all_data)} URLs processed in {difference} seconds\n')

if __name__ == '__main__': 
    tryout = Scrapper()
    tryout.scrap()
    
    #with open('urls.json', 'r') as file:
        #urls = json.load(file)

