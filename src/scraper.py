import json
import os
import time
from datetime import datetime

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
        directory = "runs"
        if not os.path.exists(directory):
            os.makedirs(directory)
            
        with open(self.name, 'w') as file:
            file.write(f'Scrapper Run, Date: {datetime.now().strftime("%m/%d/%Y")}\n')
        
        directory = "data"
        if not os.path.exists(directory):
            os.makedirs(directory)
            
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
        """Runs the URL scrapper and saves it into disk"""
        # run scrapper
        existing_urls = list(utils.read('data/database.json').keys())
        start_time = time.time()
        url_scrapper(existing_urls)
        end_time = time.time()
        difference = f"{end_time - start_time:.2f}"
        
        self.url_list = utils.read('data/fetched_urls.json')
        with open(self.name, 'a') as file:
            file.write(f'{len(self.url_list)} URLs collected in {difference} seconds\n')
            print(f'{len(self.url_list)} URLs collected in {difference} seconds')
            
    def scrap_metadata(self, url_list:list):
        """Scraps the metadata and saves it into disk
        Scrapping either asynchronous or in parallel based on success rate

        Parameters
        ----------
        url_list : list
            list of URLs to scrap the metadata for
        """
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
            print(f"{method} processed {processed_url_count} URLs in {difference} seconds")
            print(f'Success Rate: {int(success_rate)}%')
        
        # if success rate is < threshold, change the scrapper
        if success_rate < ScrapperConfig.SUCCESS_RATE_THRESHOLD:
            self.async_metadata = not self.async_metadata
            
    def scrap_comments(self, url_list:list):
        """Scraps the metadata and saves it into disk
        Scrapping either asynchronous or in parallel based on success rate

        Parameters
        ----------
        url_list : list
            list of URLs to scrap the metadata for
        """
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
        print(f'processed_url_count: {processed_url_count}')
        print(f'Number of URLs used to fetch comments: {len(url_list)}')
        success_rate = int(processed_url_count/len(url_list)*100)
        
        with open(self.name, 'a') as file:
            file.write(f'{method} processed {processed_url_count} URLs in {difference} seconds, ')
            file.write(f'Success Rate: {int(success_rate)}%\n')
            print(f"{method} processed {processed_url_count} URLs in {difference} seconds")
            print(f'Success Rate: {int(success_rate)}%')
            
        # if success rate is < threshold, change the scrapper
        if success_rate < ScrapperConfig.SUCCESS_RATE_THRESHOLD:
            self.async_comments = not self.async_comments
        
    def update_database(self, full_data:dict):
        """Updates the database with collected full data information
        Reports back the left over data in URLs, Metadata, and Comments database

        Parameters
        ----------
        full_data : dict --> keys: URLs | values: metadata + comments
            complete fetched data in where all the URLs have both comments and 
            metadata information
        """
        with open(self.name, 'a') as file:
            file.write(f'{len(full_data)} new URLs processed\n')
            print(f'{len(full_data)} new URLs processed')
            
        if full_data:
            with open('data/database.json', 'r') as file:
                database = json.load(file)
                
            # new urls that does not exist in the database
            new_data = set(full_data.keys()).difference(set(database.keys()))
            
            # add new urls to the database
            database = database | full_data
            utils.write('data/database.json', database)
            print(f'{len(new_data)} new data added')
            
            # update the data that is pulled in the current run 
            current_run = utils.read('data/fetched_full_data.json')
            current_run = current_run | full_data
            utils.write('data/fetched_full_data.json', current_run)
            
        # calculate the data that are left over
        metadata = utils.read('data/fetched_metadata.json')
        comments = utils.read('data/fetched_comments.json')
        urls = utils.read('data/fetched_urls.json')
        with open(self.name, 'a') as file:
            file.write(f'Left overs -> {len(urls)} URLs, {len(metadata)} Metadata, {len(comments)} Comments\n')
            print(f'Left overs -> {len(urls)} URLs, {len(metadata)} Metadata, {len(comments)} Comments')
        
    def merge_results(self, clear:bool=False) -> dict:
        """After metadata and comments information is collected
        URLs that have both information are merged and removed from 
        comments and metadata database. Unprocessed URLs are kept in each database
        for future run. After this method call, either url, metadata, and comments
        database all have unique URLs among each other or they are cleared

        Parameters
        ----------
        clear : bool, optional
            if True, clears the database for urls, metadata, and comments

        Returns
        -------
        full_data : dict --> keys: URLs | values: metadata + comments
            complete fetched data in where all the URLs have both comments and 
            metadata information
        """
        metadata = utils.read('data/fetched_metadata.json')
        comments = utils.read('data/fetched_comments.json')
        urls = utils.read('data/fetched_urls.json')
        
        # urls that have all information
        complete = set(metadata.keys()) & set(comments.keys())
        
        # urls that have either metadata or comment information
        union = set(metadata.keys()) | set(comments.keys())
        
        # urls that have no information
        unprocessed_urls = list(set(urls).difference(union))
        
        # process urls that have full information
        full_data = {}
        for url in complete: 
            metadata[url]['Comments'] = comments[url]
            full_data[url] = metadata[url]
            if not clear:
                del metadata[url]
                del comments[url]
        
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
        """Updates the URLs that have metadata information but not comments (missing_comment_urls)
        as well as URL that have comments but not metadata information (missing_metadata_urls)"""
        metadata = utils.read('data/fetched_metadata.json')
        comments = utils.read('data/fetched_comments.json')
        self.missing_comment_urls = list(set(metadata.keys()).difference(set(comments.keys())))
        self.missing_metadata_urls = list(set(comments.keys()).difference(set(metadata.keys())))
    
    def full_run(self):
        """Full run of the scrapper with the following steps:
        1 - run URL scrapper
        2 - run metadata scrapper
        3 - run comment scrapper
        4 - merge results
        5 - update database"""
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
                print(f'No new URLs acquired')
            
    def left_over_run(self, clear:bool=False):
        """Scrap only metadata and comments for left over urls
        merge the results and updates the database

        Parameters
        ----------
        clear : bool, optional
            if True, clears the database for urls, metadata, and comments
        """
        with open(self.name, 'a') as file:
                file.write(f'\n{"-"*5}Initiating Left Over Run{"-"*5}\n')
                print(f'\n{"-"*5}Initiating Left Over Run{"-"*5}')
                
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
        """Main scrapper method
        Runs the scrapper until desired total number of URLs are fully processed"""
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
            with open(self.name, 'a') as file:
                file.write(f'\n{"-"*5}Initiating Full Run{"-"*5}\n')
                print(f'\n{"-"*5}Initiating Full Run{"-"*5}')
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
                print(f'In total {len(all_data)} URLs processed in {difference} seconds')

if __name__ == '__main__': 
    scrapper = Scrapper()
    scrapper.scrap()


