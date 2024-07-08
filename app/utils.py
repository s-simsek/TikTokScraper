
import json
import time
from datetime import datetime

import pytz
from langdetect import DetectorFactory, detect
from langdetect.lang_detect_exception import LangDetectException


def time_it(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"{func.__name__} took {end_time - start_time:.2f} seconds")
        return result
    return wrapper

def exponential_backoff(attempt:int):
    delay = min(2 ** attempt, 60)  # Exponential backoff with a maximum delay of 60 seconds
    time.sleep(delay)
    
def is_english(comment:str):
    try:
        return detect(comment) == 'en'
    except LangDetectException:
        return False
    
def write(filename:str, file:dict):
    with open(filename, 'w') as json_file:
        json.dump(file, json_file, indent=4)
        
def read(filename:str) -> dict:
    with open(filename, 'r') as file:
            result = json.load(file)
    return result
    
def record_now():
    pst = pytz.timezone('America/Los_Angeles')
    now_pst = datetime.now(pst)
    formatted_time = now_pst.strftime('%d_%m_%Hh%Mm')
    return formatted_time
