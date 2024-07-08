
import json
import time
from datetime import datetime

import pytz
from langdetect import DetectorFactory, detect
from langdetect.lang_detect_exception import LangDetectException


def exponential_backoff(attempt:int):
    """
    Applies exponential backoff strategy with a maximum delay.

    Parameters
    ----------
    attempt : int
        The current attempt number to calculate the backoff delay.
    """
    delay = min(2 ** attempt, 60)  # Exponential backoff with a maximum delay of 60 seconds
    time.sleep(delay)
    
def is_english(comment:str):
    """
    Detects if a given comment is in English.

    Parameters
    ----------
    comment : str
        The comment text to be checked.

    Returns
    -------
    bool
        True if the comment is in English, False otherwise.
    """
    try:
        return detect(comment) == 'en'
    except LangDetectException:
        return False
    
def write(filename:str, file:dict):
    """
    Writes a dictionary to a JSON file.

    Parameters
    ----------
    filename : str
        The name of the file to write to.
    file : dict
        The dictionary to be written to the file.
    """
    with open(filename, 'w') as json_file:
        json.dump(file, json_file, indent=4)
        
def read(filename:str) -> dict:
    """
    Reads a dictionary from a JSON file.

    Parameters
    ----------
    filename : str
        The name of the file to read from.

    Returns
    -------
    dict
        The dictionary read from the file.
    """
    with open(filename, 'r') as file:
            result = json.load(file)
    return result
    
def record_now():
    """
    Records the current time in a specific format.

    Returns
    -------
    str
        The current time formatted as 'dd_mm_HHhMMm'.
    """
    pst = pytz.timezone('America/Los_Angeles')
    now_pst = datetime.now(pst)
    formatted_time = now_pst.strftime('%d_%m_%Hh%Mm')
    return formatted_time
