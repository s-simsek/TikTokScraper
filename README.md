# TikTok Scraper

A TikTok scraper that collects video metadata including username, video url, comments, likes, views, shared counts, hashtags, and more. 

## Installation

You have to have chrome installed!

1. Clone the repository:
    ```sh
    git clone https://github.com/s-simsek/TikTokScraper.git
    ```
2. Navigate to the project directory:
    ```sh
    cd TikTokScraper
    ```
3. Create a virtual environment and download the required dependencies:
    ```sh
    virtualenv venv
    source venv/bin/activate 
    pip3 install -r requirements.txt
    ```
4. run the app
   ```sh
   python3 src/scraper.py
   ```
   
Or, with *Docker*:

1. Pull the Docker image
    ```sh
    docker pull safaksimsek/finesse-scrapper-app
    ```
2. Run the Docker container:
   ```sh
    docker run -it safaksimsek/finesse-scrapper-app
    ```


