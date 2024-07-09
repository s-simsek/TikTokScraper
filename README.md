# TikTok Scraper

A powerful TikTok scraper that collects video metadata including comments, likes, views, shared counts, hashtags, and more.

## Introduction

The TikTok Scraper is a tool designed to collect comprehensive data from TikTok videos given hashtags. 

## Features

- Fetches TikTok URL based on hashtags provided on the config file
- Collects video metadata such as comments, likes, views, shared counts, and hashtags.

## Installation

You have to have chrome installed!

- Clone the repository:
    ```sh
    git clone https://github.com/your-username/tiktok-scraper.git
    ```
- Navigate to the project directory:
    ```sh
    cd TikTokScrapper
    ```
Create a virtual environment and download the required dependencies:
    ```sh
    virtualenv venv
    source venv/bin/activate 
    pip3 install -r requirements.txt
    ```
run the app
   ```sh
   python3 src/scrapper.py
   ```
   
Or, with *Docker*:

Pull the Docker image
    ```sh
    docker pull safaksimsek/finesse-scrapper-app
    ```
Run the Docker container:
   ```sh
    docker run -it safaksimsek/finesse-scrapper-app
    ```


