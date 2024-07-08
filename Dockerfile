# Use the official Python image from the Docker Hub
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements.txt file to the working directory
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install additional packages needed for headless browser
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    chromium-driver

# Install Google Chrome
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list' && \
    apt-get update && \
    apt-get install -y google-chrome-stable

# Set environment variables
ENV CHROME_BIN=/usr/bin/google-chrome \
    CHROME_DRIVER=/usr/bin/chromedriver

# Copy the rest of the application code to the working directory
COPY src /app/src

# Expose the port the app runs on
EXPOSE 8000

# Define the command to run the application
CMD ["python", "src/scrapper.py"]