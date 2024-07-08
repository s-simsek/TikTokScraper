# Use the official Python image from the Docker Hub
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /

# Copy the requirements.txt file to the working directory
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install additional packages needed for headless browser
RUN apt-get update && apt-get install -y \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

# Copy the rest of the application code to the working directory
COPY . .

# Set environment variable to use the Chrome headless browser
ENV CHROME_BIN=/usr/bin/chromium

# Expose the port the app runs on
EXPOSE 8000

# Define the command to run the application
CMD ["python", "src/scrapper.py"]