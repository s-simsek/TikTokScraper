# Use the official Selenium image with Chrome and ChromeDriver pre-installed
FROM selenium/standalone-chrome:3.141.59

# Set the working directory in the container
WORKDIR /app

# Install Python and pip
USER root
RUN apt-get update && apt-get install -y python3 python3-pip

# Copy the requirements.txt file to the working directory
COPY requirements.txt .

# Install the dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy the rest of the application code to the working directory
COPY src /app/src

# Expose the port the app runs on
EXPOSE 8000

# Define the command to run the applicatioån
CMD ["python3", "src/scrapper.py"]
