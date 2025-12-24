# Use official Python slim image
FROM python:3.12-slim

# Avoids interactive dialog
ENV DEBIAN_FRONTEND=noninteractive

# Install Chrome, ChromeDriver, and system dependencies (no apt-key usage!)
RUN apt-get update && \
    apt-get install -y wget gnupg2 ca-certificates unzip fonts-liberation libnss3 libatk-bridge2.0-0 \
        libgtk-3-0 libgbm1 libasound2 libxss1 libxtst6 curl && \
    mkdir -p /etc/apt/keyrings && \
    wget -q -O /etc/apt/keyrings/google-linux-signing-key.gpg https://dl.google.com/linux/linux_signing_key.pub && \
    echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/google-linux-signing-key.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

# Install ChromeDriver (auto version match for Chrome)
RUN CHROME_VERSION=$(google-chrome --version | grep -oP '\d+\.\d+\.\d+\.\d+') && \
    CHROMEDRIVER_VERSION=$(curl -s "https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_${CHROME_VERSION%%.*}") && \
    wget -O chromedriver.zip "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/${CHROMEDRIVER_VERSION}/linux64/chromedriver-linux64.zip" && \
    unzip chromedriver.zip && \
    mv chromedriver-linux64/chromedriver /usr/bin/chromedriver && \
    chmod +x /usr/bin/chromedriver && \
    rm -rf chromedriver.zip chromedriver-linux64

# Set display port to avoid crash (for Selenium headless)
ENV DISPLAY=:99

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy your bot code (assuming main.py as entry point)
COPY . /app
WORKDIR /app

CMD ["python", "main.py"]
