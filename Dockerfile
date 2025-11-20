# Base image
FROM python:3.10-slim

# Install system dependencies required for building lxml and other tools
RUN apt-get update && apt-get install -y \
gcc \
g++ \
libxml2-dev \
libxslt-dev \
libffi-dev \
libssl-dev \
git \
&& rm -rf /var/lib/apt/lists/*

# Set work directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project folder (including lncrawl/, sources/, and bot.py)
COPY . .

# Create downloads directory
RUN mkdir -p downloads

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Command to run the bot
CMD ["python", "bot.py"]