# Base image
FROM python:3.10-slim

# 1. Install system build dependencies
RUN apt-get update && apt-get install -y \
gcc g++ git libxml2-dev libxslt-dev zlib1g-dev \
libjpeg-dev libffi-dev libssl-dev \
&& rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2. Upgrade pip
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# 3. Install Python dependencies
COPY requirements.txt .
# Add pymongo manually here or ensure it's in your requirements.txt
RUN pip install --no-cache-dir -r requirements.txt && pip install pymongo dnspython

# 4. Copy project files
COPY . .
COPY start.sh .

# 5. Make start script executable
RUN chmod +x start.sh

# Environment variables
ENV LNCRAWL_MODE="dev"
ENV PYTHONUNBUFFERED=1

# Create downloads directory
RUN mkdir -p downloads

# Command to run the wrapper script instead of python directly
CMD ["./start.sh"]